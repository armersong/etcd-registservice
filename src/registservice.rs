/// etcd
/// /services/<instance_id>/nodes/<ip:port>
/// node info: {
///     ip: 
///     port:
///     updated_time:
///     added_time:
///     expire_time:
///     desc:
///     status:
/// }

use etcd::Client;
use etcd::kv::{self};
use futures::Future;
use tokio::runtime::Runtime;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use std::time::SystemTime;
use std::thread;
use std::time::Duration;

const SERVICE_PREFIX:&str = "/services";

#[derive(Debug, Clone)]
pub enum ServiceInstanceStatus {
    Initing,
    Ready,
    Offline,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct ServiceInstance {
    pub instance_id: String,
    pub ip: String,
    pub port: u16,
    /// last update time in milliseconds
    pub updated_time: u64,
    /// last regist time in milliseconds
    pub added_time: u64,
    /// service description
    pub desc: String,
    /// 0: initing, 1: ready, 2: offline
    pub status: ServiceInstanceStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub instance_id: String,
    pub ip: String,
    pub port: u16,
    /// last update time in milliseconds
    pub updated_time: u64,
    /// last regist time in milliseconds
    pub added_time: u64,
    /// info expire time in milliseconds
    pub expire_time: u64,
    /// service description
    pub desc: String,
    /// 0: initing, 1: ready, 2: offline
    pub status: i8,
}

struct RegistServiceInner {
    /// url: "http://localhost:2379"
    servers: Vec<String>,
    instance_id: String,
    ip: String,
    port: u16,
    keep_alive: u64,    //ms
    instances: RwLock<HashMap<String, Vec<ServiceInstance>>>,
    started_time: u64,
    is_running: AtomicBool,
}

pub struct RegistService(Arc<RegistServiceInner>);

impl Clone for RegistService {
    fn clone(&self) -> Self {
        RegistService(self.0.clone())
    }
}


impl RegistService {
    /// @param keep_alive_duration: seconds
    pub fn new(instance_id:&str, servers:&Vec<String>, ip:&str, port:u16, keep_alive_duration:usize) -> Result<RegistService, String> {
        let inner = RegistServiceInner {
            instance_id: instance_id.to_string(),
            servers: servers.clone(),
            ip: ip.to_string(),
            port: port,
            keep_alive: (keep_alive_duration*1000) as u64,
            instances: RwLock::new(HashMap::new()),
            started_time: now(),
            is_running: AtomicBool::new(true),
        };
        let service = RegistService(Arc::new(inner));
        service.sync()?;
        Ok(service)
    }

    pub fn stop(&self) {
        self.0.is_running.store(false, Ordering::Relaxed);
    }

    fn sync(&self) -> Result<(), String>{
        self.regist()?;
        let this = self.clone();
        thread::spawn(move || {
            println!("sync thread>>>");
            let mut last = now();
            let retry_count = 3;
            while this.0.is_running.load(Ordering::Relaxed) {
                let n = now();
                if n < last + this.0.keep_alive * 4/5 {
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
                last = n;

                for _ in 0..retry_count {
                    match this.heartbeat() {
                        Ok(_) => break,
                        Err(e) => println!("send heartbeat failed: {}", e),
                    };
                }

                for _ in 0..retry_count {
                    match this.sync_instances() {
                        Ok(_) => break,
                        Err(e) => println!("sync instances failed: {}", e),
                    };
                }
            }
            println!("sync thread<<<");
        });
        Ok(())
    }

    pub fn find_instances(&self, instance_id:&str) -> Vec<ServiceInstance> {
        let mut instances:Vec<ServiceInstance> = Vec::new();
        let cache = self.0.instances.read().unwrap();
        if let Some(insts) = cache.get(instance_id) {
            for inst in insts.iter() {
                instances.push(inst.clone());
            }
        }
        instances
    }

    pub fn sync_instances(&self) -> Result<(), String>{
        let mut servers:Vec<&str> = Vec::new();
        for v in self.0.servers.iter() {
            servers.push(v);
        }
        let client = Client::new(&servers, None).unwrap();
        let prefix = format!("{}/", SERVICE_PREFIX);;
        let mut opt = kv::GetOptions::default();
        opt.recursive = true;
        opt.sort = true;
        let this = self.clone();
        let work = kv::get(&client, prefix.as_str(), opt).and_then(move|response| {
            // println!("data {:?}", response.data);
            let mut instances:HashMap<String,Vec<ServiceInstance>> = HashMap::new();
            let mut count = 0;
            if let Some(nodes) = response.data.node.nodes {
                for node in nodes.iter() {
                    // println!("node {:?}", node);
                    if node.dir == None || node.nodes.is_none() {
                        continue;
                    }
                    if let Some(children) = &node.nodes {
                        for n in children.iter() {
                            if let Some(ref v) = n.value {
                                match serde_json::from_str::<Instance>(v.as_str()) {
                                    Ok(instance) => {
                                        if instance.instance_id == this.0.instance_id {
                                            continue;
                                        }
                                        let status = match instance.status {
                                            0 => ServiceInstanceStatus::Initing,
                                            1=> ServiceInstanceStatus::Ready,
                                            2=> ServiceInstanceStatus::Offline,
                                            _=> ServiceInstanceStatus::Unknown,
                                        };
                                        let inst = ServiceInstance{
                                            instance_id: instance.instance_id.clone(),
                                            ip: instance.ip.clone(),
                                            port: instance.port,
                                            updated_time: instance.updated_time,
                                            added_time: instance.added_time,
                                            desc: instance.desc.to_string(),
                                            status: status,
                                        };
                                        // println!("add instance {:?}", inst);
                                        if let Some(v) = instances.get_mut(&instance.instance_id) {
                                            v.push(inst);
                                        } else {
                                            instances.insert(instance.instance_id.clone(), vec![inst]);
                                        }
                                        count +=1;
                                    },
                                    Err(e) => {
                                        println!("sync_instances: parse {} failed: {}", v, e);
                                    },
                                };
                            }

                        }
                    }
                }
                println!("total {} instance", count);
            }
            // println!("instance_id {} instances: {:?}", this.0.instance_id, instances);
            Ok(instances)
        });
        Runtime::new().unwrap().block_on(work).map(|instances|{
            let mut cache = self.0.instances.write().unwrap();
            *cache = instances;
        }).map_err(|e| format!("sync_instances failed:{:?}", e))?;
        Ok(())
    }

    fn regist(&self) -> Result<(), String> {
        self.heartbeat()
    }

    fn heartbeat(&self) -> Result<(), String> {
        // println!("heartbeat>>>");
        let mut servers:Vec<&str> = Vec::new();
        for v in self.0.servers.iter() {
            servers.push(v);
        }
        let client = Client::new(&servers, None).unwrap();
        let key = format!("{}/{}/{}:{}", SERVICE_PREFIX, self.0.instance_id, self.0.ip, self.0.port);
        let n = now();
        let instance = Instance {
            instance_id: self.0.instance_id.clone(),
            ip: self.0.ip.clone(),
            port: self.0.port,
            updated_time: n,
            added_time: self.0.started_time,
            expire_time: n + self.0.keep_alive,
            desc: String::from(""),
            status: 1,
        };
        let v = serde_json::to_string(&instance).map_err(|e| format!("heartbeat: json convert failed:{}",e))?;
        // println!("key {} value {}", key, v);
        let work = kv::set(&client, key.as_str(), v.as_str(), Some(self.0.keep_alive/1000));
        Runtime::new().unwrap().block_on(work).map_err(|e| format!("heartbeat failed:{:?}", e))?;
        // println!("heartbeat<<<");
        Ok(())
    }
}

fn now() -> u64 {
    let duration = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    duration.as_secs() * 1000 + duration.subsec_millis() as u64
}
