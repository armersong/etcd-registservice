use std::thread;
use std::time::Duration;

use etcd_registservice::RegistService;

pub fn demo() {
    let service = RegistService::new("aaa", &vec!["http://localhost:2379".to_string()], "localhost", 1000, 3).unwrap();
    let service1 = RegistService::new("bbb", &vec!["http://localhost:2379".to_string()], "localhost", 1200, 3).unwrap();
    thread::sleep(Duration::from_secs(10));
    println!("service find {:?}", service.find_instances(&"bbb"));
    println!("service1 find {:?}", service1.find_instances(&"aaa"));
    service.stop();
    service1.stop();
}

fn main() {
    demo();
}
