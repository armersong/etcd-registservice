#[macro_use]
extern crate serde_derive;

mod registservice;
pub use registservice::{ServiceInstanceStatus, ServiceInstance, RegistService};
