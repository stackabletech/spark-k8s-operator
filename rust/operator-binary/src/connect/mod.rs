mod common;
pub mod controller;
pub mod crd;
mod executor;
pub mod server;
mod service;

pub(crate) const GRPC: &str = "grpc";
pub(crate) const HTTP: &str = "http";
