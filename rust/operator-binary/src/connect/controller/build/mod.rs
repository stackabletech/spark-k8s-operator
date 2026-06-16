//! Builders that turn a `ValidatedSparkConnectServer` into Kubernetes resources.
//!
//! These are grouped by role (`server`, `executor`) rather than by resource kind: each Spark
//! Connect role bundles a cohesive set of builders — its ConfigMap, StatefulSet/pod template,
//! Spark properties, environment variables and JVM arguments — so keeping a role's builders
//! together in one module is clearer than scattering them across per-kind modules.

pub(crate) mod executor;
pub(crate) mod server;
pub(crate) mod service;
