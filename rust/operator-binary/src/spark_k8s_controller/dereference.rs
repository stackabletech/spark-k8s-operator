//! The dereference step in the SparkApplication controller.
//!
//! Fetches all Kubernetes objects referenced by the SparkApplication spec (templates, S3
//! connection, log directory) and returns them in [`DereferencedSparkApplication`].
//! Synchronous validation belongs in the sibling [`super::validate`] module.

use snafu::Snafu;

#[derive(Snafu, Debug)]
pub enum Error {}

type Result<T, E = Error> = std::result::Result<T, E>;
