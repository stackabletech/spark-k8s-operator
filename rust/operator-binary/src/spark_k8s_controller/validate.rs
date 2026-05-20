//! The validate step in the SparkApplication controller.
//!
//! Synchronously validates the [`super::dereference::DereferencedSparkApplication`] and
//! resolves the product image and product config. Does not touch the Kubernetes API.

use snafu::Snafu;

#[derive(Snafu, Debug)]
pub enum Error {}

type Result<T, E = Error> = std::result::Result<T, E>;
