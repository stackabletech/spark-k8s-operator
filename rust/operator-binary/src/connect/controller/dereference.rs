//! The dereference step in the SparkConnectServer controller.

use snafu::Snafu;

#[derive(Snafu, Debug)]
pub enum Error {}

type Result<T, E = Error> = std::result::Result<T, E>;
