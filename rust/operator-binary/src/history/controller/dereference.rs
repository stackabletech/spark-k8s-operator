//! The dereference step in the SparkHistoryServer controller.
//!
//! Fetches the resolved log directory (including the underlying S3 connection/bucket and
//! TLS secret if any) and returns it in [`DereferencedSparkHistoryServer`].

use snafu::{ResultExt, Snafu};
use stackable_operator::client::Client;

use crate::crd::{history::v1alpha1, logdir::ResolvedLogDir};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve log directory"))]
    LogDir { source: crate::crd::logdir::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Kubernetes objects referenced from a SparkHistoryServer, already fetched.
pub struct DereferencedSparkHistoryServer {
    pub log_dir: ResolvedLogDir,
}

pub async fn dereference(
    client: &Client,
    shs: &v1alpha1::SparkHistoryServer,
) -> Result<DereferencedSparkHistoryServer> {
    let log_dir = ResolvedLogDir::resolve(
        &shs.spec.log_file_directory,
        shs.metadata.namespace.clone(),
        client,
    )
    .await
    .context(LogDirSnafu)?;

    Ok(DereferencedSparkHistoryServer { log_dir })
}
