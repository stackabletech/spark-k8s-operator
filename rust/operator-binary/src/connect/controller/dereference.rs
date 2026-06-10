//! The dereference step in the SparkConnectServer controller.
//!
//! Fetches the resolved S3 configuration referenced by the SparkConnectServer spec.

use snafu::{ResultExt, Snafu};
use stackable_operator::{client::Client, kube::ResourceExt};

use crate::connect::{crd::v1alpha1, s3};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve S3 connection for {name}"))]
    ResolveS3Connections { source: s3::Error, name: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DereferencedSparkConnectServer {
    pub resolved_s3: s3::ResolvedS3,
}

pub async fn dereference(
    client: &Client,
    scs: &v1alpha1::SparkConnectServer,
) -> Result<DereferencedSparkConnectServer> {
    let resolved_s3 = s3::ResolvedS3::resolve(client, scs)
        .await
        .with_context(|_| ResolveS3ConnectionsSnafu {
            name: scs.name_unchecked(),
        })?;

    Ok(DereferencedSparkConnectServer { resolved_s3 })
}
