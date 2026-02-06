use std::collections::BTreeMap;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::secret_class::SecretClassVolumeError,
    crd::s3::{self, v1alpha1::S3AccessStyle},
    k8s_openapi::api::core::v1::{Volume, VolumeMount},
};

use crate::connect::crd;

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to resolve S3 connection"))]
    ResolveS3Connection { source: s3::v1alpha1::BucketError },

    #[snafu(display("missing namespace"))]
    MissingNamespace,

    #[snafu(display("failed to get endpoint for S3 bucket {bucket_name:?}"))]
    BucketEndpoint {
        bucket_name: String,
        source: s3::v1alpha1::ConnectionError,
    },

    #[snafu(display(
        "failed to create secret volume for S3 bucket with secret class {secret_class:?}"
    ))]
    S3SecretVolume {
        secret_class: String,
        source: SecretClassVolumeError,
    },

    #[snafu(display("failed to get volumes and mounts for S3 connection"))]
    ConnectionVolumesAndMounts {
        source: s3::v1alpha1::ConnectionError,
    },
}

pub(crate) struct ResolvedS3Buckets {
    s3_buckets: Vec<s3::v1alpha1::ResolvedBucket>,
}

impl ResolvedS3Buckets {
    pub(crate) async fn resolve(
        client: &stackable_operator::client::Client,
        connect_server: &crd::v1alpha1::SparkConnectServer,
    ) -> Result<ResolvedS3Buckets, Error> {
        let mut s3_buckets = Vec::new();
        let namespace = connect_server
            .metadata
            .namespace
            .as_ref()
            .context(MissingNamespaceSnafu)?;
        for conn in connect_server.spec.connectors.s3.iter() {
            let resolved_bucket = conn
                .clone()
                .resolve(client, namespace)
                .await
                .context(ResolveS3ConnectionSnafu)?;

            s3_buckets.push(resolved_bucket);
        }

        Ok(ResolvedS3Buckets { s3_buckets })
    }

    // Generate Spark properties for the resolved S3 buckets.
    // Properties are generated "per bucket" using the prefix: spark.hadoop.fs.s3a.bucket.{bucket_name}.
    pub(crate) fn spark_properties(&self) -> Result<BTreeMap<String, Option<String>>, Error> {
        let mut result = BTreeMap::new();

        for bucket in &self.s3_buckets {
            let bucket_name = bucket.bucket_name.clone();
            result.insert(
                format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.endpoint"),
                Some(
                    bucket
                        .connection
                        .endpoint()
                        .with_context(|_| BucketEndpointSnafu {
                            bucket_name: bucket_name.clone(),
                        })?
                        .to_string(),
                ),
            );
            result.insert(
                format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.path.style.access"),
                Some((bucket.connection.access_style == S3AccessStyle::Path).to_string()),
            );
            result.insert(
                format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.endpoint.region"),
                Some(bucket.connection.region.name.clone()),
            );
            if let Some((access_key_file_path, secret_key_file_path)) =
                bucket.connection.credentials_mount_paths()
            {
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.access.key"),
                    Some(format!("${{file:UTF-8:{access_key_file_path}}}")),
                );
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.secret.key"),
                    Some(format!("${{file:UTF-8:{secret_key_file_path}}}")),
                );
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.aws.credentials.provider"),
                    Some("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".to_string()),
                );
            } else {
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.aws.credentials.provider"),
                    Some("org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".to_string()),
                );
            }
        }

        Ok(result)
    }

    // Ensures that there are no duplicate volumes or mounts across buckets.
    pub(crate) fn volumes_and_mounts(&self) -> Result<(Vec<Volume>, Vec<VolumeMount>), Error> {
        let mut volumes_by_name = BTreeMap::new();
        let mut mounts_by_name = BTreeMap::new();

        for bucket in self.s3_buckets.iter() {
            let (bucket_volumes, bucket_mounts) = bucket
                .connection
                .volumes_and_mounts()
                .context(ConnectionVolumesAndMountsSnafu)?;

            for volume in bucket_volumes.iter() {
                let volume_name = volume.name.clone();
                volumes_by_name.entry(volume_name).or_insert(volume.clone());
            }
            for mount in bucket_mounts.iter() {
                let mount_name = mount.name.clone();
                mounts_by_name.entry(mount_name).or_insert(mount.clone());
            }
        }

        Ok((
            volumes_by_name.into_values().collect(),
            mounts_by_name.into_values().collect(),
        ))
    }
}
