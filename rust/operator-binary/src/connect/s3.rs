use std::collections::{BTreeMap, BTreeSet};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::secret_class::{SecretClassVolume, SecretClassVolumeError},
    crd::s3,
    k8s_openapi::api::core::v1::{Volume, VolumeMount},
};

use crate::{
    connect::crd,
    crd::constants::{ACCESS_KEY_ID, S3_SECRET_DIR_NAME, SECRET_ACCESS_KEY},
};

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
}

pub(crate) struct ResolvedS3Buckets {
    s3_buckets: Vec<s3::v1alpha1::ResolvedBucket>,
    secret_class_volumes: BTreeSet<SecretClassVolume>,
}

impl ResolvedS3Buckets {
    pub(crate) async fn resolve(
        client: &stackable_operator::client::Client,
        connect_server: &crd::v1alpha1::SparkConnectServer,
    ) -> Result<ResolvedS3Buckets, Error> {
        let mut s3_buckets = Vec::new();
        let mut secret_class_volumes = BTreeSet::new();
        let namespace = connect_server
            .metadata
            .namespace
            .as_ref()
            .context(MissingNamespaceSnafu)?;
        for conn in connect_server.spec.s3.iter() {
            let resolved_bucket = conn
                .clone()
                .resolve(client, namespace)
                .await
                .context(ResolveS3ConnectionSnafu)?;

            if let Some(credentials) = &resolved_bucket.connection.credentials {
                secret_class_volumes.insert(credentials.clone());
            }

            s3_buckets.push(resolved_bucket);
        }

        Ok(ResolvedS3Buckets {
            s3_buckets,
            secret_class_volumes,
        })
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
                Some(bucket.connection.access_style.to_string()),
            );
            result.insert(
                format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.endpoint.region"),
                Some(bucket.connection.region.name.clone()),
            );
            if let Some(credentials) = &bucket.connection.credentials {
                let secret_class_name = credentials.secret_class.clone();
                let secret_dir = format!("{S3_SECRET_DIR_NAME}/{secret_class_name}");

                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.access.key"),
                    Some(format!("${{file:UTF-8:{secret_dir}/{ACCESS_KEY_ID}}}")),
                );
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.secret.key"),
                    Some(format!("${{file:UTF-8:{secret_dir}/{SECRET_ACCESS_KEY}}}")),
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

    pub(crate) fn volumes(&self) -> Result<Vec<Volume>, Error> {
        let mut volumes = Vec::new();
        for secret_class_volume in self.secret_class_volumes.iter() {
            volumes.push(
                secret_class_volume
                    .to_volume(&secret_class_volume.secret_class)
                    .with_context(|_| S3SecretVolumeSnafu {
                        secret_class: secret_class_volume.secret_class.clone(),
                    })?,
            );
        }
        Ok(volumes)
    }

    pub(crate) fn volume_mounts(&self) -> Vec<VolumeMount> {
        let mut mounts = Vec::new();

        for secret_class_volume in self.secret_class_volumes.iter() {
            let secret_class_name = secret_class_volume.secret_class.clone();
            let secret_dir = format!("{S3_SECRET_DIR_NAME}/{secret_class_name}");

            mounts.push(VolumeMount {
                name: secret_class_name,
                mount_path: secret_dir,
                ..VolumeMount::default()
            });
        }

        mounts
    }
}
