use std::collections::{BTreeMap, BTreeSet};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        product_image_selection::ResolvedProductImage, secret_class::SecretClassVolumeError,
    },
    crd::s3::{self, v1alpha1::S3AccessStyle},
    k8s_openapi::api::core::v1::{Volume, VolumeMount},
};

use crate::{
    connect::crd,
    crd::{
        constants::{
            STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE, STACKABLE_TRUST_STORE_NAME,
        },
        tlscerts,
    },
};

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to resolve S3 connection"))]
    ResolveS3Connection {
        source: s3::v1alpha1::ConnectionError,
    },

    #[snafu(display("failed to resolve S3 bucket"))]
    ResolveS3Bucket { source: s3::v1alpha1::BucketError },

    #[snafu(display("missing namespace"))]
    MissingNamespace,

    #[snafu(display("failed to get endpoint for S3 connection"))]
    S3ConnectionEndpoint {
        source: s3::v1alpha1::ConnectionError,
    },

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

pub(crate) struct ResolvedS3 {
    s3_buckets: Vec<s3::v1alpha1::ResolvedBucket>,
    s3_connection: Option<s3::v1alpha1::ConnectionSpec>,
}

impl ResolvedS3 {
    pub(crate) async fn resolve(
        client: &stackable_operator::client::Client,
        connect_server: &crd::v1alpha1::SparkConnectServer,
    ) -> Result<ResolvedS3, Error> {
        let mut s3_buckets = Vec::new();
        let namespace = connect_server
            .metadata
            .namespace
            .as_ref()
            .context(MissingNamespaceSnafu)?;
        for bucket in connect_server.spec.connectors.s3buckets.iter() {
            let resolved_bucket = bucket
                .clone()
                .resolve(client, namespace)
                .await
                .context(ResolveS3BucketSnafu)?;

            s3_buckets.push(resolved_bucket);
        }

        let s3_connection = match connect_server.spec.connectors.s3connection.clone() {
            Some(conn) => Some(
                conn.resolve(client, namespace)
                    .await
                    .context(ResolveS3ConnectionSnafu)?,
            ),
            None => None,
        };

        Ok(ResolvedS3 {
            s3_buckets,
            s3_connection,
        })
    }

    // Generate Spark properties for the resolved S3 buckets.
    // Properties are generated "per bucket" using the prefix: spark.hadoop.fs.s3a.bucket.{bucket_name}.
    pub(crate) fn spark_properties(&self) -> Result<BTreeMap<String, Option<String>>, Error> {
        let mut result = BTreeMap::new();

        // --------------------------------------------------------------------------------
        // Add global connection properties if a connection is defined.
        // --------------------------------------------------------------------------------
        if let Some(conn) = &self.s3_connection {
            result.insert(
                "spark.hadoop.fs.s3a.endpoint".to_string(),
                Some(
                    conn.endpoint()
                        .context(S3ConnectionEndpointSnafu)?
                        .to_string(),
                ),
            );
            result.insert(
                "spark.hadoop.fs.s3a.path.style.access".to_string(),
                Some((conn.access_style == S3AccessStyle::Path).to_string()),
            );
            result.insert(
                "spark.hadoop.fs.s3a.endpoint.region".to_string(),
                Some(conn.region.name.clone()),
            );
            if let Some((access_key_file_path, secret_key_file_path)) =
                conn.credentials_mount_paths()
            {
                result.insert(
                    "spark.hadoop.fs.s3a.access.key".to_string(),
                    Some(format!("${{file:UTF-8:{access_key_file_path}}}")),
                );
                result.insert(
                    "spark.hadoop.fs.s3a.secret.key".to_string(),
                    Some(format!("${{file:UTF-8:{secret_key_file_path}}}")),
                );
                result.insert(
                    "spark.hadoop.fs.s3a.aws.credentials.provider".to_string(),
                    Some("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".to_string()),
                );
            } else {
                result.insert(
                    "spark.hadoop.fs.s3a.aws.credentials.provider".to_string(),
                    Some("org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".to_string()),
                );
            }
        }

        // --------------------------------------------------------------------------------
        // Add per-bucket properties for all the buckets.
        // --------------------------------------------------------------------------------
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

        // Add any extra properties needed for TLS configuration.
        if let Some(extra_tls_properties) = self.extra_java_options() {
            result.extend(extra_tls_properties);
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

        // Always add the truststore volume and mount even if they are not populated by an init
        // container.
        volumes_by_name
            .entry(STACKABLE_TRUST_STORE_NAME.to_string())
            .or_insert_with(|| self.truststore_volume());
        mounts_by_name
            .entry(STACKABLE_TRUST_STORE_NAME.to_string())
            .or_insert_with(|| VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            });

        Ok((
            volumes_by_name.into_values().collect(),
            mounts_by_name.into_values().collect(),
        ))
    }

    pub(crate) fn truststore_init_container(
        &self,
        image: ResolvedProductImage,
    ) -> Option<stackable_operator::k8s_openapi::api::core::v1::Container> {
        self.build_truststore_command().map(|command| {
            stackable_operator::k8s_openapi::api::core::v1::Container {
                name: "tls-truststore-init".to_string(),
                image: Some(image.image.clone()),
                command: Some(vec![
                    "/bin/bash".to_string(),
                    "-x".to_string(),
                    "-euo".to_string(),
                    "pipefail".to_string(),
                    "-c".to_string(),
                    command,
                ]),
                volume_mounts: Some(self.truststore_init_container_volume_mounts()),
                ..Default::default()
            }
        })
    }

    // The list of volume mounts for the init container that builds the truststore from PEM CA certs.
    // It contains the output volume mount as well as all the input mounts for the PEM CA certs.
    fn truststore_init_container_volume_mounts(&self) -> Vec<VolumeMount> {
        let mut result = self.tls_volume_mounts();
        result.extend([VolumeMount {
            name: STACKABLE_TRUST_STORE_NAME.to_string(),
            mount_path: STACKABLE_TRUST_STORE.to_string(),
            ..Default::default()
        }]);
        result
    }

    // The list of volume mounts for TLS CAs.
    fn tls_volume_mounts(&self) -> Vec<VolumeMount> {
        // Ensure volume mounts are unique by name across all buckets and the connection.
        let mounts_by_name = BTreeMap::from_iter(
            self.s3_buckets
                .iter()
                .flat_map(|bucket| bucket.connection.tls.volumes_and_mounts())
                .flat_map(|(_, mount)| mount)
                .map(|mount| (mount.name.clone(), mount))
                .chain(
                    self.s3_connection
                        .iter()
                        .flat_map(|conn| conn.tls.volumes_and_mounts())
                        .flat_map(|(_, mount)| mount)
                        .map(|mount| (mount.name.clone(), mount)),
                ),
        );
        mounts_by_name.into_values().collect()
    }

    // The volume where the truststore is written to by the init container.
    fn truststore_volume(&self) -> Volume {
        Volume {
            name: STACKABLE_TRUST_STORE_NAME.to_string(),
            empty_dir: Some(Default::default()),
            ..Default::default()
        }
    }

    // List of paths to ca.crt files mounted by the secret classes.
    fn secret_class_tls_ca_paths(&self) -> Vec<String> {
        // Ensure that CA paths are unique across all buckets and the connection.
        let paths: BTreeSet<String> = self
            .s3_buckets
            .iter()
            .flat_map(|bucket| bucket.connection.tls.tls_ca_cert_mount_path())
            .chain(
                self.s3_connection
                    .iter()
                    .flat_map(|conn| conn.tls.tls_ca_cert_mount_path().into_iter()),
            )
            .collect();

        paths.into_iter().collect()
    }

    // Builds the command that generates a truststore from the system trust store
    // and any additional CA certs provided by the user through secret classes.
    fn build_truststore_command(&self) -> Option<String> {
        let input_ca_paths: Vec<String> = self.secret_class_tls_ca_paths();

        let out_truststore_path = format!("{STACKABLE_TRUST_STORE}/truststore.p12");

        if input_ca_paths.is_empty() {
            None
        } else {
            Some(
                Some(tlscerts::convert_system_trust_store_to_pkcs12())
                .into_iter()
                .chain(input_ca_paths.iter().map(|path| format!("cert-tools generate-pkcs12-truststore --out {out_truststore_path} --out-password {STACKABLE_TLS_STORE_PASSWORD} --pkcs12 {out_truststore_path}:{STACKABLE_TLS_STORE_PASSWORD} --pkcs12 {path}")))
                .collect::<Vec<String>>()
                .join(" && "))
        }
    }

    fn extra_java_options(&self) -> Option<BTreeMap<String, Option<String>>> {
        if self.build_truststore_command().is_some() {
            let mut ssl_options = BTreeMap::new();
            ssl_options.insert(
                "-Djavax.net.ssl.trustStore".to_string(),
                format!("{STACKABLE_TRUST_STORE}/truststore.p12"),
            );
            ssl_options.insert(
                "-Djavax.net.ssl.trustStorePassword".to_string(),
                STACKABLE_TLS_STORE_PASSWORD.to_string(),
            );
            ssl_options.insert(
                "-Djavax.net.ssl.trustStoreType".to_string(),
                "pkcs12".to_string(),
            );

            let ssl_options_str = ssl_options
                .into_iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<String>>()
                .join(" ");

            Some(BTreeMap::from([
                (
                    "spark.driver.extraJavaOptions".to_string(),
                    Some(ssl_options_str.clone()),
                ),
                (
                    "spark.executor.extraJavaOptions".to_string(),
                    Some(ssl_options_str),
                ),
            ]))
        } else {
            None
        }
    }
}
