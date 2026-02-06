use std::collections::BTreeMap;

use indoc::formatdoc;
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
    crd::constants::{
        STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE, STACKABLE_TRUST_STORE_NAME,
    },
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
        self.build_truststore_from_pem_ca_command().map(|command| {
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
        self.s3_buckets
            .iter()
            .flat_map(|bucket| bucket.connection.tls.volumes_and_mounts())
            .flat_map(|(_, mount)| mount)
            .collect()
    }

    // The volume where the truststore is written to by the init container.
    fn truststore_volume(&self) -> Volume {
        Volume {
            name: STACKABLE_TRUST_STORE_NAME.to_string(),
            empty_dir: Some(Default::default()),
            ..Default::default()
        }
    }

    fn secret_class_tls_ca_paths(&self) -> Vec<String> {
        // List of ca.crt files mounted by the secret classes.
        self.s3_buckets
            .iter()
            .flat_map(|bucket| bucket.connection.tls.tls_ca_cert_mount_path())
            .collect()
    }

    fn build_truststore_from_pem_ca_command(&self) -> Option<String> {
        let input_ca_paths: Vec<String> = self.secret_class_tls_ca_paths();

        let out_truststore_path = format!("{STACKABLE_TRUST_STORE}/truststore.p12");

        if input_ca_paths.is_empty() {
            None
        } else {
            Some(formatdoc! { "
                cert-tools generate-pkcs12-truststore --out {out_truststore_path} --out-password {STACKABLE_TLS_STORE_PASSWORD} {pem_args}",
                pem_args = input_ca_paths
                    .iter()
                    .map(|path| format!("--pem {path}"))
                    .collect::<Vec<String>>()
                    .join(" ")
            })
        }
    }

    fn extra_java_options(&self) -> Option<BTreeMap<String, Option<String>>> {
        if self.build_truststore_from_pem_ca_command().is_some() {
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
