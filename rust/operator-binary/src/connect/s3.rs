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

    // Build the list of volumes and mounts needed for the S3 credentials and/or TLS trust stores.
    pub(crate) fn volumes_and_mounts(&self) -> Result<(Vec<Volume>, Vec<VolumeMount>), Error> {
        // Ensures that there are no duplicate volumes or mounts across buckets.
        let mut volumes_by_name = BTreeMap::new();
        let mut mounts_by_name = BTreeMap::new();

        // --------------------------------------------------------------------------------
        // Connection volumes and mounts.
        // --------------------------------------------------------------------------------
        if let Some(conn) = &self.s3_connection {
            let (conn_volumes, conn_mounts) = conn
                .volumes_and_mounts()
                .context(ConnectionVolumesAndMountsSnafu)?;

            for volume in conn_volumes.into_iter() {
                volumes_by_name.entry(volume.name.clone()).or_insert(volume);
            }
            for mount in conn_mounts.into_iter() {
                mounts_by_name.entry(mount.name.clone()).or_insert(mount);
            }
        }

        // --------------------------------------------------------------------------------
        // Per-bucket volumes and mounts.
        // --------------------------------------------------------------------------------
        for bucket in self.s3_buckets.iter() {
            let (bucket_volumes, bucket_mounts) = bucket
                .connection
                .volumes_and_mounts()
                .context(ConnectionVolumesAndMountsSnafu)?;

            for volume in bucket_volumes.into_iter() {
                volumes_by_name.entry(volume.name.clone()).or_insert(volume);
            }
            for mount in bucket_mounts.into_iter() {
                mounts_by_name.entry(mount.name.clone()).or_insert(mount);
            }
        }

        // --------------------------------------------------------------------------------
        // Trust store volume and mount.
        // This is where the the trust store built by `cert-tools` in the init container is stored.
        // --------------------------------------------------------------------------------
        volumes_by_name
            .entry(STACKABLE_TRUST_STORE_NAME.to_string())
            .or_insert_with(|| Volume {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                empty_dir: Some(Default::default()),
                ..Default::default()
            });
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
    ) -> Result<Option<stackable_operator::k8s_openapi::api::core::v1::Container>, Error> {
        if let Some(command) = self.truststore_init_container_command() {
            let (_, volume_mounts) = self.volumes_and_mounts()?;
            Ok(Some(
                stackable_operator::k8s_openapi::api::core::v1::Container {
                    name: "tls-truststore-init".to_string(),
                    image: Some(image.image),
                    command: Some(vec![
                        "/bin/bash".to_string(),
                        "-x".to_string(),
                        "-euo".to_string(),
                        "pipefail".to_string(),
                        "-c".to_string(),
                        command,
                    ]),
                    volume_mounts: Some(volume_mounts),
                    ..Default::default()
                },
            ))
        } else {
            Ok(None)
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
    //
    // IMPORTANT: We assume the CA certs are in PEM format because we know that `s3::v1alpha1::ConnectionSpec::volumes_and_mounts()`
    // does NOT set the format annotation on the secret volumes.
    //
    fn truststore_init_container_command(&self) -> Option<String> {
        let input_ca_paths: Vec<String> = self.secret_class_tls_ca_paths();

        let out_truststore_path = format!("{STACKABLE_TRUST_STORE}/truststore.p12");

        if input_ca_paths.is_empty() {
            None
        } else {
            Some(
                Some(tlscerts::convert_system_trust_store_to_pkcs12())
                .into_iter()
                .chain(input_ca_paths.iter().map(|path| format!("cert-tools generate-pkcs12-truststore --out {out_truststore_path} --out-password {STACKABLE_TLS_STORE_PASSWORD} --pkcs12 {out_truststore_path}:{STACKABLE_TLS_STORE_PASSWORD} --pem {path}")))
                .collect::<Vec<String>>()
                .join(" && "))
        }
    }

    fn extra_java_options(&self) -> Option<BTreeMap<String, Option<String>>> {
        if self.truststore_init_container_command().is_some() {
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

#[cfg(test)]
mod tests {
    use product_config::writer::to_java_properties_string;
    use rstest::*;
    use stackable_operator::commons::{
        secret_class::SecretClassVolume,
        tls_verification::{CaCert, Tls, TlsClientDetails, TlsServerVerification, TlsVerification},
    };

    use super::*;

    #[rstest]
    #[case("no connection and no buckets",
        ResolvedS3 {
            s3_buckets: vec![],
            s3_connection: None,
        },
        vec![STACKABLE_TRUST_STORE_NAME.to_string(),],
        vec![VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            }],
        None,
    )]
    #[case("connection without credentials and without tls",
        ResolvedS3 {
            s3_buckets: vec![],
            s3_connection: Some(s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: None,
                tls: TlsClientDetails {
                    tls: None,
                },
            })
        },
        vec![STACKABLE_TRUST_STORE_NAME.to_string()],
        vec![VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            }],
        None,
    )]
    #[case("connection with credentials and no tls",
        ResolvedS3 {
            s3_buckets: vec![],
            s3_connection: Some(s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "connection-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: None,
                },
            })
        },
        vec!["connection-secret-class-s3-credentials".to_string(),
             STACKABLE_TRUST_STORE_NAME.to_string()],
        vec![
            VolumeMount {
                name: "connection-secret-class-s3-credentials".to_string(),
                mount_path: "/stackable/secrets/connection-secret-class".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            }],
        None,
    )]
    #[case("connection with credentials and tls",
        ResolvedS3 {
            s3_buckets: vec![],
            s3_connection: Some(s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "connection-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: Some(Tls {
                        verification: TlsVerification::Server(
                            TlsServerVerification{
                            ca_cert: CaCert::SecretClass("connection-tls-ca-secret-class".to_string()),
                        })
                    }),
                },
            })
        },
        vec!["connection-secret-class-s3-credentials".to_string(),
            "connection-tls-ca-secret-class-ca-cert".to_string(),
             STACKABLE_TRUST_STORE_NAME.to_string()],
        vec![
            VolumeMount {
                name: "connection-secret-class-s3-credentials".to_string(),
                mount_path: "/stackable/secrets/connection-secret-class".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: "connection-tls-ca-secret-class-ca-cert".to_string(),
                mount_path: "/stackable/secrets/connection-tls-ca-secret-class".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            }],
        Some("cert-tools generate-pkcs12-truststore --pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem --out /stackable/truststore/truststore.p12 --out-password changeit && cert-tools generate-pkcs12-truststore --out /stackable/truststore/truststore.p12 --out-password changeit --pkcs12 /stackable/truststore/truststore.p12:changeit --pem /stackable/secrets/connection-tls-ca-secret-class/ca.crt".to_string()),
    )]
    #[case("one bucket without credentials and without tls",
        ResolvedS3 {
            s3_connection: None,
            s3_buckets: vec![
                s3::v1alpha1::ResolvedBucket {
                    bucket_name: "bucket-1".to_string(),
                    connection: s3::v1alpha1::ConnectionSpec {
                        host: "my-s3-endpoint.com".parse().unwrap(),
                        port: None,
                        region: s3::v1alpha1::Region {
                            name: "us-east-1".to_string(),
                        },
                        access_style: S3AccessStyle::Path,
                        credentials: None,
                        tls: TlsClientDetails {
                            tls: None,
                        },
            }}]
        },
        vec![STACKABLE_TRUST_STORE_NAME.to_string()],
        vec![VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            }],
        None,
    )]
    #[case("one bucket with credentials and no tls",
        ResolvedS3 {
            s3_connection: None,
            s3_buckets: vec![
                s3::v1alpha1::ResolvedBucket {
                    bucket_name: "bucket-1".to_string(),
                    connection: s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "bucket-1-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: None,
                },
            }}]
        },
        vec!["bucket-1-secret-class-s3-credentials".to_string(),
             STACKABLE_TRUST_STORE_NAME.to_string()],
        vec![
            VolumeMount {
                name: "bucket-1-secret-class-s3-credentials".to_string(),
                mount_path: "/stackable/secrets/bucket-1-secret-class".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            }],
        None,
    )]
    #[case("one bucket with credentials and tls",
        ResolvedS3 {
            s3_connection: None,
            s3_buckets: vec![
                s3::v1alpha1::ResolvedBucket {
                    bucket_name: "bucket-1".to_string(),
                    connection: s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "bucket-1-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: Some(Tls {
                        verification: TlsVerification::Server(
                            TlsServerVerification{
                            ca_cert: CaCert::SecretClass("bucket-1-tls-ca-secret-class".to_string()),
                        })
                    }),
                },
            }}]
        },
        vec!["bucket-1-secret-class-s3-credentials".to_string(),
            "bucket-1-tls-ca-secret-class-ca-cert".to_string(),
             STACKABLE_TRUST_STORE_NAME.to_string()],
        vec![
            VolumeMount {
                name: "bucket-1-secret-class-s3-credentials".to_string(),
                mount_path: "/stackable/secrets/bucket-1-secret-class".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: "bucket-1-tls-ca-secret-class-ca-cert".to_string(),
                mount_path: "/stackable/secrets/bucket-1-tls-ca-secret-class".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            }],
        Some("cert-tools generate-pkcs12-truststore --pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem --out /stackable/truststore/truststore.p12 --out-password changeit && cert-tools generate-pkcs12-truststore --out /stackable/truststore/truststore.p12 --out-password changeit --pkcs12 /stackable/truststore/truststore.p12:changeit --pem /stackable/secrets/bucket-1-tls-ca-secret-class/ca.crt".to_string()),
    )]
    #[case("connection and one bucket with different credentials and same TLS secret class",
        ResolvedS3 {
            s3_connection: Some(
                s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "connection-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: Some(Tls {
                        verification: TlsVerification::Server(
                            TlsServerVerification{
                            ca_cert: CaCert::SecretClass("tls-ca-secret-class".to_string()),
                        })
                    }),
                },
            }
            ),
            s3_buckets: vec![
                s3::v1alpha1::ResolvedBucket {
                    bucket_name: "bucket-1".to_string(),
                    connection: s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "bucket-1-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: Some(Tls {
                        verification: TlsVerification::Server(
                            TlsServerVerification{
                            ca_cert: CaCert::SecretClass("tls-ca-secret-class".to_string()),
                        })
                    }),
                },
            }}]
        },
        vec![
            "bucket-1-secret-class-s3-credentials".to_string(),
            "connection-secret-class-s3-credentials".to_string(),
             STACKABLE_TRUST_STORE_NAME.to_string(),
            "tls-ca-secret-class-ca-cert".to_string(),],
        vec![
            VolumeMount {
                name: "bucket-1-secret-class-s3-credentials".to_string(),
                mount_path: "/stackable/secrets/bucket-1-secret-class".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: "connection-secret-class-s3-credentials".to_string(),
                mount_path: "/stackable/secrets/connection-secret-class".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            },
                        VolumeMount {
                name: "tls-ca-secret-class-ca-cert".to_string(),
                mount_path: "/stackable/secrets/tls-ca-secret-class".to_string(),
                ..Default::default()
            }],
        Some("cert-tools generate-pkcs12-truststore --pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem --out /stackable/truststore/truststore.p12 --out-password changeit && cert-tools generate-pkcs12-truststore --out /stackable/truststore/truststore.p12 --out-password changeit --pkcs12 /stackable/truststore/truststore.p12:changeit --pem /stackable/secrets/tls-ca-secret-class/ca.crt".to_string()),
    )]
    #[case("connection and one bucket with same credentials and same TLS secret class",
        ResolvedS3 {
            s3_connection: Some(
                s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "credentials-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: Some(Tls {
                        verification: TlsVerification::Server(
                            TlsServerVerification{
                            ca_cert: CaCert::SecretClass("tls-ca-secret-class".to_string()),
                        })
                    }),
                },
            }
            ),
            s3_buckets: vec![
                s3::v1alpha1::ResolvedBucket {
                    bucket_name: "bucket-1".to_string(),
                    connection: s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "credentials-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: Some(Tls {
                        verification: TlsVerification::Server(
                            TlsServerVerification{
                            ca_cert: CaCert::SecretClass("tls-ca-secret-class".to_string()),
                        })
                    }),
                },
            }}]
        },
        vec![
            "credentials-secret-class-s3-credentials".to_string(),
             STACKABLE_TRUST_STORE_NAME.to_string(),
            "tls-ca-secret-class-ca-cert".to_string(),],
        vec![
            VolumeMount {
                name: "credentials-secret-class-s3-credentials".to_string(),
                mount_path: "/stackable/secrets/credentials-secret-class".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.to_string(),
                mount_path: STACKABLE_TRUST_STORE.to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: "tls-ca-secret-class-ca-cert".to_string(),
                mount_path: "/stackable/secrets/tls-ca-secret-class".to_string(),
                ..Default::default()
            }],
        Some("cert-tools generate-pkcs12-truststore --pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem --out /stackable/truststore/truststore.p12 --out-password changeit && cert-tools generate-pkcs12-truststore --out /stackable/truststore/truststore.p12 --out-password changeit --pkcs12 /stackable/truststore/truststore.p12:changeit --pem /stackable/secrets/tls-ca-secret-class/ca.crt".to_string()),
    )]
    fn test_volumes_and_mounts(
        #[case] case_name: &str,
        #[case] resolved_s3: ResolvedS3,
        #[case] expected_volumes_names: Vec<String>,
        #[case] expected_mounts: Vec<VolumeMount>,
        #[case] init_container_command: Option<String>,
    ) {
        let (volumes, mounts) = resolved_s3.volumes_and_mounts().unwrap();
        assert_eq!(
            volumes.into_iter().map(|v| v.name).collect::<Vec<_>>(),
            expected_volumes_names,
            "Case failed for volumes: {}",
            case_name
        );
        assert_eq!(
            mounts, expected_mounts,
            "Case failed for mounts: {}",
            case_name
        );

        assert_eq!(
            resolved_s3.truststore_init_container_command(),
            init_container_command,
            "Case failed for init container command: {}",
            case_name
        );
    }

    #[rstest]
    #[case("connection and one bucket with same credentials",
        ResolvedS3 {
            s3_connection: Some(
                s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "credentials-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: None,
                },
            }),
            s3_buckets: vec![
                s3::v1alpha1::ResolvedBucket {
                    bucket_name: "bucket-1".to_string(),
                    connection: s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "credentials-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: None,
                },
            }}]
        },
        indoc::indoc! {r#"
            spark.hadoop.fs.s3a.access.key=${file\:UTF-8\:/stackable/secrets/credentials-secret-class/accessKey}
            spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
            spark.hadoop.fs.s3a.bucket.bucket-1.access.key=${file\:UTF-8\:/stackable/secrets/credentials-secret-class/accessKey}
            spark.hadoop.fs.s3a.bucket.bucket-1.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
            spark.hadoop.fs.s3a.bucket.bucket-1.endpoint=http\://my-s3-endpoint.com/
            spark.hadoop.fs.s3a.bucket.bucket-1.endpoint.region=us-east-1
            spark.hadoop.fs.s3a.bucket.bucket-1.path.style.access=true
            spark.hadoop.fs.s3a.bucket.bucket-1.secret.key=${file\:UTF-8\:/stackable/secrets/credentials-secret-class/secretKey}
            spark.hadoop.fs.s3a.endpoint=http\://my-s3-endpoint.com/
            spark.hadoop.fs.s3a.endpoint.region=us-east-1
            spark.hadoop.fs.s3a.path.style.access=true
            spark.hadoop.fs.s3a.secret.key=${file\:UTF-8\:/stackable/secrets/credentials-secret-class/secretKey}
        "#}.to_string(),
    )]
    #[case("connection and one bucket with different credentials and endpoints",
        ResolvedS3 {
            s3_connection: Some(
                s3::v1alpha1::ConnectionSpec {
                host: "far-away.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "connection-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: None,
                },
            }),
            s3_buckets: vec![
                s3::v1alpha1::ResolvedBucket {
                    bucket_name: "bucket-1".to_string(),
                    connection: s3::v1alpha1::ConnectionSpec {
                host: "my-s3-endpoint.com".parse().unwrap(),
                port: None,
                region: s3::v1alpha1::Region {
                    name: "us-east-1".to_string(),
                },
                access_style: S3AccessStyle::Path,
                credentials: Some(SecretClassVolume {
                    secret_class: "bucket-1-secret-class".to_string(),
                    scope: None,
                }),
                tls: TlsClientDetails {
                    tls: None,
                },
            }}]
        },
        indoc::indoc! {r#"
            spark.hadoop.fs.s3a.access.key=${file\:UTF-8\:/stackable/secrets/connection-secret-class/accessKey}
            spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
            spark.hadoop.fs.s3a.bucket.bucket-1.access.key=${file\:UTF-8\:/stackable/secrets/bucket-1-secret-class/accessKey}
            spark.hadoop.fs.s3a.bucket.bucket-1.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
            spark.hadoop.fs.s3a.bucket.bucket-1.endpoint=http\://my-s3-endpoint.com/
            spark.hadoop.fs.s3a.bucket.bucket-1.endpoint.region=us-east-1
            spark.hadoop.fs.s3a.bucket.bucket-1.path.style.access=true
            spark.hadoop.fs.s3a.bucket.bucket-1.secret.key=${file\:UTF-8\:/stackable/secrets/bucket-1-secret-class/secretKey}
            spark.hadoop.fs.s3a.endpoint=http\://far-away.com/
            spark.hadoop.fs.s3a.endpoint.region=us-east-1
            spark.hadoop.fs.s3a.path.style.access=true
            spark.hadoop.fs.s3a.secret.key=${file\:UTF-8\:/stackable/secrets/connection-secret-class/secretKey}
        "#}.to_string(),
    )]

    fn test_spark_properties(
        #[case] case_name: &str,
        #[case] resolved_s3: ResolvedS3,
        #[case] spark_properties_string: String,
    ) {
        let properties = resolved_s3.spark_properties().unwrap();

        assert_eq!(
            to_java_properties_string(properties.iter()).unwrap(),
            spark_properties_string,
            "Case failed for spark properties: {}",
            case_name
        );
    }
}
