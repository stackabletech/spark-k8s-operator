use crate::{
    constants::*,
    history::{
        LogFileDirectorySpec::{self, S3},
        S3LogFileDirectorySpec,
    },
    tlscerts,
};
use stackable_operator::{
    builder::{SecretOperatorVolumeSourceBuilder, VolumeBuilder},
    commons::{
        authentication::tls::{CaCert, TlsVerification},
        s3::{InlinedS3BucketSpec, S3AccessStyle},
        secret_class::SecretClassVolume,
    },
    k8s_openapi::api::core::v1::{Volume, VolumeMount},
};
use std::collections::BTreeMap;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::builder::SecretFormat;
use strum::{EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("s3 bucket error"))]
    S3Bucket {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("missing bucket name for history logs"))]
    BucketNameMissing,
    #[snafu(display("tls non-verification not supported"))]
    S3TlsNoVerificationNotSupported,
    #[snafu(display("ca-cert verification not supported"))]
    S3TlsCaVerificationNotSupported,
}

pub struct S3LogDir {
    pub bucket: InlinedS3BucketSpec,
    pub prefix: String,
}

impl S3LogDir {
    pub async fn resolve(
        log_file_dir: Option<&LogFileDirectorySpec>,
        namespace: Option<String>,
        client: &stackable_operator::client::Client,
    ) -> Result<Option<S3LogDir>, Error> {
        #[allow(irrefutable_let_patterns)]
        let (s3bucket, prefix) =
            if let Some(S3(S3LogFileDirectorySpec { bucket, prefix })) = log_file_dir {
                (
                    bucket
                        .resolve(client, namespace.unwrap().as_str())
                        .await
                        .context(S3BucketSnafu)
                        .ok(),
                    prefix.clone(),
                )
            } else {
                // !!!!!
                // Ugliness alert!
                // No point in trying to resolve the connection anymore since there is no
                // log_file_dir in the first place.
                // This can casually happen for Spark applications that don't use a history server
                // !!!!!
                return Ok(None);
            };

        // Check that a bucket name has been defined
        s3bucket
            .as_ref()
            .and_then(|i| i.bucket_name.as_ref())
            .context(BucketNameMissingSnafu)?;

        if let Some(conn) = s3bucket.as_ref().and_then(|i| i.connection.as_ref()) {
            if let Some(tls) = &conn.tls {
                match &tls.verification {
                    TlsVerification::None {} => return S3TlsNoVerificationNotSupportedSnafu.fail(),
                    TlsVerification::Server(server_verification) => {
                        match &server_verification.ca_cert {
                            CaCert::WebPki {} => {}
                            CaCert::SecretClass(_) => {}
                        }
                    }
                }
            }
        }

        Ok(Some(S3LogDir {
            bucket: s3bucket.unwrap(),
            prefix,
        }))
    }

    /// Constructs the properties needed for loading event logs from S3.
    /// These properties are later written in the `SPARK_DEFAULTS_FILE_NAME` file.
    ///
    /// The following properties related to credentials are not included:
    /// * spark.hadoop.fs.s3a.aws.credentials.provider
    /// * spark.hadoop.fs.s3a.access.key
    /// * spark.hadoop.fs.s3a.secret.key
    /// instead, the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set
    /// on the container start command.
    pub fn history_server_spark_config(&self) -> BTreeMap<String, String> {
        let mut result = BTreeMap::new();

        result.insert("spark.history.fs.logDirectory".to_string(), self.url());

        if let Some(endpoint) = self.bucket.endpoint() {
            result.insert("spark.hadoop.fs.s3a.endpoint".to_string(), endpoint);
        }

        if let Some(conn) = self.bucket.connection.as_ref() {
            if let Some(S3AccessStyle::Path) = conn.access_style {
                result.insert(
                    "spark.hadoop.fs.s3a.path.style.access".to_string(),
                    "true".to_string(),
                );
            }
        }

        result
    }

    pub fn application_spark_config(&self) -> BTreeMap<String, String> {
        let mut result = BTreeMap::new();
        result.insert("spark.eventLog.enabled".to_string(), "true".to_string());
        result.insert("spark.eventLog.dir".to_string(), self.url());

        let bucket_name = self.bucket.bucket_name.as_ref().unwrap().clone();
        if let Some(endpoint) = self.bucket.endpoint() {
            result.insert(
                format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.endpoint"),
                endpoint,
            );
        }

        if let Some(conn) = self.bucket.connection.as_ref() {
            if let Some(S3AccessStyle::Path) = conn.access_style {
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.path.style.access"),
                    "true".to_string(),
                );
            }

            if let Some(secret_dir) = self.credentials_mount_path() {
                // We don't use the credentials at all here but assume they are available
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.access.key"),
                    format!("$(cat {secret_dir}/{ACCESS_KEY_ID})"),
                );
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.secret.key"),
                    format!("$(cat {secret_dir}/{SECRET_ACCESS_KEY})"),
                );
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.aws.credentials.provider"),
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".to_string(),
                );
            } else {
                result.insert(
                    format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.aws.credentials.provider"),
                    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".to_string(),
                );
            }
        }

        result
    }

    fn url(&self) -> String {
        format!(
            "s3a://{}/{}",
            self.bucket.bucket_name.as_ref().unwrap().clone(), // this is guaranteed to exist at this point
            self.prefix
        )
    }

    pub fn volumes(&self) -> Vec<Volume> {
        let mut volumes: Vec<Volume> = self.credentials_volume().into_iter().collect();

        if let Some(secret_name) = tlscerts::tls_secret_name(&self.bucket.connection) {
            volumes.push(
                VolumeBuilder::new(secret_name)
                    .ephemeral(
                        SecretOperatorVolumeSourceBuilder::new(secret_name)
                            .with_format(SecretFormat::TlsPkcs12)
                            .build(),
                    )
                    .build(),
            );
        }
        volumes
    }

    pub fn volume_mounts(&self) -> Vec<VolumeMount> {
        let mut volume_mounts: Vec<VolumeMount> =
            self.credentials_volume_mount().into_iter().collect();

        if let Some(secret_name) = tlscerts::tls_secret_name(&self.bucket.connection) {
            let secret_dir = format!("{STACKABLE_MOUNT_PATH_TLS}/{secret_name}");

            volume_mounts.push(VolumeMount {
                name: secret_name.to_string(),
                mount_path: secret_dir,
                ..VolumeMount::default()
            });
        }
        volume_mounts
    }

    pub fn credentials_volume(&self) -> Option<Volume> {
        self.credentials()
            .map(|credentials| credentials.to_volume(credentials.secret_class.as_ref()))
    }

    pub fn credentials_volume_mount(&self) -> Option<VolumeMount> {
        self.credentials().map(|secret_class_volume| VolumeMount {
            name: secret_class_volume.secret_class.clone(),
            mount_path: format!(
                "{}/{}",
                S3_SECRET_DIR_NAME, secret_class_volume.secret_class
            ),
            ..VolumeMount::default()
        })
    }

    pub fn credentials(&self) -> Option<SecretClassVolume> {
        self.bucket
            .connection
            .as_ref()
            .and_then(|conn| conn.credentials.clone())
    }

    pub fn credentials_mount_path(&self) -> Option<String> {
        self.credentials().map(|secret_class_volume| {
            format!(
                "{}/{}",
                S3_SECRET_DIR_NAME, secret_class_volume.secret_class
            )
        })
    }
}
