use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::volume::{
        SecretFormat, SecretOperatorVolumeSourceBuilder, SecretOperatorVolumeSourceBuilderError,
        VolumeBuilder,
    },
    commons::{
        s3::{ResolvedS3Bucket, S3AccessStyle, S3Error},
        secret_class::SecretClassVolume,
    },
    k8s_openapi::api::core::v1::{Volume, VolumeMount},
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::crd::{
    constants::*,
    history::{
        LogFileDirectorySpec::{self, S3},
        S3LogFileDirectorySpec,
    },
    tlscerts,
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("missing bucket name for history logs"))]
    BucketNameMissing,

    #[snafu(display("tls non-verification not supported"))]
    S3TlsNoVerificationNotSupported,

    #[snafu(display("ca-cert verification not supported"))]
    S3TlsCaVerificationNotSupported,

    #[snafu(display("failed to build TLS certificate SecretClass Volume"))]
    TlsCertSecretClassVolumeBuild {
        source: SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build credentials Volume"))]
    CredentialsVolumeBuild {
        source: stackable_operator::commons::secret_class::SecretClassVolumeError,
    },

    #[snafu(display("failed to configure S3 connection/bucket"))]
    ConfigureS3 { source: S3Error },
}

pub enum ResolvedLogDir {
    S3(S3LogDir),
    Custom(String),
}

impl ResolvedLogDir {
    pub async fn resolve(
        log_file_dir: &LogFileDirectorySpec,
        namespace: Option<String>,
        client: &stackable_operator::client::Client,
    ) -> Result<ResolvedLogDir, Error> {
        match log_file_dir {
            S3(s3_log_dir) => S3LogDir::resolve(s3_log_dir, namespace, client)
                .await
                .map(ResolvedLogDir::S3),
            LogFileDirectorySpec::CustomLogDirectory(custom_log_dir) => {
                Ok(ResolvedLogDir::Custom(custom_log_dir.to_owned()))
            }
        }
    }

    pub fn tls_enabled(&self) -> bool {
        self.tls_secret_name().is_some()
    }

    pub fn tls_secret_name(&self) -> Option<&str> {
        match self {
            ResolvedLogDir::S3(s3_log_dir) => {
                tlscerts::tls_secret_name(&s3_log_dir.bucket.connection)
            }
            ResolvedLogDir::Custom(_) => None,
        }
    }

    pub fn history_server_spark_config(&self) -> Result<BTreeMap<String, String>, Error> {
        match self {
            ResolvedLogDir::S3(s3_log_dir) => s3_log_dir.history_server_spark_config(),
            ResolvedLogDir::Custom(custom_log_dir) => Ok(BTreeMap::from([(
                "spark.history.fs.logDirectory".to_string(),
                custom_log_dir.to_string(),
            )])),
        }
    }

    pub fn application_spark_config(&self) -> Result<BTreeMap<String, String>, Error> {
        match self {
            ResolvedLogDir::S3(s3_log_dir) => s3_log_dir.application_spark_config(),
            ResolvedLogDir::Custom(custom_log_dir) => Ok(BTreeMap::from([
                ("spark.eventLog.enabled".to_string(), "true".to_string()),
                ("spark.eventLog.dir".to_string(), custom_log_dir.to_string()),
            ])),
        }
    }

    pub fn volumes(&self, requested_secret_lifetime: &Duration) -> Result<Vec<Volume>, Error> {
        match self {
            ResolvedLogDir::S3(s3_log_dir) => s3_log_dir.volumes(requested_secret_lifetime),
            ResolvedLogDir::Custom(_) => Ok(vec![]),
        }
    }

    pub fn volume_mounts(&self) -> Vec<VolumeMount> {
        match self {
            ResolvedLogDir::S3(s3_log_dir) => s3_log_dir.volume_mounts(),
            ResolvedLogDir::Custom(_) => vec![],
        }
    }

    pub fn credentials_volume(&self) -> Result<Option<Volume>, Error> {
        match self {
            ResolvedLogDir::S3(s3_log_dir) => s3_log_dir.credentials_volume(),
            ResolvedLogDir::Custom(_) => Ok(None),
        }
    }

    pub fn credentials_volume_mount(&self) -> Option<VolumeMount> {
        match self {
            ResolvedLogDir::S3(s3_log_dir) => s3_log_dir.credentials_volume_mount(),
            ResolvedLogDir::Custom(_) => None,
        }
    }

    pub fn credentials_mount_path(&self) -> Option<String> {
        match self {
            ResolvedLogDir::S3(s3_log_dir) => s3_log_dir.credentials_mount_path(),
            ResolvedLogDir::Custom(_) => None,
        }
    }
}

pub struct S3LogDir {
    pub bucket: ResolvedS3Bucket,
    pub prefix: String,
}

impl S3LogDir {
    pub async fn resolve(
        log_file_dir: &S3LogFileDirectorySpec,
        namespace: Option<String>,
        client: &stackable_operator::client::Client,
    ) -> Result<S3LogDir, Error> {
        let bucket = log_file_dir
            .bucket
            .clone()
            // TODO (@NickLarsenNZ): Explain this unwrap. Either convert to expect, or gracefully handle the error.
            .resolve(client, namespace.unwrap().as_str())
            .await
            .context(ConfigureS3Snafu)?;

        if bucket.connection.tls.uses_tls() && !bucket.connection.tls.uses_tls() {
            return S3TlsNoVerificationNotSupportedSnafu.fail();
        }

        Ok(S3LogDir {
            bucket,
            prefix: log_file_dir.prefix.to_owned(),
        })
    }

    /// Constructs the properties needed for loading event logs from S3.
    /// These properties are later written in the `SPARK_DEFAULTS_FILE_NAME` file.
    ///
    /// The following properties related to credentials are not included:
    /// * spark.hadoop.fs.s3a.aws.credentials.provider
    /// * spark.hadoop.fs.s3a.access.key
    /// * spark.hadoop.fs.s3a.secret.key
    ///
    /// Instead, the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set
    /// on the container start command.
    pub fn history_server_spark_config(&self) -> Result<BTreeMap<String, String>, Error> {
        let connection = &self.bucket.connection;

        let config = BTreeMap::from([
            ("spark.history.fs.logDirectory".to_string(), self.url()),
            (
                "spark.hadoop.fs.s3a.endpoint".to_string(),
                connection.endpoint().context(ConfigureS3Snafu)?.to_string(),
            ),
            (
                "spark.hadoop.fs.s3a.path.style.access".to_string(),
                (connection.access_style == S3AccessStyle::Path).to_string(),
            ),
            (
                "spark.hadoop.fs.s3a.endpoint.region".to_string(),
                connection.region.name.clone(),
            ),
        ]);

        Ok(config)
    }

    pub fn application_spark_config(&self) -> Result<BTreeMap<String, String>, Error> {
        let mut result = BTreeMap::from([
            ("spark.eventLog.enabled".to_string(), "true".to_string()),
            ("spark.eventLog.dir".to_string(), self.url()),
        ]);

        let connection = &self.bucket.connection;
        let bucket_name = &self.bucket.bucket_name;
        result.insert(
            format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.endpoint"),
            connection.endpoint().context(ConfigureS3Snafu)?.to_string(),
        );
        result.insert(
            format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.path.style.access"),
            (connection.access_style == S3AccessStyle::Path).to_string(),
        );
        result.insert(
            format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.region"),
            connection.region.name.clone(),
        );
        if let Some(secret_dir) = self.credentials_mount_path() {
            // We don't use the credentials at all here but assume they are available
            result.insert(
                format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.access.key"),
                format!("\"$(cat {secret_dir}/{ACCESS_KEY_ID})\""),
            );
            result.insert(
                format!("spark.hadoop.fs.s3a.bucket.{bucket_name}.secret.key"),
                format!("\"$(cat {secret_dir}/{SECRET_ACCESS_KEY})\""),
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

        Ok(result)
    }

    fn url(&self) -> String {
        format!(
            "s3a://{bucket_name}/{prefix}",
            bucket_name = self.bucket.bucket_name,
            prefix = self.prefix
        )
    }

    pub fn volumes(&self, requested_secret_lifetime: &Duration) -> Result<Vec<Volume>, Error> {
        let mut volumes: Vec<Volume> = self.credentials_volume()?.into_iter().collect();

        if let Some(secret_name) = tlscerts::tls_secret_name(&self.bucket.connection) {
            volumes.push(
                VolumeBuilder::new(secret_name)
                    .ephemeral(
                        SecretOperatorVolumeSourceBuilder::new(secret_name)
                            .with_format(SecretFormat::TlsPkcs12)
                            .with_auto_tls_cert_lifetime(*requested_secret_lifetime)
                            .build()
                            .context(TlsCertSecretClassVolumeBuildSnafu)?,
                    )
                    .build(),
            );
        }
        Ok(volumes)
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

    pub fn credentials_volume(&self) -> Result<Option<Volume>, Error> {
        self.credentials()
            .map(|credentials| {
                credentials
                    .to_volume(credentials.secret_class.as_ref())
                    .context(CredentialsVolumeBuildSnafu)
            })
            .transpose()
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
        self.bucket.connection.credentials.clone()
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
