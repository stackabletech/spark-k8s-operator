use crate::{
    constants::*,
    history::{
        LogFileDirectorySpec::{self, S3},
        S3LogFileDirectorySpec,
    },
    tlscerts,
};
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
};
use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use strum::{EnumDiscriminants, IntoStaticStr};

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

pub struct S3LogDir {
    pub bucket: ResolvedS3Bucket,
    pub prefix: String,
}

impl S3LogDir {
    pub async fn resolve(
        log_file_dir: Option<&LogFileDirectorySpec>,
        namespace: Option<String>,
        client: &stackable_operator::client::Client,
    ) -> Result<Option<S3LogDir>, Error> {
        #[allow(irrefutable_let_patterns)]
        let (bucket, prefix) = if let Some(S3(S3LogFileDirectorySpec {
            bucket: bucket_def,
            prefix,
        })) = log_file_dir
        {
            (
                bucket_def
                    .clone()
                    .resolve(client, namespace.unwrap().as_str())
                    .await
                    .context(ConfigureS3Snafu)?,
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

        if bucket.connection.tls.uses_tls() && !bucket.connection.tls.uses_tls() {
            return S3TlsNoVerificationNotSupportedSnafu.fail();
        }

        Ok(Some(S3LogDir { bucket, prefix }))
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

        Ok(BTreeMap::from([
            ("spark.history.fs.logDirectory".to_string(), self.url()),
            (
                "spark.hadoop.fs.s3a.endpoint".to_string(),
                connection.endpoint().context(ConfigureS3Snafu)?.to_string(),
            ),
            (
                "spark.hadoop.fs.s3a.path.style.access".to_string(),
                (connection.access_style == S3AccessStyle::Path).to_string(),
            ),
        ]))
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

    pub fn volumes(&self) -> Result<Vec<Volume>, Error> {
        let mut volumes: Vec<Volume> = self.credentials_volume()?.into_iter().collect();

        if let Some(secret_name) = tlscerts::tls_secret_name(&self.bucket.connection) {
            volumes.push(
                VolumeBuilder::new(secret_name)
                    .ephemeral(
                        SecretOperatorVolumeSourceBuilder::new(secret_name)
                            .with_format(SecretFormat::TlsPkcs12)
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
