use stackable_operator::{
    commons::{
        s3::{InlinedS3BucketSpec, S3AccessStyle, S3ConnectionSpec},
        secret_class::SecretClassVolume,
        tls::{CaCert, TlsVerification},
    },
    k8s_openapi::api::core::v1::{Volume, VolumeMount},
};
use stackable_spark_k8s_crd::{
    constants::*,
    history::{LogFileDirectorySpec::S3, S3LogFileDirectorySpec, SparkHistoryServer},
};
use std::collections::BTreeMap;

use snafu::{OptionExt, ResultExt, Snafu};
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
    bucket: InlinedS3BucketSpec,
    prefix: String,
}

impl S3LogDir {
    pub async fn resolve(
        shs: &SparkHistoryServer,
        client: &stackable_operator::client::Client,
    ) -> Result<S3LogDir, Error> {
        #[allow(irrefutable_let_patterns)]
        let (s3bucket, prefix) =
            if let S3(S3LogFileDirectorySpec { bucket, prefix }) = &shs.spec.log_file_directory {
                (
                    bucket
                        .resolve(client, shs.metadata.namespace.as_deref().unwrap())
                        .await
                        .context(S3BucketSnafu)
                        .ok(),
                    prefix.clone(),
                )
            } else {
                (None, "".to_string())
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
                            CaCert::SecretClass(_) => {
                                return S3TlsCaVerificationNotSupportedSnafu.fail()
                            }
                        }
                    }
                }
            }
        }

        if let Some(conn) = s3bucket.as_ref().and_then(|i| i.connection.as_ref()) {
            if conn.tls.as_ref().is_some() {
                tracing::warn!("The resource indicates S3-access should use TLS: TLS-verification has not yet been implemented \
            but an HTTPS-endpoint will be used!");
            }
        }
        Ok(S3LogDir {
            bucket: s3bucket.unwrap(),
            prefix,
        })
    }

    /// Constructs the properties needed for loading event logs from S3.
    /// These properties are later written in the `HISTORY_CONFIG_FILE_NAME_FULL` file.
    ///
    /// The following properties related to credentials are not included:
    /// * spark.hadoop.fs.s3a.aws.credentials.provider
    /// * spark.hadoop.fs.s3a.access.key
    /// * spark.hadoop.fs.s3a.secret.key
    /// instead, the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set
    /// on the container start command.
    pub fn spark_config(&self) -> BTreeMap<String, String> {
        let mut result = BTreeMap::new();

        result.insert(
            "spark.history.fs.logDirectory".to_string(),
            format!(
                "s3a://{}/{}",
                self.bucket.bucket_name.as_ref().unwrap().clone(), // this is guaranteed to exist at this point
                self.prefix
            ),
        );

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

    pub fn credentials_volume(&self) -> Option<Volume> {
        self.credentials()
            .map(|credentials| credentials.to_volume(VOLUME_NAME_S3_CREDENTIALS))
    }

    pub fn credentials_volume_mount(&self) -> Option<VolumeMount> {
        self.credentials().map(|_| VolumeMount {
            name: VOLUME_NAME_S3_CREDENTIALS.into(),
            mount_path: S3_SECRET_DIR_NAME.into(),
            ..VolumeMount::default()
        })
    }

    pub fn credentials(&self) -> Option<SecretClassVolume> {
        if let Some(&S3ConnectionSpec {
            credentials: Some(ref credentials),
            ..
        }) = self.bucket.connection.as_ref()
        {
            Some(credentials.clone())
        } else {
            None
        }
    }
}
