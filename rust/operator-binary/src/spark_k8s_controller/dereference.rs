//! The dereference step in the SparkApplication controller.
//!
//! Fetches all Kubernetes objects referenced by the SparkApplication spec (templates, S3
//! connection, log directory) and returns them in [`DereferencedSparkApplication`].
//! Synchronous validation belongs in the sibling [`super::validate`] module.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    commons::tls_verification::{CaCert, TlsVerification},
    crd::s3,
};

use crate::crd::{
    logdir::ResolvedLogDir,
    template_spec::{self},
    v1alpha1,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to merge application templates"))]
    MergeApplicationTemplates { source: template_spec::Error },

    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: stackable_operator::crd::s3::v1alpha1::ConnectionError,
    },

    #[snafu(display("S3 TLS verification with no verification is not supported"))]
    S3TlsNoVerificationNotSupported,

    #[snafu(display("failed to resolve log directory"))]
    LogDir { source: crate::crd::logdir::Error },

    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Kubernetes objects referenced from a SparkApplication, already fetched.
pub struct DereferencedSparkApplication {
    /// SparkApplication after merging any referenced templates.
    pub spark_application: v1alpha1::SparkApplication,
    /// Resolved template references for status reporting.
    pub resolved_template_refs: Vec<v1alpha1::ResolvedSparkApplicationTemplate>,
    /// Resolved S3 connection, if `spec.s3connection` is set.
    pub s3_connection: Option<s3::v1alpha1::ConnectionSpec>,
    /// Resolved log directory, if `spec.log_file_directory` is set.
    pub log_dir: Option<ResolvedLogDir>,
}

/// Fetches all Kubernetes objects referenced from the given SparkApplication.
pub async fn dereference(
    client: &Client,
    spark_application: &v1alpha1::SparkApplication,
) -> Result<DereferencedSparkApplication> {
    // 1. Template merging — must happen first so subsequent lookups see the merged spec.
    let merged = template_spec::merge_application_templates(client, spark_application)
        .await
        .context(MergeApplicationTemplatesSnafu)?;
    let merged_app = merged.app.unwrap_or_else(|| spark_application.clone());
    let resolved_template_refs = merged.resolved_template_ref;

    let namespace = merged_app
        .metadata
        .namespace
        .as_deref()
        .ok_or(Error::ObjectHasNoNamespace)?;

    // 2. S3 connection.
    let s3_connection = match merged_app.spec.s3connection.as_ref() {
        Some(s3bd) => Some(
            s3bd.clone()
                .resolve(client, namespace)
                .await
                .context(ConfigureS3ConnectionSnafu)?,
        ),
        None => None,
    };

    // Early "no verification" rejection — preserves today's behavior (was inline at
    // spark_k8s_controller.rs:278–290 before this refactor).
    if let Some(conn) = s3_connection.as_ref() {
        if let Some(tls) = &conn.tls.tls {
            match &tls.verification {
                TlsVerification::None {} => return S3TlsNoVerificationNotSupportedSnafu.fail(),
                TlsVerification::Server(server_verification) => match &server_verification.ca_cert {
                    CaCert::WebPki {} => {}
                    CaCert::SecretClass(_) => {}
                },
            }
        }
    }

    // 3. Log directory (also pulls S3Bucket + TLS secret internally).
    let log_dir = match merged_app.spec.log_file_directory.as_ref() {
        Some(log_file_dir) => Some(
            ResolvedLogDir::resolve(log_file_dir, merged_app.metadata.namespace.clone(), client)
                .await
                .context(LogDirSnafu)?,
        ),
        None => None,
    };

    Ok(DereferencedSparkApplication {
        spark_application: merged_app,
        resolved_template_refs,
        s3_connection,
        log_dir,
    })
}
