//! The dereference step in the SparkApplication controller.
//!
//! Fetches all Kubernetes objects referenced by the SparkApplication spec (templates, S3
//! connection, log directory) and returns them in [`DereferencedSparkApplication`].
//! Synchronous validation belongs in the sibling [`super::validate`] module.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    crd::{openlineage, s3},
};

use crate::{
    crd::{
        logdir::ResolvedLogDir,
        template_spec::{self},
        v1alpha1,
    },
    openlineage::{ResolvedOpenLineageAuth, openlineage_auth_secret_name},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to merge application templates"))]
    MergeApplicationTemplates { source: template_spec::Error },

    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: stackable_operator::crd::s3::v1alpha1::ConnectionError,
    },

    #[snafu(display("failed to resolve OpenLineage connection"))]
    ResolveOpenLineageConnection {
        source: stackable_operator::crd::openlineage::v1alpha1::OpenLineageError,
    },

    #[snafu(display("failed to resolve the OpenLineage AuthenticationClass"))]
    ResolveOpenLineageAuthClass {
        source: stackable_operator::crd::openlineage::v1alpha1::OpenLineageError,
    },

    #[snafu(display(
        "unsupported AuthenticationClass provider {provider:?} for OpenLineage; only the Static provider is supported"
    ))]
    UnsupportedOpenLineageAuthProvider { provider: String },

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
    /// Resolved OpenLineage backend connection, if `spec.openLineage` is set.
    pub open_lineage_connection: Option<openlineage::ResolvedOpenLineageConnection>,
    /// Resolved OpenLineage authentication, if the connection references an `AuthenticationClass`.
    pub open_lineage_auth: Option<ResolvedOpenLineageAuth>,
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

    // 3. OpenLineage connection (inline or referenced `OpenLineageConnection`).
    let open_lineage_connection = match merged_app.spec.open_lineage.as_ref() {
        Some(job) => Some(
            job.connection
                .clone()
                .resolve(client, namespace)
                .await
                .context(ResolveOpenLineageConnectionSnafu)?,
        ),
        None => None,
    };

    // 3b. OpenLineage authentication: resolve the connection's `authenticationClassRef` (if any)
    //     and extract the credentials Secret. Only the Static provider is supported.
    let open_lineage_auth = match &open_lineage_connection {
        Some(connection) => {
            match connection
                .resolve_authentication_class(client)
                .await
                .context(ResolveOpenLineageAuthClassSnafu)?
            {
                Some(auth_class) => {
                    let secret_name =
                        openlineage_auth_secret_name(&auth_class).map_err(|provider| {
                            UnsupportedOpenLineageAuthProviderSnafu { provider }.build()
                        })?;
                    Some(ResolvedOpenLineageAuth { secret_name })
                }
                None => None,
            }
        }
        None => None,
    };

    // 4. Log directory (also pulls S3Bucket + TLS secret internally).
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
        open_lineage_connection,
        open_lineage_auth,
        log_dir,
    })
}
