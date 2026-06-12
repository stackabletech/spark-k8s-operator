//! The validate step in the SparkApplication controller.
//!
//! Synchronously validates the [`super::dereference::DereferencedSparkApplication`] and
//! resolves the product image. Does not touch the Kubernetes API.

use std::borrow::Cow;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::{
        product_image_selection::{self, ResolvedProductImage},
        tls_verification::TlsVerification,
    },
    crd::s3,
    k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    kube::Resource,
    v2::{
        controller_utils::{get_cluster_name, get_namespace},
        types::{kubernetes::NamespaceName, operator::ClusterName},
    },
};

use crate::{
    crd::{constants::CONTAINER_IMAGE_BASE_NAME, logdir::ResolvedLogDir, v1alpha1},
    spark_k8s_controller::dereference::DereferencedSparkApplication,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to resolve cluster name"))]
    ResolveClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve namespace"))]
    ResolveNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("S3 TLS with verification disabled is not supported ({context})"))]
    S3TlsNoVerificationNotSupported { context: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Inputs the rest of `reconcile` needs after dereferencing.
pub struct ValidatedSparkApplication {
    /// Metadata mirroring the source [`v1alpha1::SparkApplication`] (name, namespace and UID), so
    /// this struct can be used as the owner of generated objects via its [`Resource`] impl.
    metadata: ObjectMeta,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    // Still carried in full because `reconcile` builds the submit/driver pod from the whole spec.
    pub spark_application: v1alpha1::SparkApplication,
    pub resolved_template_refs: Vec<v1alpha1::ResolvedSparkApplicationTemplate>,
    pub s3_connection: Option<s3::v1alpha1::ConnectionSpec>,
    pub log_dir: Option<ResolvedLogDir>,
    pub resolved_product_image: ResolvedProductImage,
}

impl Resource for ValidatedSparkApplication {
    type DynamicType = ();
    type Scope = <v1alpha1::SparkApplication as Resource>::Scope;

    fn kind(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkApplication as Resource>::kind(&())
    }

    fn group(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkApplication as Resource>::group(&())
    }

    fn version(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkApplication as Resource>::version(&())
    }

    fn plural(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkApplication as Resource>::plural(&())
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

pub fn validate(
    dereferenced: DereferencedSparkApplication,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedSparkApplication> {
    if let Some(conn) = &dereferenced.s3_connection {
        reject_tls_no_verification(conn, "S3 connection")?;
    }
    if let Some(ResolvedLogDir::S3(s3_log_dir)) = &dereferenced.log_dir {
        reject_tls_no_verification(&s3_log_dir.bucket.connection, "S3 log directory")?;
    }

    let resolved_product_image = dereferenced
        .spark_application
        .spec
        .spark_image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let name =
        get_cluster_name(&dereferenced.spark_application).context(ResolveClusterNameSnafu)?;
    let namespace =
        get_namespace(&dereferenced.spark_application).context(ResolveNamespaceSnafu)?;
    let metadata = dereferenced.spark_application.meta().clone();

    Ok(ValidatedSparkApplication {
        metadata,
        name,
        namespace,
        spark_application: dereferenced.spark_application,
        resolved_template_refs: dereferenced.resolved_template_refs,
        s3_connection: dereferenced.s3_connection,
        log_dir: dereferenced.log_dir,
        resolved_product_image,
    })
}

fn reject_tls_no_verification(conn: &s3::v1alpha1::ConnectionSpec, context: &str) -> Result<()> {
    if let Some(tls) = &conn.tls.tls
        && matches!(&tls.verification, TlsVerification::None {})
    {
        return S3TlsNoVerificationNotSupportedSnafu {
            context: context.to_owned(),
        }
        .fail();
    }
    Ok(())
}
