//! The validate step in the SparkApplication controller.
//!
//! Synchronously validates the [`super::dereference::DereferencedSparkApplication`] and
//! resolves the product image. Does not touch the Kubernetes API.

use std::{borrow::Cow, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    cli::OperatorEnvironmentOptions,
    commons::{
        product_image_selection::{self, ResolvedProductImage},
        tls_verification::TlsVerification,
    },
    crd::s3,
    k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    kube::Resource,
    kvp::Labels,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::meta::ownerreference_from_resource,
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        kvp::label::recommended_labels,
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::{
                ClusterName, ControllerName, OperatorName, ProductName, ProductVersion,
                RoleGroupName, RoleName,
            },
        },
    },
};

use crate::{
    crd::{
        constants::{APP_NAME, CONTAINER_IMAGE_BASE_NAME, OPERATOR_NAME, SPARK_CONTROLLER_NAME},
        logdir::ResolvedLogDir,
        v1alpha1,
    },
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

    #[snafu(display("failed to resolve uid"))]
    ResolveUid {
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
    pub uid: Uid,
    // Still carried in full because `reconcile` builds the submit/driver pod from the whole spec.
    pub spark_application: v1alpha1::SparkApplication,
    pub resolved_template_refs: Vec<v1alpha1::ResolvedSparkApplicationTemplate>,
    pub s3_connection: Option<s3::v1alpha1::ConnectionSpec>,
    pub log_dir: Option<ResolvedLogDir>,
    pub resolved_product_image: ResolvedProductImage,
}

impl NameIsValidLabelValue for ValidatedSparkApplication {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

impl HasName for ValidatedSparkApplication {
    fn to_name(&self) -> String {
        String::from(&self.name)
    }
}

impl HasUid for ValidatedSparkApplication {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
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

impl ValidatedSparkApplication {
    /// Recommended labels for a resource fulfilling the given `role` within the SparkApplication.
    ///
    /// A SparkApplication has no Stackable role groups, so the role group label is fixed to the
    /// controller name (preserving the previous `build_recommended_labels` behaviour). `role` is a
    /// free-form component name such as "spark", "spark-submit" or "role-binding".
    pub(crate) fn recommended_labels(&self, role: &str) -> Labels {
        // `app_version_label_value` is constructed to be a valid label value, so it is also a
        // valid `ProductVersion`.
        let product_version =
            ProductVersion::from_str(&self.resolved_product_image.app_version_label_value)
                .expect("the app version label value is a valid product version");
        let role_name = RoleName::from_str(role).expect("the role is a valid role name");
        let role_group = RoleGroupName::from_str(SPARK_CONTROLLER_NAME)
            .expect("SPARK_CONTROLLER_NAME is a valid role group name");
        recommended_labels(
            self,
            &product_name(),
            &product_version,
            &operator_name(),
            &controller_name(),
            &role_name,
            &role_group,
        )
    }

    /// Object metadata for a child resource named `name`, owned by this SparkApplication and
    /// carrying the recommended labels for the given `role`. Returns the builder so callers can add
    /// extra labels before building.
    pub(crate) fn object_meta(&self, name: impl Into<String>, role: &str) -> ObjectMetaBuilder {
        let mut builder = ObjectMetaBuilder::new();
        builder
            .namespace(self.namespace.clone())
            .name(name)
            .ownerreference(ownerreference_from_resource(self, None, Some(true)))
            .with_labels(self.recommended_labels(role));
        builder
    }
}

/// The product name (`spark-k8s`) as a type-safe label value.
pub(crate) fn product_name() -> ProductName {
    ProductName::from_str(APP_NAME).expect("APP_NAME is a valid product name")
}

/// The operator name as a type-safe label value.
pub(crate) fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub(crate) fn controller_name() -> ControllerName {
    ControllerName::from_str(SPARK_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
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
    let uid = get_uid(&dereferenced.spark_application).context(ResolveUidSnafu)?;
    let metadata = dereferenced.spark_application.meta().clone();

    Ok(ValidatedSparkApplication {
        metadata,
        name,
        namespace,
        uid,
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
