//! The validate step in the SparkConnectServer controller.
//!
//! Resolves the product image and the server/executor configs.
//! Does not touch the Kubernetes API.

use std::borrow::Cow;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    kube::Resource,
    v2::{
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ClusterName,
        },
    },
};

use crate::{
    connect::{
        controller::dereference::DereferencedSparkConnectServer,
        crd::{self, v1alpha1},
        s3::ResolvedS3,
    },
    crd::constants::CONTAINER_IMAGE_BASE_NAME,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to resolve server config"))]
    ServerConfig { source: crd::Error },

    #[snafu(display("failed to resolve executor config"))]
    ExecutorConfig { source: crd::Error },

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
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ValidatedSparkConnectServer {
    metadata: ObjectMeta,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    pub resolved_s3: ResolvedS3,
    pub resolved_product_image: ResolvedProductImage,
    pub server_config: v1alpha1::ServerConfig,
    pub executor_config: v1alpha1::ExecutorConfig,
}

impl Resource for ValidatedSparkConnectServer {
    type DynamicType = ();
    type Scope = <v1alpha1::SparkConnectServer as Resource>::Scope;

    fn kind(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkConnectServer as Resource>::kind(&())
    }

    fn group(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkConnectServer as Resource>::group(&())
    }

    fn version(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkConnectServer as Resource>::version(&())
    }

    fn plural(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkConnectServer as Resource>::plural(&())
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

pub fn validate(
    scs: &v1alpha1::SparkConnectServer,
    dereferenced: DereferencedSparkConnectServer,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedSparkConnectServer> {
    let resolved_product_image = scs
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let server_config = scs.server_config().context(ServerConfigSnafu)?;
    let executor_config = scs.executor_config().context(ExecutorConfigSnafu)?;
    let name = get_cluster_name(scs).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(scs).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(scs).context(ResolveUidSnafu)?;

    Ok(ValidatedSparkConnectServer {
        metadata: scs.meta().clone(),
        name,
        namespace,
        uid,
        resolved_s3: dereferenced.resolved_s3,
        resolved_product_image,
        server_config,
        executor_config,
    })
}
