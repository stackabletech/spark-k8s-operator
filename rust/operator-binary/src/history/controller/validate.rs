//! The validate step in the SparkHistoryServer controller.
//!
//! Resolves the product image.
//! Does not touch the Kubernetes API.

use std::{borrow::Cow, collections::BTreeMap};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference},
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
    crd::{constants::CONTAINER_IMAGE_BASE_NAME, history::v1alpha1, logdir::ResolvedLogDir},
    history::controller::dereference::DereferencedSparkHistoryServer,
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

    #[snafu(display("invalid cleaner configuration"))]
    InvalidCleanerConfiguration { source: crate::crd::history::Error },

    #[snafu(display("invalid log directory settings"))]
    InvalidLogDirSettings { source: crate::crd::logdir::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ValidatedSparkHistoryServer {
    metadata: ObjectMeta,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    pub cleaner_rolegroup_name: Option<String>,
    pub spark_conf: BTreeMap<String, String>,
    pub resolved_product_image: ResolvedProductImage,
    // These two are a bit redundant right now.
    // This is a temporary situation until we remove all v1alpha1::SparkHistoryServer usages after validation.
    // Currently log_dir_settings is needed for  history::controller::build_configmap() function whereas log_dir
    // is needed for command args and volume mounts.
    pub log_dir: ResolvedLogDir,
    pub log_dir_settings: BTreeMap<String, String>,
}

impl ValidatedSparkHistoryServer {
    pub fn owner_reference(&self) -> OwnerReference {
        let mut owner_reference = self.controller_owner_ref(&()).unwrap_or(OwnerReference {
            api_version: v1alpha1::SparkHistoryServer::api_version(&()).to_string(),
            block_owner_deletion: Some(true),
            controller: Some(true),
            kind: v1alpha1::SparkHistoryServer::kind(&()).to_string(),
            name: String::from(&self.name),
            uid: String::from(&self.uid),
        });
        owner_reference.block_owner_deletion = Some(true);
        owner_reference
    }
}

impl Resource for ValidatedSparkHistoryServer {
    type DynamicType = ();
    type Scope = <v1alpha1::SparkHistoryServer as Resource>::Scope;

    fn kind(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkHistoryServer as Resource>::kind(&())
    }

    fn group(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkHistoryServer as Resource>::group(&())
    }

    fn version(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkHistoryServer as Resource>::version(&())
    }

    fn plural(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkHistoryServer as Resource>::plural(&())
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

pub fn validate(
    shs: &v1alpha1::SparkHistoryServer,
    dereferenced: DereferencedSparkHistoryServer,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedSparkHistoryServer> {
    let resolved_product_image = shs
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let name = get_cluster_name(shs).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(shs).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(shs).context(ResolveUidSnafu)?;

    let cleaner_rolegroup_name = shs
        .cleaner_rolegroup_name()
        .context(InvalidCleanerConfigurationSnafu)?;

    let log_dir_settings = dereferenced
        .log_dir
        .history_server_spark_config()
        .context(InvalidLogDirSettingsSnafu)?;

    Ok(ValidatedSparkHistoryServer {
        metadata: shs.meta().clone(),
        name,
        namespace,
        uid,
        cleaner_rolegroup_name,
        spark_conf: shs.spec.spark_conf.clone(),
        log_dir: dereferenced.log_dir,
        log_dir_settings,
        resolved_product_image,
    })
}
