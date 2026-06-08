//! The validate step in the SparkHistoryServer controller.
//!
//! Resolves the product image.
//! Does not touch the Kubernetes API.

use std::collections::BTreeMap;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference,
    kube::{Resource, ResourceExt},
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

    #[snafu(display("object is missing metadata to build owner reference"))]
    MissingOwnerReference,

    #[snafu(display("object is missing namespace"))]
    MissingNamespace,

    #[snafu(display("invalid cleaner configuration"))]
    InvalidCleanerConfiguration { source: crate::crd::history::Error },

    #[snafu(display("invalid log directory settings"))]
    InvalidLogDirSettings { source: crate::crd::logdir::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ValidatedSparkHistoryServer {
    pub namespace: String,
    pub owner_reference: OwnerReference,
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

    let owner_reference = shs
        .controller_owner_ref(&())
        .context(MissingOwnerReferenceSnafu)?;
    let namespace = shs.namespace().context(MissingNamespaceSnafu)?;

    let cleaner_rolegroup_name = shs
        .cleaner_rolegroup_name()
        .context(InvalidCleanerConfigurationSnafu)?;

    let log_dir_settings = dereferenced
        .log_dir
        .history_server_spark_config()
        .context(InvalidLogDirSettingsSnafu)?;

    Ok(ValidatedSparkHistoryServer {
        namespace,
        owner_reference,
        cleaner_rolegroup_name,
        spark_conf: shs.spec.spark_conf.clone(),
        log_dir: dereferenced.log_dir,
        log_dir_settings,
        resolved_product_image,
    })
}
