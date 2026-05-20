//! The validate step in the SparkApplication controller.
//!
//! Synchronously validates the [`super::dereference::DereferencedSparkApplication`] and
//! resolves the product image and product config. Does not touch the Kubernetes API.

use product_config::ProductConfigManager;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    product_config_utils::ValidatedRoleConfigByPropertyKind,
};

use crate::{
    crd::constants::CONTAINER_IMAGE_BASE_NAME,
    spark_k8s_controller::dereference::DereferencedSparkApplication,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig { source: crate::crd::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Inputs the rest of `reconcile` needs after dereferencing.
pub struct ValidatedSparkApplication {
    pub dereferenced: DereferencedSparkApplication,
    pub resolved_product_image: ResolvedProductImage,
    pub product_config: ValidatedRoleConfigByPropertyKind,
}

pub fn validate(
    dereferenced: DereferencedSparkApplication,
    operator_environment: &OperatorEnvironmentOptions,
    product_config: &ProductConfigManager,
) -> Result<ValidatedSparkApplication> {
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

    let product_config = dereferenced
        .spark_application
        .validated_role_config(&resolved_product_image, product_config)
        .context(InvalidProductConfigSnafu)?;

    Ok(ValidatedSparkApplication {
        dereferenced,
        resolved_product_image,
        product_config,
    })
}
