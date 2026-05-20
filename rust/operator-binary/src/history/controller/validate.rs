//! The validate step in the SparkHistoryServer controller.
//!
//! Resolves the product image and runs role/role-group config validation.
//! Does not touch the Kubernetes API.

use product_config::ProductConfigManager;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    product_config_utils::ValidatedRoleConfigByPropertyKind,
};

use crate::{
    crd::{constants::CONTAINER_IMAGE_BASE_NAME, history::v1alpha1},
    history::controller::dereference::DereferencedSparkHistoryServer,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig { source: crate::crd::history::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ValidatedSparkHistoryServer {
    pub dereferenced: DereferencedSparkHistoryServer,
    pub resolved_product_image: ResolvedProductImage,
    pub product_config: ValidatedRoleConfigByPropertyKind,
}

pub fn validate(
    shs: &v1alpha1::SparkHistoryServer,
    dereferenced: DereferencedSparkHistoryServer,
    operator_environment: &OperatorEnvironmentOptions,
    product_config: &ProductConfigManager,
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

    let product_config_validated = shs
        .validated_role_config(&resolved_product_image, product_config)
        .context(InvalidProductConfigSnafu)?;

    Ok(ValidatedSparkHistoryServer {
        dereferenced,
        resolved_product_image,
        product_config: product_config_validated,
    })
}
