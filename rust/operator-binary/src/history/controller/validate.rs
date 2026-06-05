//! The validate step in the SparkHistoryServer controller.
//!
//! Resolves the product image.
//! Does not touch the Kubernetes API.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
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
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ValidatedSparkHistoryServer {
    pub log_dir: ResolvedLogDir,
    pub resolved_product_image: ResolvedProductImage,
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

    Ok(ValidatedSparkHistoryServer {
        log_dir: dereferenced.log_dir,
        resolved_product_image,
    })
}
