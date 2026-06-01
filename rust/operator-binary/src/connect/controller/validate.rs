//! The validate step in the SparkConnectServer controller.
//!
//! Resolves the product image and the server/executor configs. Does not touch K8s.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
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
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ValidatedSparkConnectServer {
    pub resolved_s3: ResolvedS3,
    pub resolved_product_image: ResolvedProductImage,
    pub server_config: v1alpha1::ServerConfig,
    pub executor_config: v1alpha1::ExecutorConfig,
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

    Ok(ValidatedSparkConnectServer {
        resolved_s3: dereferenced.resolved_s3,
        resolved_product_image,
        server_config,
        executor_config,
    })
}
