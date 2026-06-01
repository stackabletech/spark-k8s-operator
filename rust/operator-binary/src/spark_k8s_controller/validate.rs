//! The validate step in the SparkApplication controller.
//!
//! Synchronously validates the [`super::dereference::DereferencedSparkApplication`] and
//! resolves the product image and product config. Does not touch the Kubernetes API.

use product_config::ProductConfigManager;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::{
        product_image_selection::{self, ResolvedProductImage},
        tls_verification::TlsVerification,
    },
    crd::s3,
    product_config_utils::ValidatedRoleConfigByPropertyKind,
};

use crate::{
    crd::{constants::CONTAINER_IMAGE_BASE_NAME, logdir::ResolvedLogDir},
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

    #[snafu(display("S3 TLS with verification disabled is not supported ({context})"))]
    S3TlsNoVerificationNotSupported { context: String },
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

fn reject_tls_no_verification(conn: &s3::v1alpha1::ConnectionSpec, context: &str) -> Result<()> {
    if let Some(tls) = &conn.tls.tls {
        if matches!(&tls.verification, TlsVerification::None {}) {
            return S3TlsNoVerificationNotSupportedSnafu {
                context: context.to_owned(),
            }
            .fail();
        }
    }
    Ok(())
}
