use snafu::Snafu;
use stackable_operator::{
    builder, builder::pod::PodBuilder, k8s_openapi::api::core::v1::PodTemplateSpec,
};

use crate::connect::crd::v1alpha1;

type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to serialize spark connect executor pod template for {name}"))]
    PodTemplateSerde {
        source: serde_yaml::Error,
        name: String,
    },

    #[snafu(display("failed to build spark connect pod template config map for {name}"))]
    PodTemplateConfigMap {
        source: stackable_operator::builder::configmap::Error,
        name: String,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build metadata for spark connect pod template config map {name}"))]
    MetadataBuild {
        source: builder::meta::Error,
        name: String,
    },
}

#[allow(clippy::result_large_err)]
pub fn build_executor_pod_template(
    _scs: &v1alpha1::SparkConnectServer,
    _config: &v1alpha1::ExecutorConfig,
) -> Result<PodTemplateSpec, Error> {
    let template = PodBuilder::new();

    Ok(template.build_template())
}
