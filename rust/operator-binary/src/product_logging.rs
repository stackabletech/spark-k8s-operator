use std::fmt::Display;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    client::Client,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::Resource,
    memory::BinaryMultiple,
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
    },
    role_utils::RoleGroupRef,
};

use crate::crd::constants::{LOG4J2_CONFIG_FILE, MAX_SPARK_LOG_FILES_SIZE, VOLUME_MOUNT_PATH_LOG};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to retrieve the ConfigMap {cm_name}"))]
    ConfigMapNotFound {
        source: stackable_operator::client::Error,
        cm_name: String,
    },

    #[snafu(display("failed to retrieve the entry {entry} for ConfigMap {cm_name}"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },

    #[snafu(display("vectorAggregatorConfigMapName must be set"))]
    MissingVectorAggregatorAddress,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub const LOG_FILE: &str = "spark.log4j2.xml";

const VECTOR_AGGREGATOR_CM_ENTRY: &str = "ADDRESS";
const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %p [%t] %c - %m%n";

/// Return the address of the Vector aggregator if the corresponding ConfigMap name is given in the
/// cluster spec
pub async fn resolve_vector_aggregator_address(
    client: &Client,
    namespace: &str,
    vector_aggregator_config_map_name: Option<&str>,
) -> Result<Option<String>> {
    let vector_aggregator_address =
        if let Some(vector_aggregator_config_map_name) = vector_aggregator_config_map_name {
            let vector_aggregator_address = client
                .get::<ConfigMap>(vector_aggregator_config_map_name, namespace)
                .await
                .context(ConfigMapNotFoundSnafu {
                    cm_name: vector_aggregator_config_map_name.to_string(),
                })?
                .data
                .and_then(|mut data| data.remove(VECTOR_AGGREGATOR_CM_ENTRY))
                .context(MissingConfigMapEntrySnafu {
                    entry: VECTOR_AGGREGATOR_CM_ENTRY,
                    cm_name: vector_aggregator_config_map_name.to_string(),
                })?;
            Some(vector_aggregator_address)
        } else {
            None
        };

    Ok(vector_aggregator_address)
}

/// Extend a ConfigMap with logging and Vector configurations
pub fn extend_config_map<C, K>(
    role_group: &RoleGroupRef<K>,
    vector_aggregator_address: Option<&str>,
    logging: &Logging<C>,
    main_container: C,
    vector_container: C,
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()>
where
    C: Clone + Ord + Display,
    K: Resource,
{
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&main_container)
    {
        cm_builder.add_data(
            LOG4J2_CONFIG_FILE,
            product_logging::framework::create_log4j2_config(
                &format!("{VOLUME_MOUNT_PATH_LOG}/{main_container}"),
                LOG_FILE,
                MAX_SPARK_LOG_FILES_SIZE
                    .scale_to(BinaryMultiple::Mebi)
                    .floor()
                    .value as u32,
                CONSOLE_CONVERSION_PATTERN,
                log_config,
            ),
        );
    }

    let vector_log_config = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&vector_container)
    {
        Some(log_config)
    } else {
        None
    };

    if logging.enable_vector_agent {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(
                role_group,
                vector_aggregator_address.context(MissingVectorAggregatorAddressSnafu)?,
                vector_log_config,
            ),
        );
    }

    Ok(())
}
