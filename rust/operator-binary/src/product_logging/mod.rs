use std::fmt::Display;

use stackable_operator::{
    memory::BinaryMultiple,
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
    },
};

use crate::crd::constants::{MAX_SPARK_LOG_FILES_SIZE, VOLUME_MOUNT_PATH_LOG};

pub const LOG_FILE: &str = "spark.log4j2.xml";

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %p [%t] %c - %m%n";

/// The static Vector agent configuration (`vector.yaml`).
///
/// Cluster/role/role-group/namespace metadata and the file-log level are injected at runtime via
/// the env vars set on the Vector sidecar container (`${CLUSTER_NAME}`, `${ROLE_NAME}`,
/// `${ROLE_GROUP_NAME}`, `${NAMESPACE}`, `${VECTOR_FILE_LOG_LEVEL}`, `${VECTOR_AGGREGATOR_ADDRESS}`,
/// …), so this file is role-group-agnostic.
const VECTOR_CONFIG: &str = include_str!("vector.yaml");

/// Returns the Vector agent config (`vector.yaml`) content.
pub fn vector_config_file_content() -> String {
    VECTOR_CONFIG.to_owned()
}

/// Renders the `log4j2.properties` content for the given main container, if it uses the operator's
/// automatic logging configuration.
///
/// Returns `None` when the container references a custom log ConfigMap (or has no log config), in
/// which case no `log4j2.properties` should be added to the rolegroup `ConfigMap`.
pub fn build_log4j2<C>(logging: &Logging<C>, main_container: C) -> Option<String>
where
    C: Clone + Ord + Display,
{
    match logging.containers.get(&main_container) {
        Some(ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
        }) => Some(product_logging::framework::create_log4j2_config(
            &format!("{VOLUME_MOUNT_PATH_LOG}/{main_container}"),
            LOG_FILE,
            MAX_SPARK_LOG_FILES_SIZE
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32,
            CONSOLE_CONVERSION_PATTERN,
            log_config,
        )),
        _ => None,
    }
}
