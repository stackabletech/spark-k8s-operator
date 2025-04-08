use std::fmt::Display;

use snafu::Snafu;
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
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
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub const LOG_FILE: &str = "spark.log4j2.xml";

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %p [%t] %c - %m%n";

/// Extend a ConfigMap with logging and Vector configurations
pub fn extend_config_map<C, K>(
    role_group: &RoleGroupRef<K>,
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
            product_logging::framework::create_vector_config(role_group, vector_log_config),
        );
    }

    Ok(())
}
