use std::collections::BTreeMap;

use snafu::ResultExt;
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    v2::{
        builder::meta::ownerreference_from_resource, config_file_writer::to_java_properties_string,
        types::operator::RoleGroupName,
    },
};

use crate::{
    crd::{
        constants::{
            JVM_SECURITY_PROPERTIES_FILE, LOG4J2_CONFIG_FILE, SPARK_DEFAULTS_FILE_NAME,
            SPARK_ENV_SH_FILE_NAME, default_jvm_security_properties,
        },
        history::SparkHistoryServerContainer,
        to_spark_env_sh_string,
    },
    history::controller::{
        Error, InvalidConfigMapSnafu, InvalidSparkDefaultsSnafu, JvmSecurityPropertiesSnafu,
        validate::{self, ValidatedHistoryRoleGroup},
    },
    product_logging::{self},
};

#[allow(clippy::result_large_err)]
pub(crate) fn build_config_map(
    validated: &validate::ValidatedSparkHistoryServer,
    role_group_name: &RoleGroupName,
    rg: &ValidatedHistoryRoleGroup,
) -> Result<ConfigMap, Error> {
    let cm_name = validated
        .resource_names(role_group_name)
        .role_group_config_map()
        .to_string();

    let spark_defaults = to_java_properties_string(
        spark_defaults(validated, role_group_name)
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v))),
    )
    .context(InvalidSparkDefaultsSnafu)?;

    let mut jvm_sec_props = default_jvm_security_properties();
    jvm_sec_props.extend(
        rg.config
            .config_overrides
            .security_properties
            .overrides
            .clone(),
    );

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .namespace(validated.namespace.clone())
                .name(&cm_name)
                .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
                .labels(validated.recommended_labels(role_group_name))
                .build(),
        )
        .add_data(SPARK_DEFAULTS_FILE_NAME, spark_defaults)
        .add_data(
            SPARK_ENV_SH_FILE_NAME,
            to_spark_env_sh_string(rg.config.config_overrides.spark_env_sh.overrides.iter()),
        )
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                JvmSecurityPropertiesSnafu {
                    rolegroup: role_group_name.to_string(),
                }
            })?,
        );

    if let Some(log4j2) = product_logging::build_log4j2(
        &rg.config.config.logging,
        SparkHistoryServerContainer::SparkHistory,
    ) {
        cm_builder.add_data(LOG4J2_CONFIG_FILE, log4j2);
    }
    if rg.config.config.logging.enable_vector_agent {
        cm_builder.add_data(
            VECTOR_CONFIG_FILE,
            product_logging::vector_config_file_content(),
        );
    }

    cm_builder
        .build()
        .context(InvalidConfigMapSnafu { name: cm_name })
}

fn spark_defaults(
    validated: &validate::ValidatedSparkHistoryServer,
    role_group_name: &RoleGroupName,
) -> BTreeMap<String, Option<String>> {
    let mut default_properties = validated.cluster_config.log_dir_settings.clone();

    // add cleaner spark settings if requested
    default_properties.extend(cleaner_config(validated, role_group_name));

    // add user provided configuration. These can overwrite everything.
    default_properties.extend(validated.cluster_config.spark_conf.clone());

    default_properties
        .into_iter()
        .map(|(key, value)| (key, Some(value)))
        .collect()
}

/// Return the Spark properties for the cleaner role group (if any).
fn cleaner_config(
    validated: &validate::ValidatedSparkHistoryServer,
    role_group_name: &RoleGroupName,
) -> BTreeMap<String, String> {
    match validated.cluster_config.cleaner_rolegroup_name.as_ref() {
        Some(cleaner_rolegroup) if cleaner_rolegroup == role_group_name.as_ref() => {
            BTreeMap::from([(
                "spark.history.fs.cleaner.enabled".to_string(),
                "true".to_string(),
            )])
        }
        _ => BTreeMap::new(),
    }
}
