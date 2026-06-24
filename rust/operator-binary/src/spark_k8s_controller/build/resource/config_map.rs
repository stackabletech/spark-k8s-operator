use snafu::{OptionExt, ResultExt};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, pod::volume::VolumeBuilder},
    k8s_openapi::api::core::v1::{ConfigMap, EnvVar, ServiceAccount},
    product_logging::{
        framework::VECTOR_CONFIG_FILE,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    v2::config_file_writer::to_java_properties_string,
};

use crate::{
    crd::{
        constants::*,
        roles::{RoleConfig, SparkApplicationRole, SparkContainer},
        to_spark_env_sh_string, v1alpha1,
    },
    product_logging::{self},
    spark_k8s_controller::{
        CreateVolumesSnafu, JvmSecurityPropertiesSnafu, MissingSecretLifetimeSnafu,
        PodTemplateConfigMapSnafu, PodTemplateSerdeSnafu, Result, build::pod::pod_template,
        validate,
    },
};

pub(crate) fn pod_template_config_map(
    validated: &validate::ValidatedSparkApplication,
    role: SparkApplicationRole,
    merged_config: &RoleConfig,
    config_overrides: &v1alpha1::ConfigOverrides,
    env: &[EnvVar],
    service_account: &ServiceAccount,
) -> Result<ConfigMap> {
    let spark_application = &validated.spark_application;
    let s3conn = &validated.cluster_config.s3_connection;
    let logdir = &validated.cluster_config.log_dir;
    let cm_name = spark_application.pod_template_config_map_name(role.clone());

    let log_config_map = if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = merged_config.logging.containers.get(&SparkContainer::Spark)
    {
        config_map.into()
    } else {
        cm_name.clone()
    };

    let requested_secret_lifetime = merged_config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    let mut volumes = spark_application
        .volumes(
            s3conn,
            logdir,
            Some(&log_config_map),
            &requested_secret_lifetime,
        )
        .context(CreateVolumesSnafu)?;
    volumes.push(
        VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG.as_ref())
            .with_config_map(&cm_name)
            .build(),
    );

    let template = pod_template(
        validated,
        role.clone(),
        merged_config,
        volumes.as_ref(),
        env,
        service_account,
    )?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(validated.object_meta(&cm_name, "pod-templates").build())
        .add_data(
            POD_TEMPLATE_FILE,
            serde_yaml::to_string(&template).context(PodTemplateSerdeSnafu)?,
        );

    if let Some(log4j2) =
        product_logging::build_log4j2(&merged_config.logging, SparkContainer::Spark)
    {
        cm_builder.add_data(LOG4J2_CONFIG_FILE, log4j2);
    }
    if merged_config.logging.enable_vector_agent {
        cm_builder.add_data(
            VECTOR_CONFIG_FILE,
            product_logging::vector_config_file_content(),
        );
    }

    cm_builder.add_data(
        SPARK_ENV_SH_FILE_NAME,
        to_spark_env_sh_string(config_overrides.spark_env_sh.overrides.iter()),
    );

    let mut jvm_sec_props = default_jvm_security_properties();
    jvm_sec_props.extend(config_overrides.security_properties.overrides.clone());

    cm_builder.add_data(
        JVM_SECURITY_PROPERTIES_FILE,
        to_java_properties_string(jvm_sec_props.iter())
            .with_context(|_| JvmSecurityPropertiesSnafu { role })?,
    );

    cm_builder.build().context(PodTemplateConfigMapSnafu)
}

pub(crate) fn submit_job_config_map(
    validated: &validate::ValidatedSparkApplication,
    config_overrides: &v1alpha1::ConfigOverrides,
) -> Result<ConfigMap> {
    let spark_application = &validated.spark_application;
    let cm_name = spark_application.submit_job_config_map_name();

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder.metadata(validated.object_meta(&cm_name, "spark-submit").build());

    cm_builder.add_data(
        SPARK_ENV_SH_FILE_NAME,
        to_spark_env_sh_string(config_overrides.spark_env_sh.overrides.iter()),
    );

    let mut jvm_sec_props = default_jvm_security_properties();
    jvm_sec_props.extend(config_overrides.security_properties.overrides.clone());

    cm_builder.add_data(
        JVM_SECURITY_PROPERTIES_FILE,
        to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
            JvmSecurityPropertiesSnafu {
                role: SparkApplicationRole::Submit,
            }
        })?,
    );

    cm_builder.build().context(PodTemplateConfigMapSnafu)
}
