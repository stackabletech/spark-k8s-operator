use std::{collections::BTreeMap, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, security::PodSecurityContextBuilder, volume::VolumeBuilder},
    },
    commons::resources::{CpuLimits, MemoryLimits, Resources},
    k8s_openapi::{
        DeepMerge,
        api::core::v1::{ConfigMap, PodTemplateSpec},
    },
    kube::ResourceExt,
    product_logging::framework::{VECTOR_CONFIG_FILE, calculate_log_volume_size_limit},
    v2::{
        builder::pod::container::{EnvVarSet, new_container_builder},
        product_logging::framework::vector_container,
        role_group_utils::ResourceNames,
        role_utils::JavaCommonConfig,
        types::operator::{RoleGroupName, RoleName},
    },
};

use crate::{
    connect::{
        common::{self, SparkConnectRole, object_name},
        controller::{
            build::{object_meta, recommended_labels_for_role_resources},
            validate::ValidatedSparkConnectServer,
        },
        crd::{
            CONNECT_EXECUTOR_ROLE_NAME, DEFAULT_SPARK_CONNECT_GROUP_NAME, SparkConnectContainer,
            v1alpha1,
        },
        s3,
    },
    crd::constants::{
        CONTAINERDEBUG_LOG_DIRECTORY, JVM_SECURITY_PROPERTIES_FILE, LOG4J2_CONFIG_FILE,
        MAX_SPARK_LOG_FILES_SIZE, METRICS_PROPERTIES_FILE, POD_TEMPLATE_FILE,
        VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_NAME_LOG_CONFIG,
        VOLUME_MOUNT_PATH_CONFIG, VOLUME_MOUNT_PATH_LOG, VOLUME_MOUNT_PATH_LOG_CONFIG,
    },
    product_logging,
};

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to add volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add volume mount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed build connect executor security properties"))]
    ExecutorJvmSecurityProperties { source: common::Error },

    #[snafu(display("executor metrics properties for spark connect {name}",))]
    MetricsProperties { source: common::Error, name: String },

    #[snafu(display("failed to build connect executor config map [{cm_name}]"))]
    InvalidConfigMap {
        source: builder::configmap::Error,
        cm_name: String,
    },

    #[snafu(display("failed to build S3 volumes and mounts for executors"))]
    BuildS3VolumesAndMounts { source: s3::Error },

    #[snafu(display("failed to create the init container for the S3 truststore"))]
    TrustStoreInitContainer { source: s3::Error },
}

// The executor pod template can contain only a handful of properties.
// because spark overrides them.
//
// See https://spark.apache.org/docs/latest/running-on-kubernetes.html#pod-template-properties
// for a list of properties that are overridden/changed by Spark.
//
// Most notable properties that cannot be set here are:
// - container resources
//
#[allow(clippy::result_large_err)]
pub fn executor_pod_template(
    validated: &ValidatedSparkConnectServer,
    config_map: &ConfigMap,
) -> Result<PodTemplateSpec, Error> {
    let config = &validated.executor_config;
    let resolved_product_image = &validated.resolved_product_image;
    let resolved_s3 = &validated.cluster_config.resolved_s3;
    let container_env = executor_env(&validated.executor_overrides.env_overrides);

    let (s3_volumes, s3_volume_mounts) = resolved_s3
        .volumes_and_mounts()
        .context(BuildS3VolumesAndMountsSnafu)?;

    let mut container = new_container_builder(&SparkConnectContainer::Spark.to_container_name());
    container
        .add_env_vars(container_env)
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG.as_ref(), VOLUME_MOUNT_PATH_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG.as_ref(), VOLUME_MOUNT_PATH_LOG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mounts(s3_volume_mounts)
        .context(AddVolumeMountSnafu)?;

    let metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels_for_role_resources(
            validated,
            &SparkConnectRole::Executor,
        ))
        .build();

    let mut template = PodBuilder::new();
    template
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&config.affinity)
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG.as_ref())
                .with_empty_dir(
                    None::<String>,
                    Some(calculate_log_volume_size_limit(&[MAX_SPARK_LOG_FILES_SIZE])),
                )
                .build(),
        )
        .context(AddVolumeSnafu)?
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG.as_ref())
                .with_config_map(config_map.name_unchecked())
                .build(),
        )
        .context(AddVolumeSnafu)?
        .add_volumes(s3_volumes)
        .context(AddVolumeSnafu)?
        // This is needed for shared enpryDir volumes with other containers like the truststore
        // init container.
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        );

    // S3: Add truststore init container for S3 endpoint communication with TLS.
    if let Some(truststore_init_container) = resolved_s3
        .truststore_init_container(resolved_product_image.clone())
        .context(TrustStoreInitContainerSnafu)?
    {
        template.add_init_container(truststore_init_container);
    }

    if let Some(cm_name) = config.log_config_map() {
        container
            .add_volume_mount(
                VOLUME_MOUNT_NAME_LOG_CONFIG.as_ref(),
                VOLUME_MOUNT_PATH_LOG_CONFIG,
            )
            .context(AddVolumeMountSnafu)?;

        template
            .add_volume(
                VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG_CONFIG.as_ref())
                    .with_config_map(cm_name)
                    .build(),
            )
            .context(AddVolumeSnafu)?;
    }

    template.add_container(container.build());

    // Vector log-aggregation sidecar (symmetric with the server), added when the executor enables
    // the Vector agent.
    if let Some(vector_log_config) = &validated.executor_logging.vector_container {
        // The Vector sidecar's `CLUSTER_NAME`/`ROLE_NAME`/`ROLE_GROUP_NAME` log-metadata env vars.
        // These do NOT affect resource naming: Spark Connect keeps its `{cluster}-{role}` names.
        let vector_resource_names = ResourceNames {
            cluster_name: validated.name.clone(),
            role_name: RoleName::from_str(CONNECT_EXECUTOR_ROLE_NAME)
                .expect("CONNECT_EXECUTOR_ROLE_NAME is a valid role name"),
            role_group_name: RoleGroupName::from_str(DEFAULT_SPARK_CONNECT_GROUP_NAME)
                .expect("DEFAULT_SPARK_CONNECT_GROUP_NAME is a valid role group name"),
        };

        template.add_container(vector_container(
            &SparkConnectContainer::Vector.to_container_name(),
            resolved_product_image,
            vector_log_config,
            &vector_resource_names,
            &VOLUME_MOUNT_NAME_CONFIG,
            &VOLUME_MOUNT_NAME_LOG,
            EnvVarSet::new(),
        ));
    }

    let mut result = template.build_template();

    // Merge user provided pod spec if any
    result.merge_from(validated.executor_overrides.pod_overrides.clone());

    Ok(result)
}

/// The environment variables of the executor container.
///
/// The user's `envOverrides` are merged in last so that they override any operator-set
/// environment variable.
fn executor_env(env_overrides: &EnvVarSet) -> EnvVarSet {
    EnvVarSet::new()
        .with_value(
            &CONTAINERDEBUG_LOG_DIRECTORY,
            format!("{VOLUME_MOUNT_PATH_LOG}/containerdebug"),
        )
        .merge(env_overrides.clone())
}

pub(crate) fn executor_properties(
    validated: &ValidatedSparkConnectServer,
) -> Result<BTreeMap<String, Option<String>>, Error> {
    let config = &validated.executor_config;
    let resolved_product_image = &validated.resolved_product_image;
    let spark_image = resolved_product_image.image.clone();
    let spark_version = resolved_product_image.product_version.clone();

    let mut result: BTreeMap<String, Option<String>> = [
        (
            "spark.kubernetes.executor.container.image".to_string(),
            Some(spark_image),
        ),
        // Must mirror `spark.driver.extraClassPath` on the server.
        //
        // Spark Connect jar is not in `/stackable/spark/jars`.
        // Without it, the executors cannot deserialize the closures that Connect
        // ships with every task that returns rows to a client, and any `count`, `collect` or
        // `toPandas` fails with:
        //
        //   java.lang.ClassCastException: cannot assign instance of
        //   java.lang.invoke.SerializedLambda to field
        //   org.apache.spark.rdd.MapPartitionsRDD.f of type scala.Function3
        (
            "spark.executor.extraClassPath".to_string(),
            Some(format!(
                "/stackable/spark/extra-jars/*:/stackable/spark/connect/spark-connect-{spark_version}.jar"
            )),
        ),
        (
            "spark.executor.defaultJavaOptions".to_string(),
            Some(executor_jvm_args(
                validated.executor_overrides.jvm_config.as_ref(),
                config,
            )),
        ),
        (
            "spark.kubernetes.executor.podTemplateFile".to_string(),
            Some(format!("{VOLUME_MOUNT_PATH_CONFIG}/{POD_TEMPLATE_FILE}")),
        ),
        (
            "spark.kubernetes.executor.podTemplateContainerName".to_string(),
            Some(SparkConnectContainer::Spark.to_string()),
        ),
    ]
    .into();

    // ========================================
    // Add executor resource properties
    let Resources {
        cpu: CpuLimits { min, max },
        memory: MemoryLimits {
            limit,
            runtime_limits: _,
        },
        storage: _,
    } = &config.resources;
    result.insert(
        "spark.kubernetes.executor.limit.cores".to_string(),
        max.clone().map(|v| v.0),
    );
    result.insert(
        "spark.kubernetes.executor.request.cores".to_string(),
        min.clone().map(|v| v.0),
    );
    result.insert(
        "spark.executor.memory".to_string(),
        limit.clone().map(|v| v.0),
    );
    // This ensures that the pod's memory limit is exactly the value
    // in `config.resources.memory.limit`.
    // By default, Spark computes an `executor.memoryOverhead` as 6-10% from the
    // `executor.memory`.
    result.insert(
        "spark.executor.memoryOverhead".to_string(),
        Some("0".to_string()),
    );

    // ========================================
    // Add the user provided executor properties

    let config_overrides = validated
        .executor_overrides
        .config_overrides
        .spark_defaults_conf
        .overrides
        .clone();

    result.extend(config_overrides.into_iter().map(|(k, v)| (k, Some(v))));

    Ok(result)
}

fn executor_jvm_args(
    jvm_config: Option<&JavaCommonConfig>,
    config: &v1alpha1::ExecutorConfig,
) -> String {
    let mut jvm_args = vec![format!(
        "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];

    if config.log_config_map().is_some() {
        jvm_args.push(format!(
            "-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"
        ));
    }

    common::jvm_args(&jvm_args, jvm_config)
}

// Assemble the configuration of the spark-connect executor.
// This config map contains the following entries:
// - security.properties   : with jvm dns cache ttls
// - log4j2.properties     : with logging configuration (if configured)
//
pub(crate) fn executor_config_map(
    validated: &ValidatedSparkConnectServer,
) -> Result<ConfigMap, Error> {
    let config = &validated.executor_config;
    let config_overrides = Some(&validated.executor_overrides.config_overrides);
    let cm_name = object_name(&validated.name_any(), SparkConnectRole::Executor);

    let security_properties_overrides = config_overrides
        .map(|config_overrides| config_overrides.security_properties.overrides.clone())
        .unwrap_or_default();

    let jvm_sec_props = common::security_properties(security_properties_overrides)
        .context(ExecutorJvmSecurityPropertiesSnafu)?;

    let metrics_properties_overrides = config_overrides
        .map(|config_overrides| config_overrides.metrics_properties.overrides.clone())
        .unwrap_or_default();

    let metrics_props = common::metrics_properties(metrics_properties_overrides).context(
        MetricsPropertiesSnafu {
            name: validated.name_any(),
        },
    )?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(object_meta(validated, &cm_name, SparkConnectRole::Executor).build())
        .add_data(JVM_SECURITY_PROPERTIES_FILE, jvm_sec_props)
        .add_data(METRICS_PROPERTIES_FILE, metrics_props);

    if let Some(log4j2) =
        product_logging::build_log4j2(&config.logging, SparkConnectContainer::Spark)
    {
        cm_builder.add_data(LOG4J2_CONFIG_FILE, log4j2);
    }
    if config.logging.enable_vector_agent {
        cm_builder.add_data(
            VECTOR_CONFIG_FILE,
            product_logging::vector_config_file_content(),
        );
    }

    cm_builder
        .build()
        .context(InvalidConfigMapSnafu { cm_name })
}

#[cfg(test)]
mod tests {
    use stackable_operator::k8s_openapi::{
        api::core::v1::EnvVar, apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };

    use super::*;
    use crate::connect::controller::build::test_support::minimal_validated_cluster;

    /// `envOverrides` must be applied after all operator-set environment variables, so a user
    /// override replaces the operator-set value instead of duplicating it or being ignored.
    #[test]
    fn env_overrides_override_operator_set_env_vars() {
        let mut validated = minimal_validated_cluster();
        validated.executor_overrides.env_overrides = EnvVarSet::new().with_value(
            &"CONTAINERDEBUG_LOG_DIRECTORY"
                .parse()
                .expect("valid env var name"),
            "/custom/log/dir",
        );

        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some("my-connect-executor".to_string()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };

        let pod_template = executor_pod_template(&validated, &config_map)
            .expect("the executor pod template can be built");

        let env: Vec<EnvVar> = pod_template
            .spec
            .expect("the pod template has a spec")
            .containers
            .iter()
            .find(|container| container.name == "spark")
            .expect("the spark container exists")
            .env
            .clone()
            .expect("the spark container has env vars");

        let matching: Vec<&EnvVar> = env
            .iter()
            .filter(|env_var| env_var.name == "CONTAINERDEBUG_LOG_DIRECTORY")
            .collect();

        // The override must replace the operator-set value, not duplicate it.
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].value.as_deref(), Some("/custom/log/dir"));
    }
}
