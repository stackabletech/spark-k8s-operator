use std::collections::{BTreeMap, HashMap};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{container::ContainerBuilder, volume::VolumeBuilder, PodBuilder},
    },
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::core::v1::{ConfigMap, EnvVar, PodTemplateSpec},
        DeepMerge,
    },
    kube::{runtime::reflector::ObjectRef, ResourceExt},
    product_logging::framework::calculate_log_volume_size_limit,
    role_utils::RoleGroupRef,
};

use super::{
    common::{object_name, SparkConnectRole},
    crd::{SparkConnectContainer, DUMMY_SPARK_CONNECT_GROUP_NAME},
};
use crate::{
    connect::{common, crd::v1alpha1},
    crd::constants::{
        JVM_SECURITY_PROPERTIES_FILE, LOG4J2_CONFIG_FILE, MAX_SPARK_LOG_FILES_SIZE,
        POD_TEMPLATE_FILE, SPARK_DEFAULTS_FILE_NAME, VOLUME_MOUNT_NAME_CONFIG,
        VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_CONFIG,
        VOLUME_MOUNT_PATH_LOG, VOLUME_MOUNT_PATH_LOG_CONFIG,
    },
    product_logging,
};

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

    #[snafu(display("invalid connect container name"))]
    InvalidContainerName {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed to add volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add volume mount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed build connect executor jvm args for {name}"))]
    ExecutorJvmArgs { source: common::Error, name: String },

    #[snafu(display("failed build connect executor security properties"))]
    ExecutorJvmSecurityProperties { source: common::Error },

    #[snafu(display("failed build connect executor config map metadata"))]
    ConfigMapMetadataBuild { source: builder::meta::Error },

    #[snafu(display(
        "failed to add the logging configuration to connect executor config map [{cm_name}]"
    ))]
    InvalidLoggingConfig {
        source: product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to build connect executor config map [{cm_name}]"))]
    InvalidConfigMap {
        source: builder::configmap::Error,
        cm_name: String,
    },
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
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ExecutorConfig,
    config_map: &ConfigMap,
) -> Result<PodTemplateSpec, Error> {
    let container_env = executor_env(
        scs.spec
            .executor
            .as_ref()
            .map(|s| s.env_overrides.clone())
            .as_ref(),
    )?;

    let mut container = ContainerBuilder::new(&SparkConnectContainer::Spark.to_string())
        .context(InvalidContainerNameSnafu)?;
    container
        .add_env_vars(container_env)
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_PATH_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
        .context(AddVolumeMountSnafu)?;

    let mut template = PodBuilder::new();
    template
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG)
                .with_empty_dir(
                    None::<String>,
                    Some(calculate_log_volume_size_limit(&[MAX_SPARK_LOG_FILES_SIZE])),
                )
                .build(),
        )
        .context(AddVolumeSnafu)?
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
                .with_config_map(config_map.name_unchecked())
                .build(),
        )
        .context(AddVolumeSnafu)?;

    if let Some(cm_name) = config.log_config_map() {
        container
            .add_volume_mount(VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_LOG_CONFIG)
            .context(AddVolumeMountSnafu)?;

        template
            .add_volume(
                VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG_CONFIG)
                    .with_config_map(cm_name)
                    .build(),
            )
            .context(AddVolumeSnafu)?;
    }

    let mut result = template.add_container(container.build()).build_template();

    // Merge user provided pod spec if any
    if let Some(pod_overrides_spec) = scs.spec.executor.as_ref().map(|s| s.pod_overrides.clone()) {
        result.merge_from(pod_overrides_spec);
    }

    Ok(result)
}

fn executor_env(env_overrides: Option<&HashMap<String, String>>) -> Result<Vec<EnvVar>, Error> {
    let mut envs = BTreeMap::from([
        // Needed by the `containerdebug` running in the background of the connect container
        // to log its tracing information to.
        (
            "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
            format!("{VOLUME_MOUNT_PATH_LOG}/containerdebug"),
        ),
    ]);

    // Add env overrides
    if let Some(user_env) = env_overrides {
        envs.extend(user_env.clone());
    }

    Ok(envs
        .into_iter()
        .map(|(name, value)| EnvVar {
            name: name.to_owned(),
            value: Some(value.to_owned()),
            value_from: None,
        })
        .collect())
}

pub fn executor_properties(
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ExecutorConfig,
    pi: &ResolvedProductImage,
) -> Result<BTreeMap<String, Option<String>>, Error> {
    let spark_image = pi.image.clone();

    let mut result: BTreeMap<String, Option<String>> = [
        (
            "spark.kubernetes.executor.container.image".to_string(),
            Some(spark_image),
        ),
        (
            "spark.executor.defaultJavaOptions".to_string(),
            Some(executor_jvm_args(scs, config)?),
        ),
        (
            "spark.executor.extraClassPath".to_string(),
            Some("/stackable/spark/extra-jars/*".to_string()),
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

    let config_overrides = scs
        .spec
        .executor
        .as_ref()
        .and_then(|s| s.config_overrides.get(SPARK_DEFAULTS_FILE_NAME));

    if let Some(user_config) = config_overrides {
        result.extend(
            user_config
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone()))),
        );
    }

    Ok(result)
}

fn executor_jvm_args(
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ExecutorConfig,
) -> Result<String, Error> {
    let mut jvm_args = vec![format!(
        "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];

    if config.log_config_map().is_some() {
        jvm_args.push(format!(
            "-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"
        ));
    }

    common::jvm_args(
        &jvm_args,
        scs.spec
            .executor
            .as_ref()
            .map(|s| &s.product_specific_common_config),
    )
    .context(ExecutorJvmArgsSnafu {
        name: scs.name_any(),
    })
}

// Assemble the configuration of the spark-connect executor.
// This config map contains the following entries:
// - security.properties   : with jvm dns cache ttls
// - log4j2.properties     : with logging configuration (if configured)
//
pub fn executor_config_map(
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ExecutorConfig,
    resolved_product_image: &ResolvedProductImage,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap, Error> {
    let cm_name = object_name(&scs.name_any(), SparkConnectRole::Executor);
    let jvm_sec_props = common::security_properties(
        scs.spec
            .executor
            .as_ref()
            .and_then(|s| s.config_overrides.get(JVM_SECURITY_PROPERTIES_FILE)),
    )
    .context(ExecutorJvmSecurityPropertiesSnafu)?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(scs)
                .name(&cm_name)
                .ownerreference_from_resource(scs, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(common::labels(
                    scs,
                    &resolved_product_image.app_version_label,
                    &SparkConnectRole::Executor.to_string(),
                ))
                .context(ConfigMapMetadataBuildSnafu)?
                .build(),
        )
        .add_data(JVM_SECURITY_PROPERTIES_FILE, jvm_sec_props);

    let role_group_ref = RoleGroupRef {
        cluster: ObjectRef::from_obj(scs),
        role: SparkConnectRole::Executor.to_string(),
        role_group: DUMMY_SPARK_CONNECT_GROUP_NAME.to_string(),
    };
    product_logging::extend_config_map(
        &role_group_ref,
        vector_aggregator_address,
        &config.logging,
        SparkConnectContainer::Spark,
        SparkConnectContainer::Vector,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: cm_name.clone(),
    })?;

    cm_builder
        .build()
        .context(InvalidConfigMapSnafu { cm_name })
}
