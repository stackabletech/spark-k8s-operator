use std::collections::{BTreeMap, HashMap};

use product_config::writer::to_java_properties_string;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder,
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            container::ContainerBuilder, resources::ResourceRequirementsBuilder,
            volume::VolumeBuilder, PodBuilder,
        },
    },
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::{
            apps::v1::{Deployment, DeploymentSpec},
            core::v1::{
                ConfigMap, EnvVar, PodSecurityContext, Service, ServiceAccount, ServicePort,
                ServiceSpec,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
        DeepMerge,
    },
    kube::{runtime::reflector::ObjectRef, ResourceExt},
    kvp::{Label, Labels},
    product_logging::{
        framework::{calculate_log_volume_size_limit, vector_container, LoggingError},
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
};

use crate::{
    connect::{
        common::{labels, object_name, SparkConnectRole},
        crd::{
            v1alpha1, SparkConnectServerContainer, CONNECT_CONTAINER_NAME, CONNECT_GRPC_PORT,
            CONNECT_UI_PORT,
        },
    },
    crd::constants::{
        APP_NAME, JVM_SECURITY_PROPERTIES_FILE, MAX_SPARK_LOG_FILES_SIZE, METRICS_PORT,
        POD_TEMPLATE_FILE, SPARK_DEFAULTS_FILE_NAME, SPARK_UID, VOLUME_MOUNT_NAME_CONFIG,
        VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_CONFIG,
        VOLUME_MOUNT_PATH_LOG, VOLUME_MOUNT_PATH_LOG_CONFIG,
    },
    product_logging,
};

const DUMMY_SPARK_CONNECT_GROUP_NAME: &str = "default";

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides {
        source: stackable_operator::role_utils::Error,
    },

    #[snafu(display("spark connect object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("invalid config map {name}"))]
    InvalidConfigMap {
        source: builder::configmap::Error,
        name: String,
    },

    #[snafu(display("invalid connect container name"))]
    InvalidContainerName {
        source: builder::pod::container::Error,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef { source: builder::meta::Error },

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display(
        "failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for the connect server",
    ))]
    JvmSecurityProperties {
        source: product_config::writer::PropertiesWriterError,
    },

    #[snafu(display("failed to serialize [{SPARK_DEFAULTS_FILE_NAME}] for the connect server",))]
    SparkDefaultsProperties {
        source: product_config::writer::PropertiesWriterError,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild { source: builder::meta::Error },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

// Assemble the configuration of the spark-connect server.
// This config map contains the following entries:
// - security.properties   : with jvm dns cache ttls
// - spark-defaults.conf   : with spark configuration properties
// - log4j2.properties     : with logging configuration
// - template.yaml         : executor pod template
// - spark-env.sh          : OMITTED because the environment variables are added directly
//                           to the container environment.
#[allow(clippy::result_large_err)]
pub fn build_config_map(
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ServerConfig,
    resolved_product_image: &ResolvedProductImage,
    vector_aggregator_address: Option<&str>,
    spark_properties: &str,
    executor_pod_template_spec: &str,
) -> Result<ConfigMap, Error> {
    let cm_name = object_name(&scs.name_any(), SparkConnectRole::Server);

    let jvm_sec_props = jvm_security_properties(
        scs.spec
            .server
            .as_ref()
            .and_then(|s| s.config_overrides.get(JVM_SECURITY_PROPERTIES_FILE)),
    )?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(scs)
                .name(&cm_name)
                .ownerreference_from_resource(scs, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(labels(
                    scs,
                    &resolved_product_image.app_version_label,
                    &SparkConnectRole::Server.to_string(),
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(SPARK_DEFAULTS_FILE_NAME, spark_properties)
        .add_data(POD_TEMPLATE_FILE, executor_pod_template_spec)
        .add_data(JVM_SECURITY_PROPERTIES_FILE, jvm_sec_props);

    let role_group_ref = RoleGroupRef {
        cluster: ObjectRef::from_obj(scs),
        role: SparkConnectRole::Server.to_string(),
        role_group: DUMMY_SPARK_CONNECT_GROUP_NAME.to_string(),
    };
    product_logging::extend_config_map(
        &role_group_ref,
        vector_aggregator_address,
        &config.logging,
        SparkConnectServerContainer::SparkConnect,
        SparkConnectServerContainer::Vector,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu { cm_name: &cm_name })?;

    cm_builder
        .build()
        .context(InvalidConfigMapSnafu { name: cm_name })
}

#[allow(clippy::result_large_err)]
pub fn build_deployment(
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ServerConfig,
    resolved_product_image: &ResolvedProductImage,
    service_account: &ServiceAccount,
    config_map: &ConfigMap,
    args: Vec<String>,
) -> Result<Deployment, Error> {
    let log_config_map = log_config_map_name(config, config_map);

    let metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(labels(
            scs,
            &resolved_product_image.app_version_label,
            &SparkConnectRole::Server.to_string(),
        ))
        .context(MetadataBuildSnafu)?
        .build();

    let mut pb = PodBuilder::new();

    pb.service_account_name(service_account.name_unchecked())
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
                .with_config_map(config_map.name_any())
                .build(),
        )
        .context(AddVolumeSnafu)?
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG_CONFIG)
                .with_config_map(log_config_map)
                .build(),
        )
        .context(AddVolumeSnafu)?
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG)
                .with_empty_dir(
                    None::<String>,
                    Some(calculate_log_volume_size_limit(&[MAX_SPARK_LOG_FILES_SIZE])),
                )
                .build(),
        )
        .context(AddVolumeSnafu)?
        .security_context(PodSecurityContext {
            run_as_user: Some(SPARK_UID),
            run_as_group: Some(0),
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        });

    let container_env = env(scs
        .spec
        .server
        .as_ref()
        .map(|s| s.env_overrides.clone())
        .as_ref())?;

    let container = ContainerBuilder::new(CONNECT_CONTAINER_NAME)
        .context(InvalidContainerNameSnafu)?
        .image_from_product_image(resolved_product_image)
        .resources(config.resources.clone().into())
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(args)
        .add_container_port("grpc", CONNECT_GRPC_PORT)
        .add_container_port("http", CONNECT_UI_PORT)
        .add_container_port("metrics", METRICS_PORT.into())
        .add_env_vars(container_env)
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_PATH_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_LOG_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
        .context(AddVolumeMountSnafu)?
        .build();
    pb.add_container(container);

    if config.logging.enable_vector_agent {
        pb.add_container(
            vector_container(
                resolved_product_image,
                VOLUME_MOUNT_NAME_CONFIG,
                VOLUME_MOUNT_NAME_LOG,
                config
                    .logging
                    .containers
                    .get(&SparkConnectServerContainer::Vector),
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("250m")
                    .with_cpu_limit("500m")
                    .with_memory_request("128Mi")
                    .with_memory_limit("128Mi")
                    .build(),
            )
            .context(ConfigureLoggingSnafu)?,
        );
    }

    // Merge user defined pod template if available
    let mut pod_template = pb.build_template();
    if let Some(pod_overrides_spec) = scs.spec.server.as_ref().map(|s| s.pod_overrides.clone()) {
        pod_template.merge_from(pod_overrides_spec);
    }

    Ok(Deployment {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(scs)
            .name(object_name(&scs.name_any(), SparkConnectRole::Server))
            .ownerreference_from_resource(scs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(
                scs,
                &resolved_product_image.app_version_label,
                &SparkConnectRole::Server.to_string(),
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(DeploymentSpec {
            template: pod_template,
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(
                    Labels::role_group_selector(
                        scs,
                        APP_NAME,
                        &SparkConnectRole::Server.to_string(),
                        DUMMY_SPARK_CONNECT_GROUP_NAME,
                    )
                    .context(LabelBuildSnafu)?
                    .into(),
                ),
                ..LabelSelector::default()
            },
            ..DeploymentSpec::default()
        }),
        ..Deployment::default()
    })
}

#[allow(clippy::result_large_err)]
pub fn build_service(
    scs: &v1alpha1::SparkConnectServer,
    app_version_label: &str,
    service_cluster_ip: Option<String>,
) -> Result<Service, Error> {
    let (service_name, service_type) = match service_cluster_ip.clone() {
        Some(_) => (
            object_name(&scs.name_any(), SparkConnectRole::Server),
            "ClusterIP".to_string(),
        ),
        None => (
            format!(
                "{}-{}",
                object_name(&scs.name_any(), SparkConnectRole::Server),
                SparkConnectRole::Server
            ),
            scs.spec.cluster_config.listener_class.k8s_service_type(),
        ),
    };

    let selector = Labels::role_selector(scs, APP_NAME, &SparkConnectRole::Server.to_string())
        .context(LabelBuildSnafu)?
        .into();

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(scs)
            .name(service_name)
            .ownerreference_from_resource(scs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(
                scs,
                app_version_label,
                &SparkConnectRole::Server.to_string(),
            ))
            .context(MetadataBuildSnafu)?
            .with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?)
            .build(),
        spec: Some(ServiceSpec {
            type_: Some(service_type),
            cluster_ip: service_cluster_ip,
            ports: Some(vec![
                ServicePort {
                    name: Some(String::from("grpc")),
                    port: CONNECT_GRPC_PORT,
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some(String::from("http")),
                    port: CONNECT_UI_PORT,
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some(String::from("metrics")),
                    port: METRICS_PORT.into(),
                    ..ServicePort::default()
                },
            ]),
            selector: Some(selector),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

#[allow(clippy::result_large_err)]
pub fn command_args(spark_version: &str) -> Vec<String> {
    let command = [
        // ---------- start containerdebug
        format!(
            "containerdebug --output={VOLUME_MOUNT_PATH_LOG}/containerdebug-state.json --loop &"
        ),
        // ---------- start spark connect server
        "/stackable/spark/sbin/start-connect-server.sh".to_string(),
        "--deploy-mode client".to_string(), // 'cluster' mode not supported
        "--master k8s://https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT_HTTPS}"
            .to_string(),
        format!("--jars /stackable/spark/connect/spark-connect_2.12-{spark_version}.jar"),
        format!("--properties-file {VOLUME_MOUNT_PATH_CONFIG}/{SPARK_DEFAULTS_FILE_NAME}"),
    ];

    vec![command.join(" ")]
}

#[allow(clippy::result_large_err)]
fn env(env_overrides: Option<&HashMap<String, String>>) -> Result<Vec<EnvVar>, Error> {
    let mut envs = BTreeMap::from([
        // Needed by the `containerdebug` running in the background of the connect container
        // to log its tracing information to.
        (
            "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
            format!("{VOLUME_MOUNT_PATH_LOG}/containerdebug"),
        ),
        // This env var prevents the connect server from detaching itself from the
        // start script because this leads to the Pod terminating immediately.
        ("SPARK_NO_DAEMONIZE".to_string(), "true".to_string()),
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

#[allow(clippy::result_large_err)]
fn jvm_security_properties(
    config_overrides: Option<&HashMap<String, String>>,
) -> Result<String, Error> {
    let mut result: BTreeMap<String, Option<String>> = [
        (
            "networkaddress.cache.ttl".to_string(),
            Some("30".to_string()),
        ),
        (
            "networkaddress.cache.negative.ttl".to_string(),
            Some("0".to_string()),
        ),
    ]
    .into();

    if let Some(user_config) = config_overrides {
        result.extend(
            user_config
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone()))),
        );
    }

    to_java_properties_string(result.iter()).context(JvmSecurityPropertiesSnafu)
}

// Returns the contents of the spark properties file.
// It merges operator properties with user properties.
#[allow(clippy::result_large_err)]
pub fn spark_properties(
    driver_service: &Service,
    service_account: &ServiceAccount,
    pi: &ResolvedProductImage,
    server_jvm_args: &str,
    executor_jvm_args: &str,
    config_overrides: Option<&HashMap<String, String>>,
) -> Result<String, Error> {
    let spark_image = pi.image.clone();
    let service_account_name = service_account.name_unchecked();
    let namespace = driver_service
        .namespace()
        .context(ObjectHasNoNamespaceSnafu)?;

    let mut result: BTreeMap<String, Option<String>> = [
        // This needs to match the name of the headless service for the executors to be able
        // to connect back to the driver.
        (
            "spark.driver.host".to_string(),
            Some(driver_service.name_any()),
        ),
        (
            "spark.kubernetes.driver.container.image".to_string(),
            Some(spark_image.clone()),
        ),
        (
            "spark.kubernetes.executor.container.image".to_string(),
            Some(spark_image),
        ),
        ("spark.kubernetes.namespace".to_string(), Some(namespace)),
        (
            "spark.kubernetes.authenticate.driver.serviceAccountName".to_string(),
            Some(service_account_name),
        ),
        (
            "spark.kubernetes.driver.pod.name".to_string(),
            Some("${env:HOSTNAME}".to_string()),
        ),
        (
            "spark.driver.defaultJavaOptions".to_string(),
            Some(server_jvm_args.to_string()),
        ),
        (
            "spark.driver.extraClassPath".to_string(),
            Some("/stackable/spark/extra-jars/*".to_string()),
        ),
        (
            "spark.executor.defaultJavaOptions".to_string(),
            Some(executor_jvm_args.to_string()),
        ),
        (
            "spark.executor.extraClassPath".to_string(),
            Some("/stackable/spark/extra-jars/*".to_string()),
        ),
        (
            "spark.kubernetes.executor.podTemplateFile".to_string(),
            Some(format!("{VOLUME_MOUNT_PATH_CONFIG}/{POD_TEMPLATE_FILE}")),
        ),
    ]
    .into();

    if let Some(user_config) = config_overrides {
        result.extend(
            user_config
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone()))),
        );
    }

    to_java_properties_string(result.iter()).context(SparkDefaultsPropertiesSnafu)
}

// Returns the name of the logging config map, which is either a custom one
// or the default server CM.
fn log_config_map_name(connect_config: &v1alpha1::ServerConfig, default_cm: &ConfigMap) -> String {
    let cc = connect_config
        .logging
        .containers
        .get(&SparkConnectServerContainer::SparkConnect)
        .cloned();

    match cc {
        Some(ContainerLogConfig {
            choice:
                Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                    custom: ConfigMapLogConfig { config_map },
                })),
        }) => config_map,
        _ => default_cm.name_any(),
    }
}
