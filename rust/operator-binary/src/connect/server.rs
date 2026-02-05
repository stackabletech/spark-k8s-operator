use std::collections::{BTreeMap, HashMap};

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            volume::{
                ListenerOperatorVolumeSourceBuilder, ListenerOperatorVolumeSourceBuilderError,
                ListenerReference, VolumeBuilder,
            },
        },
    },
    commons::product_image_selection::ResolvedProductImage,
    crd::listener,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EnvVar, HTTPGetAction, PodSecurityContext, Probe, Service,
                ServiceAccount,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{ResourceExt, runtime::reflector::ObjectRef},
    kvp::{Label, Labels},
    product_logging::framework::{LoggingError, calculate_log_volume_size_limit, vector_container},
    role_utils::RoleGroupRef,
};

use super::crd::CONNECT_APP_NAME;
use crate::{
    connect::{
        GRPC, HTTP,
        common::{self, SparkConnectRole, object_name},
        crd::{
            CONNECT_GRPC_PORT, CONNECT_UI_PORT, DEFAULT_SPARK_CONNECT_GROUP_NAME,
            SparkConnectContainer, v1alpha1,
        },
        s3,
    },
    crd::{
        constants::{
            JVM_SECURITY_PROPERTIES_FILE, LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME,
            LOG4J2_CONFIG_FILE, MAX_SPARK_LOG_FILES_SIZE, METRICS_PROPERTIES_FILE,
            POD_TEMPLATE_FILE, SPARK_DEFAULTS_FILE_NAME, VOLUME_MOUNT_NAME_CONFIG,
            VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_CONFIG,
            VOLUME_MOUNT_PATH_LOG, VOLUME_MOUNT_PATH_LOG_CONFIG,
        },
        listener_ext,
    },
    product_logging,
};

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to build spark connect listener"))]
    BuildListener {
        source: crate::crd::listener_ext::Error,
    },

    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

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

    #[snafu(display("server jvm security properties for spark connect {name}",))]
    ServerJvmSecurityProperties { source: common::Error, name: String },

    #[snafu(display("server metrics properties for spark connect {name}",))]
    MetricsProperties { source: common::Error, name: String },

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

    #[snafu(display("failed build connect server jvm args for {name}"))]
    ServerJvmArgs { source: common::Error, name: String },

    #[snafu(display("failed to add S3 secret or tls volume mounts to stateful set"))]
    AddS3VolumeMount { source: s3::Error },

    #[snafu(display("failed to add S3 secret volumes to stateful set"))]
    AddS3Volume { source: s3::Error },
}

// Assemble the configuration of the spark-connect server.
// This config map contains the following entries:
// - security.properties   : with jvm dns cache ttls
// - spark-defaults.conf   : with spark configuration properties
// - log4j2.properties     : with logging configuration (if configured)
// - template.yaml         : executor pod template
// - spark-env.sh          : OMITTED because the environment variables are added directly
//                           to the container environment.
#[allow(clippy::result_large_err)]
pub(crate) fn server_config_map(
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ServerConfig,
    resolved_product_image: &ResolvedProductImage,
    spark_properties: &str,
    executor_pod_template_spec: &str,
) -> Result<ConfigMap, Error> {
    let cm_name = object_name(&scs.name_any(), SparkConnectRole::Server);
    let jvm_sec_props = common::security_properties(
        scs.spec
            .server
            .config
            .as_ref()
            .and_then(|s| s.config_overrides.get(JVM_SECURITY_PROPERTIES_FILE)),
    )
    .context(ServerJvmSecurityPropertiesSnafu {
        name: scs.name_unchecked(),
    })?;

    let metrics_props = common::metrics_properties(
        scs.spec
            .server
            .config
            .as_ref()
            .and_then(|s| s.config_overrides.get(METRICS_PROPERTIES_FILE)),
    )
    .context(MetricsPropertiesSnafu {
        name: scs.name_unchecked(),
    })?;

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
                    &resolved_product_image.app_version_label_value,
                    &SparkConnectRole::Server.to_string(),
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(SPARK_DEFAULTS_FILE_NAME, spark_properties)
        .add_data(POD_TEMPLATE_FILE, executor_pod_template_spec)
        .add_data(JVM_SECURITY_PROPERTIES_FILE, jvm_sec_props)
        .add_data(METRICS_PROPERTIES_FILE, metrics_props);

    let role_group_ref = default_role_group_ref(scs);
    product_logging::extend_config_map(
        &role_group_ref,
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
        .context(InvalidConfigMapSnafu { name: cm_name })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_stateful_set(
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ServerConfig,
    resolved_product_image: &ResolvedProductImage,
    service_account: &ServiceAccount,
    config_map: &ConfigMap,
    listener_name: &str,
    args: Vec<String>,
    resolved_s3_buckets: &s3::ResolvedS3Buckets,
) -> Result<StatefulSet, Error> {
    let server_role = SparkConnectRole::Server.to_string();
    let recommended_object_labels = common::labels(
        scs,
        &resolved_product_image.app_version_label_value,
        &server_role,
    );

    let recommended_labels =
        Labels::recommended(recommended_object_labels.clone()).context(LabelBuildSnafu)?;

    let metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(recommended_object_labels)
        .context(MetadataBuildSnafu)?
        .with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?)
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
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG)
                .with_empty_dir(
                    None::<String>,
                    Some(calculate_log_volume_size_limit(&[MAX_SPARK_LOG_FILES_SIZE])),
                )
                .build(),
        )
        .context(AddVolumeSnafu)?
        .security_context(PodSecurityContext {
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        });

    let container_env = env(scs
        .spec
        .server
        .config
        .as_ref()
        .map(|s| s.env_overrides.clone())
        .as_ref())?;

    let mut container = ContainerBuilder::new(&SparkConnectContainer::Spark.to_string())
        .context(InvalidContainerNameSnafu)?;
    container
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
        .add_container_port(GRPC, CONNECT_GRPC_PORT)
        .add_container_port(HTTP, CONNECT_UI_PORT)
        .add_env_vars(container_env)
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_PATH_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mounts(
            resolved_s3_buckets
                .volumes_and_mounts()
                .context(AddS3VolumeMountSnafu)?
                .1,
        )
        .context(AddVolumeMountSnafu)?
        .readiness_probe(probe())
        .liveness_probe(probe());

    // Add custom log4j config map volumes if configured
    if let Some(cm_name) = config.log_config_map() {
        pb.add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG_CONFIG)
                .with_config_map(cm_name)
                .build(),
        )
        .context(AddVolumeSnafu)?;

        container
            .add_volume_mount(VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_LOG_CONFIG)
            .context(AddVolumeMountSnafu)?;
    }

    pb.add_container(container.build());

    if config.logging.enable_vector_agent {
        match scs.spec.vector_aggregator_config_map_name.to_owned() {
            Some(vector_aggregator_config_map_name) => {
                pb.add_container(
                    vector_container(
                        resolved_product_image,
                        VOLUME_MOUNT_NAME_CONFIG,
                        VOLUME_MOUNT_NAME_LOG,
                        config
                            .logging
                            .containers
                            .get(&SparkConnectContainer::Vector),
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("500m")
                            .with_memory_request("128Mi")
                            .with_memory_limit("128Mi")
                            .build(),
                        &vector_aggregator_config_map_name,
                    )
                    .context(ConfigureLoggingSnafu)?,
                );
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }

    // Add listener volume
    // Listener endpoints for the Webserver role will use persistent volumes
    // so that load balancers can hard-code the target addresses. This will
    // be the case even when no class is set (and the value defaults to
    // cluster-internal) as the address should still be consistent.
    let volume_claim_templates = Some(vec![
        ListenerOperatorVolumeSourceBuilder::new(
            &ListenerReference::ListenerName(listener_name.to_string()),
            &recommended_labels,
        )
        .build_pvc(LISTENER_VOLUME_NAME.to_string())
        .context(BuildListenerVolumeSnafu)?,
    ]);

    // Add any secret volumes needed for the configured S3 buckets
    pb.add_volumes(
        resolved_s3_buckets
            .volumes_and_mounts()
            .context(AddS3VolumeSnafu)?
            .0,
    )
    .context(AddVolumeSnafu)?;

    // Merge user defined pod template if available
    let mut pod_template = pb.build_template();
    if let Some(pod_overrides_spec) = scs
        .spec
        .server
        .config
        .as_ref()
        .map(|s| s.pod_overrides.clone())
    {
        pod_template.merge_from(pod_overrides_spec);
    }

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(scs)
            .name(object_name(&scs.name_any(), SparkConnectRole::Server))
            .ownerreference_from_resource(scs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(common::labels(
                scs,
                &resolved_product_image.app_version_label_value,
                &SparkConnectRole::Server.to_string(),
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(StatefulSetSpec {
            template: pod_template,
            replicas: Some(1),
            volume_claim_templates,
            selector: LabelSelector {
                match_labels: Some(
                    Labels::role_group_selector(
                        scs,
                        CONNECT_APP_NAME,
                        &SparkConnectRole::Server.to_string(),
                        DEFAULT_SPARK_CONNECT_GROUP_NAME,
                    )
                    .context(LabelBuildSnafu)?
                    .into(),
                ),
                ..LabelSelector::default()
            },
            ..StatefulSetSpec::default()
        }),
        ..StatefulSet::default()
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn command_args(user_args: &[String]) -> Vec<String> {
    let mut command = vec![formatdoc! { "
    containerdebug --output={VOLUME_MOUNT_PATH_LOG}/containerdebug-state.json --loop &

    cp {VOLUME_MOUNT_PATH_CONFIG}/{SPARK_DEFAULTS_FILE_NAME} /tmp/spark.properties
    config-utils template /tmp/spark.properties

    /stackable/spark/sbin/start-connect-server.sh \\
    --deploy-mode client \\
    --master k8s://https://${{KUBERNETES_SERVICE_HOST}}:${{KUBERNETES_SERVICE_PORT_HTTPS}} \\
    --properties-file /tmp/spark.properties
    " }];

    // User provided command line arguments
    command.extend_from_slice(user_args);

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

// Returns the contents of the spark properties file.
// It merges operator properties with user properties.
#[allow(clippy::result_large_err)]
pub(crate) fn server_properties(
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ServerConfig,
    driver_service: &Service,
    service_account: &ServiceAccount,
    resolved_product_image: &ResolvedProductImage,
) -> Result<BTreeMap<String, Option<String>>, Error> {
    let spark_image = resolved_product_image.image.clone();
    let spark_version = resolved_product_image.product_version.clone();
    let service_account_name = service_account.name_unchecked();
    let namespace = driver_service
        .namespace()
        .context(ObjectHasNoNamespaceSnafu)?;

    let config_overrides = scs
        .spec
        .server
        .config
        .as_ref()
        .and_then(|s| s.config_overrides.get(SPARK_DEFAULTS_FILE_NAME));

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
        ("spark.kubernetes.namespace".to_string(), Some(namespace)),
        (
            "spark.kubernetes.authenticate.driver.serviceAccountName".to_string(),
            Some(service_account_name),
        ),
        (
            "spark.kubernetes.driver.pod.name".to_string(),
            Some("${{env:HOSTNAME}}".to_string()),
        ),
        (
            "spark.driver.defaultJavaOptions".to_string(),
            Some(server_jvm_args(scs, config)?),
        ),
        (
            "spark.driver.extraClassPath".to_string(),
            Some(format!("/stackable/spark/extra-jars/*:/stackable/spark/connect/spark-connect-{spark_version}.jar")),
        ),
        (
            "spark.metrics.conf".to_string(),
            Some(format!(
                "{VOLUME_MOUNT_PATH_CONFIG}/{METRICS_PROPERTIES_FILE}"
            )),
        ),
        // This enables the "/metrics/executors/prometheus" endpoint on the server pod.
        // The driver collects metrics from the executors and makes them available here.
        // The "/metrics/prometheus" endpoint delivers the driver metrics.
        (
            "spark.ui.prometheus.enabled".to_string(),
            Some("true".to_string()),
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
    Ok(result)
}

fn server_jvm_args(
    scs: &v1alpha1::SparkConnectServer,
    config: &v1alpha1::ServerConfig,
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
            .server
            .config
            .as_ref()
            .map(|s| &s.product_specific_common_config),
    )
    .context(ServerJvmArgsSnafu {
        name: scs.name_any(),
    })
}

fn probe() -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            port: IntOrString::Int(CONNECT_UI_PORT),
            scheme: Some("HTTP".to_string()),
            path: Some("/metrics/prometheus".to_string()),
            ..Default::default()
        }),
        failure_threshold: Some(10),
        ..Probe::default()
    }
}

fn default_role_group_ref(
    scs: &v1alpha1::SparkConnectServer,
) -> RoleGroupRef<v1alpha1::SparkConnectServer> {
    RoleGroupRef {
        cluster: ObjectRef::from_obj(scs),
        role: SparkConnectRole::Server.to_string(),
        role_group: DEFAULT_SPARK_CONNECT_GROUP_NAME.to_string(),
    }
}

pub(crate) fn build_listener(
    scs: &v1alpha1::SparkConnectServer,
    role_config: &v1alpha1::SparkConnectServerRoleConfig,
    resolved_product_image: &ResolvedProductImage,
) -> Result<listener::v1alpha1::Listener, Error> {
    let listener_name = format!(
        "{cluster}-{role}",
        cluster = scs.name_any(),
        role = SparkConnectRole::Server
    );

    let listener_class = role_config.listener_class.clone();
    let role = SparkConnectRole::Server.to_string();
    let recommended_object_labels =
        common::labels(scs, &resolved_product_image.app_version_label_value, &role);

    let listener_ports = [
        listener::v1alpha1::ListenerPort {
            name: GRPC.to_string(),
            port: CONNECT_GRPC_PORT,
            protocol: Some("TCP".to_string()),
        },
        listener::v1alpha1::ListenerPort {
            name: HTTP.to_string(),
            port: CONNECT_UI_PORT,
            protocol: Some("TCP".to_string()),
        },
    ];

    listener_ext::build_listener(
        scs,
        &listener_name,
        &listener_class,
        recommended_object_labels,
        &listener_ports,
    )
    .context(BuildListenerSnafu)
}
