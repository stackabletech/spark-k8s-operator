use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            security::PodSecurityContextBuilder,
            volume::{
                ListenerOperatorVolumeSourceBuilder, ListenerOperatorVolumeSourceBuilderError,
                ListenerReference, VolumeBuilder,
            },
        },
    },
    crd::listener,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMap, EnvVar, HTTPGetAction, Probe, Service},
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::ResourceExt,
    product_logging::framework::{VECTOR_CONFIG_FILE, calculate_log_volume_size_limit},
    v2::{
        builder::{
            pod::container::{EnvVarSet, new_container_builder},
            service::{Scraping, prometheus_labels},
        },
        product_logging::framework::vector_container,
        role_group_utils::ResourceNames,
        role_utils::JavaCommonConfig,
        types::operator::{RoleGroupName, RoleName},
    },
};

use crate::{
    connect::{
        GRPC, HTTP,
        common::{self, SparkConnectRole, object_name},
        controller::{build::object_meta, validate::ValidatedSparkConnectServer},
        crd::{
            CONNECT_GRPC_PORT, CONNECT_SERVER_ROLE_NAME, CONNECT_UI_PORT,
            DEFAULT_SPARK_CONNECT_GROUP_NAME, SparkConnectContainer, v1alpha1,
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
    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("spark connect object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("invalid config map {name}"))]
    InvalidConfigMap {
        source: builder::configmap::Error,
        name: String,
    },

    #[snafu(display("server jvm security properties for spark connect {name}",))]
    ServerJvmSecurityProperties { source: common::Error, name: String },

    #[snafu(display("server metrics properties for spark connect {name}",))]
    MetricsProperties { source: common::Error, name: String },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed to build S3 volumes and mounts for the server"))]
    BuildS3VolumesAndMounts { source: s3::Error },

    #[snafu(display("failed to create the init container for the S3 truststore"))]
    TrustStoreInitContainer { source: s3::Error },
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
    validated: &ValidatedSparkConnectServer,
    spark_properties: &str,
    executor_pod_template_spec: &str,
) -> Result<ConfigMap, Error> {
    let config = &validated.server_config;
    let config_overrides = Some(&validated.server_overrides.config_overrides);
    let cm_name = object_name(&validated.name_any(), SparkConnectRole::Server);

    let security_properties_overrides = config_overrides
        .map(|config_overrides| config_overrides.security_properties.overrides.clone())
        .unwrap_or_default();

    let jvm_sec_props = common::security_properties(security_properties_overrides).context(
        ServerJvmSecurityPropertiesSnafu {
            name: validated.name_any(),
        },
    )?;

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
        .metadata(object_meta(validated, &cm_name, SparkConnectRole::Server).build())
        .add_data(SPARK_DEFAULTS_FILE_NAME, spark_properties)
        .add_data(POD_TEMPLATE_FILE, executor_pod_template_spec)
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
        .context(InvalidConfigMapSnafu { name: cm_name })
}

pub(crate) fn build_stateful_set(
    validated: &ValidatedSparkConnectServer,
    config_map: &ConfigMap,
    listener_name: &str,
    args: Vec<String>,
) -> Result<StatefulSet, Error> {
    let config = &validated.server_config;
    let resolved_product_image = &validated.resolved_product_image;
    let resolved_s3 = &validated.cluster_config.resolved_s3;

    let recommended_labels = validated.recommended_labels(SparkConnectRole::Server);

    let metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .with_labels(prometheus_labels(&Scraping::Enabled))
        .build();

    let mut pb = PodBuilder::new();

    pb.service_account_name(validated.cluster_resource_names().service_account_name())
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG.as_ref())
                .with_config_map(config_map.name_any())
                .build(),
        )
        .context(AddVolumeSnafu)?
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG.as_ref())
                .with_empty_dir(
                    None::<String>,
                    Some(calculate_log_volume_size_limit(&[MAX_SPARK_LOG_FILES_SIZE])),
                )
                .build(),
        )
        .context(AddVolumeSnafu)?
        // This is needed for shared enpryDir volumes with other containers like the truststore
        // init container.
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        );

    let container_env = env(Some(&validated.server_overrides.env_overrides))?;

    let (s3_volumes, s3_volume_mounts) = resolved_s3
        .volumes_and_mounts()
        .context(BuildS3VolumesAndMountsSnafu)?;

    let mut container = new_container_builder(&SparkConnectContainer::Spark.to_container_name());
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
        .add_container_port(GRPC, CONNECT_GRPC_PORT.into())
        .add_container_port(HTTP, CONNECT_UI_PORT.into())
        .add_env_vars(container_env)
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG.as_ref(), VOLUME_MOUNT_PATH_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG.as_ref(), VOLUME_MOUNT_PATH_LOG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_VOLUME_NAME.as_ref(), LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mounts(s3_volume_mounts)
        .context(AddVolumeMountSnafu)?
        .readiness_probe(probe())
        .liveness_probe(probe());

    // Add custom log4j config map volumes if configured
    if let Some(cm_name) = config.log_config_map() {
        pb.add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG_CONFIG.as_ref())
                .with_config_map(cm_name)
                .build(),
        )
        .context(AddVolumeSnafu)?;

        container
            .add_volume_mount(
                VOLUME_MOUNT_NAME_LOG_CONFIG.as_ref(),
                VOLUME_MOUNT_PATH_LOG_CONFIG,
            )
            .context(AddVolumeMountSnafu)?;
    }

    pb.add_container(container.build());

    if let Some(vector_log_config) = &validated.server_logging.vector_container {
        // These resource names are constructed SOLELY to provide the Vector sidecar with its
        // `CLUSTER_NAME`/`ROLE_NAME`/`ROLE_GROUP_NAME` log-metadata env vars. They do NOT affect
        // resource naming: Spark Connect keeps its `{cluster}-{role}` resource names.
        let vector_resource_names = ResourceNames {
            cluster_name: validated.name.clone(),
            role_name: RoleName::from_str(CONNECT_SERVER_ROLE_NAME)
                .expect("CONNECT_SERVER_ROLE_NAME is a valid role name"),
            role_group_name: RoleGroupName::from_str(DEFAULT_SPARK_CONNECT_GROUP_NAME)
                .expect("DEFAULT_SPARK_CONNECT_GROUP_NAME is a valid role group name"),
        };

        pb.add_container(vector_container(
            &SparkConnectContainer::Vector.to_container_name(),
            resolved_product_image,
            vector_log_config,
            &vector_resource_names,
            &VOLUME_MOUNT_NAME_CONFIG,
            &VOLUME_MOUNT_NAME_LOG,
            EnvVarSet::new(),
        ));
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

    // S3: Add volumes (credentials and certificates) needed for accessing S3 buckets.
    pb.add_volumes(s3_volumes).context(AddVolumeSnafu)?;

    // S3: Add truststore init container for S3 endpoint communication with TLS.
    if let Some(truststore_init_container) = resolved_s3
        .truststore_init_container(resolved_product_image.clone())
        .context(TrustStoreInitContainerSnafu)?
    {
        pb.add_init_container(truststore_init_container);
    }

    // Merge user defined pod template if available
    let mut pod_template = pb.build_template();
    pod_template.merge_from(validated.server_overrides.pod_overrides.clone());

    Ok(StatefulSet {
        metadata: object_meta(
            validated,
            object_name(&validated.name_any(), SparkConnectRole::Server),
            SparkConnectRole::Server,
        )
        .build(),
        spec: Some(StatefulSetSpec {
            template: pod_template,
            replicas: Some(1),
            volume_claim_templates,
            selector: LabelSelector {
                match_labels: Some(
                    validated
                        .role_group_selector(SparkConnectRole::Server)
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
    let mut command = formatdoc! { "
    containerdebug --output={VOLUME_MOUNT_PATH_LOG}/containerdebug-state.json --loop &

    cp {VOLUME_MOUNT_PATH_CONFIG}/{SPARK_DEFAULTS_FILE_NAME} /tmp/spark.properties
    config-utils template /tmp/spark.properties

    /stackable/spark/sbin/start-connect-server.sh \\
    --deploy-mode client \\
    --master k8s://https://${{KUBERNETES_SERVICE_HOST}}:${{KUBERNETES_SERVICE_PORT_HTTPS}} \\
    --properties-file /tmp/spark.properties"};

    // Append user-provided command line arguments as continuation lines.
    for user_arg in user_args {
        command.push_str(&format!(" \\\n{user_arg}"));
    }
    vec![command]
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
    validated: &ValidatedSparkConnectServer,
    driver_service: &Service,
) -> Result<BTreeMap<String, Option<String>>, Error> {
    let config = &validated.server_config;
    let resolved_product_image = &validated.resolved_product_image;
    let spark_image = resolved_product_image.image.clone();
    let spark_version = resolved_product_image.product_version.clone();
    let service_account_name = validated.cluster_resource_names().service_account_name();
    let namespace = driver_service
        .namespace()
        .context(ObjectHasNoNamespaceSnafu)?;

    let config_overrides = validated
        .server_overrides
        .config_overrides
        .spark_defaults_conf
        .overrides
        .clone();

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
            Some(service_account_name.to_string()),
        ),
        (
            "spark.kubernetes.driver.pod.name".to_string(),
            Some("${env:HOSTNAME}".to_string()),
        ),
        (
            "spark.driver.defaultJavaOptions".to_string(),
            Some(server_jvm_args(
                validated.server_overrides.jvm_config.as_ref(),
                config,
            )),
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

    result.extend(config_overrides.into_iter().map(|(k, v)| (k, Some(v))));

    Ok(result)
}

fn server_jvm_args(
    jvm_config: Option<&JavaCommonConfig>,
    config: &v1alpha1::ServerConfig,
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

fn probe() -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            port: IntOrString::Int(CONNECT_UI_PORT.into()),
            scheme: Some("HTTP".to_string()),
            path: Some("/metrics/prometheus".to_string()),
            ..Default::default()
        }),
        failure_threshold: Some(10),
        ..Probe::default()
    }
}

pub(crate) fn build_listener(
    validated: &ValidatedSparkConnectServer,
) -> listener::v1alpha1::Listener {
    let listener_name = format!(
        "{cluster}-{role}",
        cluster = validated.name_any(),
        role = SparkConnectRole::Server
    );

    let listener_class = validated.role_config.listener_class.clone();
    let recommended_object_labels = validated.recommended_labels(SparkConnectRole::Server);

    let listener_ports = [
        listener::v1alpha1::ListenerPort {
            name: GRPC.to_string(),
            port: CONNECT_GRPC_PORT.into(),
            protocol: Some("TCP".to_string()),
        },
        listener::v1alpha1::ListenerPort {
            name: HTTP.to_string(),
            port: CONNECT_UI_PORT.into(),
            protocol: Some("TCP".to_string()),
        },
    ];

    listener_ext::build_listener(
        validated,
        &listener_name,
        &listener_class,
        recommended_object_labels,
        &listener_ports,
    )
}
