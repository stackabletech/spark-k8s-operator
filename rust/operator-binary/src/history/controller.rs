use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, container::ContainerBuilder, volume::VolumeBuilder},
    },
    cluster_resources::ClusterResourceApplyStrategy,
    commons::rbac::build_rbac_resources,
    crd::listener,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMap, PodSecurityContext, ServiceAccount},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::{
        ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    product_logging::{
        framework::calculate_log_volume_size_limit,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    shared::time::Duration,
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::{
                container::{EnvVarName, EnvVarSet},
                volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
            },
        },
        cluster_resources::cluster_resources_new,
        config_file_writer::{PropertiesWriterError, to_java_properties_string},
        product_logging::framework::vector_container,
        types::{kubernetes::PersistentVolumeClaimName, operator::RoleGroupName},
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

// PVC name for the listener volume, required by the v2 listener-volume builder. Its value matches
// `LISTENER_VOLUME_NAME` in `crd::constants`.
stackable_operator::constant!(LISTENER_VOLUME_NAME_PVC: PersistentVolumeClaimName = "listener");

use crate::{
    Ctx,
    crd::{
        constants::{
            ACCESS_KEY_ID, HISTORY_APP_NAME, HISTORY_ROLE_NAME, HISTORY_UI_PORT,
            JVM_SECURITY_PROPERTIES_FILE, LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME,
            MAX_SPARK_LOG_FILES_SIZE, METRICS_PORT, SECRET_ACCESS_KEY, SPARK_DEFAULTS_FILE_NAME,
            SPARK_ENV_SH_FILE_NAME, STACKABLE_TRUST_STORE, VECTOR_CONTAINER_NAME,
            VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_NAME_CONFIG_TYPED, VOLUME_MOUNT_NAME_LOG,
            VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_NAME_LOG_TYPED, VOLUME_MOUNT_PATH_CONFIG,
            VOLUME_MOUNT_PATH_LOG, VOLUME_MOUNT_PATH_LOG_CONFIG, default_jvm_security_properties,
        },
        history::{SparkHistoryServerContainer, v1alpha1},
        listener_ext,
        logdir::ResolvedLogDir,
        tlscerts, to_spark_env_sh_string,
    },
    history::{
        config::jvm::construct_history_jvm_args, controller::validate::ValidatedHistoryRoleGroup,
        operations::pdb::build_pdb, service::build_rolegroup_metrics_service,
    },
    product_logging::{self},
};

pub mod dereference;
pub mod validate;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("invalid config map {name}"))]
    InvalidConfigMap {
        source: stackable_operator::builder::configmap::Error,
        name: String,
    },

    #[snafu(display("invalid history container name"))]
    InvalidContainerName {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to update the history server stateful set"))]
    ApplyStatefulSet {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update history server config map"))]
    ApplyConfigMap {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update history server metrics service"))]
    ApplyMetricsService {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply role ServiceAccount"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply global RoleBinding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to dereference SparkHistoryServer"))]
    DereferenceSparkHistoryServer { source: dereference::Error },

    #[snafu(display("failed to validate SparkHistoryServer"))]
    ValidateSparkHistoryServer { source: validate::Error },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: product_logging::Error,
        cm_name: String,
    },

    #[snafu(display(
        "History server : failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for group {}",
        rolegroup
    ))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("failed to apply PodDisruptionBudget"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("failed to create the log dir volumes specification"))]
    CreateLogDirVolumesSpec { source: crate::crd::logdir::Error },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("SparkHistoryServer object is invalid"))]
    InvalidSparkHistoryServer {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to apply group listener"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to serialize Spark default properties"))]
    InvalidSparkDefaults { source: PropertiesWriterError },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}
/// Updates the status of the SparkApplication that started the pod.
pub async fn reconcile(
    shs: Arc<DeserializeGuard<v1alpha1::SparkHistoryServer>>,
    ctx: Arc<Ctx>,
) -> Result<Action, Error> {
    tracing::info!("Starting reconcile history server");

    let shs = shs
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidSparkHistoryServerSnafu)?;

    let client = &ctx.client;

    let dereferenced = dereference::dereference(client, shs)
        .await
        .context(DereferenceSparkHistoryServerSnafu)?;

    let validated = validate::validate(shs, dereferenced, &ctx.operator_environment)
        .context(ValidateSparkHistoryServerSnafu)?;

    let mut cluster_resources = cluster_resources_new(
        &validate::product_name(),
        &validate::operator_name(),
        &validate::controller_name(),
        &validated.name,
        &validated.namespace,
        &validated.uid,
        ClusterResourceApplyStrategy::Default,
        &shs.spec.object_overrides,
    );

    let log_dir = &validated.cluster_config.log_dir;

    // Use a dedicated service account for history server pods.
    let (service_account, role_binding) = build_rbac_resources(
        shs,
        HISTORY_APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(GetRequiredLabelsSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;
    let service_account = cluster_resources
        .add(client, service_account)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, role_binding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    for (role_group_name, rg) in &validated.role_groups {
        let config_map = build_config_map(&validated, role_group_name, rg)?;

        let metrics_service = build_rolegroup_metrics_service(&validated, role_group_name);

        let sts = build_stateful_set(&validated, role_group_name, rg, log_dir, &service_account)?;

        cluster_resources
            .add(client, config_map)
            .await
            .context(ApplyConfigMapSnafu)?;
        cluster_resources
            .add(client, metrics_service)
            .await
            .context(ApplyMetricsServiceSnafu)?;
        cluster_resources
            .add(client, sts)
            .await
            .context(ApplyStatefulSetSnafu)?;
    }

    let rg_group_listener = build_group_listener(
        &validated,
        HISTORY_ROLE_NAME,
        validated.role_config.listener_class.clone(),
    );

    cluster_resources
        .add(client, rg_group_listener)
        .await
        .context(ApplyGroupListenerSnafu)?;

    if let Some(pdb) = build_pdb(&validated.role_config.pdb, &validated) {
        cluster_resources
            .add(client, pdb)
            .await
            .context(ApplyPdbSnafu)?;
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    Ok(Action::await_change())
}

fn build_group_listener(
    validated: &validate::ValidatedSparkHistoryServer,
    role: &str,
    listener_class: String,
) -> listener::v1alpha1::Listener {
    let listener_name = group_listener_name(validated, role);

    // Group listeners are shared across role groups, so the role-group label is "none" (preserving
    // the previous behaviour).
    let recommended_object_labels = validated.recommended_labels(
        &RoleGroupName::from_str("none").expect("\"none\" is a valid role group name"),
    );

    let listener_ports = [listener::v1alpha1::ListenerPort {
        name: "http".to_string(),
        port: HISTORY_UI_PORT.into(),
        protocol: Some("TCP".to_string()),
    }];

    listener_ext::build_listener(
        validated,
        &listener_name,
        &listener_class,
        recommended_object_labels,
        &listener_ports,
    )
}

fn group_listener_name(validated: &validate::ValidatedSparkHistoryServer, role: &str) -> String {
    format!("{cluster}-{role}", cluster = validated.name_any())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::SparkHistoryServer>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidSparkHistoryServer { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

#[allow(clippy::result_large_err)]
fn build_config_map(
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
                .ownerreference(ownerreference_from_resource(
                    validated,
                    Some(true),
                    Some(true),
                ))
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

    product_logging::extend_config_map(
        &rg.config.config.logging,
        SparkHistoryServerContainer::SparkHistory,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu { cm_name: &cm_name })?;

    cm_builder
        .build()
        .context(InvalidConfigMapSnafu { name: cm_name })
}

#[allow(clippy::result_large_err)]
fn build_stateful_set(
    validated: &validate::ValidatedSparkHistoryServer,
    role_group_name: &RoleGroupName,
    rg: &ValidatedHistoryRoleGroup,
    log_dir: &ResolvedLogDir,
    serviceaccount: &ServiceAccount,
) -> Result<StatefulSet, Error> {
    let resolved_product_image = &validated.resolved_product_image;
    let resource_names = validated.resource_names(role_group_name);

    let log_config_map = if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = rg
        .config
        .config
        .logging
        .containers
        .get(&SparkHistoryServerContainer::SparkHistory)
    {
        config_map.into()
    } else {
        resource_names.role_group_config_map().to_string()
    };

    let recommended_labels = validated.recommended_labels(role_group_name);

    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    let mut pb = PodBuilder::new();

    let requested_secret_lifetime = rg
        .config
        .config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    pb.service_account_name(serviceaccount.name_unchecked())
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
                .with_config_map(resource_names.role_group_config_map().to_string())
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
        .add_volumes(
            log_dir
                .volumes(&requested_secret_lifetime)
                .context(CreateLogDirVolumesSpecSnafu)?,
        )
        .context(AddVolumeSnafu)?
        .security_context(PodSecurityContext {
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        });

    // Base environment variables, with the already-merged (role + role group) env overrides
    // layered on top (overrides win). The base names are static and known to be valid.
    let known_env_var_name = |name: &str| {
        EnvVarName::from_str(name).expect("the operator-generated env var name is valid")
    };
    let merged_env = EnvVarSet::new()
        .with_values([
            // Needed by the `containerdebug` running in the background of the history container
            // to log it's tracing information to.
            (
                known_env_var_name("CONTAINERDEBUG_LOG_DIRECTORY"),
                format!("{VOLUME_MOUNT_PATH_LOG}/containerdebug"),
            ),
            // This env var prevents the history server from detaching itself from the
            // start script because this leads to the Pod terminating immediately.
            (known_env_var_name("SPARK_NO_DAEMONIZE"), "true".to_owned()),
            (
                known_env_var_name("SPARK_DAEMON_CLASSPATH"),
                "/stackable/spark/extra-jars/*".to_owned(),
            ),
            // JVM arguments for the history server.
            (
                known_env_var_name("SPARK_HISTORY_OPTS"),
                construct_history_jvm_args(&rg.config, log_dir),
            ),
        ])
        .merge(rg.config.env_overrides.clone());

    let container_name = "spark-history";
    let container = ContainerBuilder::new(container_name)
        .context(InvalidContainerNameSnafu)?
        .image_from_product_image(resolved_product_image)
        .resources(rg.config.config.resources.clone().into())
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(command_args(log_dir))
        .add_container_port("http", HISTORY_UI_PORT.into())
        .add_container_port("metrics", METRICS_PORT.into())
        .add_env_vars(merged_env)
        .add_volume_mounts(log_dir.volume_mounts())
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_PATH_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_LOG_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?
        .build();

    // Add listener volume
    // Listener endpoints for the Webserver role will use persistent volumes
    // so that load balancers can hard-code the target addresses. This will
    // be the case even when no class is set (and the value defaults to
    // cluster-internal) as the address should still be consistent.
    let volume_claim_templates = Some(vec![listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(
            group_listener_name(validated, HISTORY_ROLE_NAME)
                .parse()
                .expect("the group listener name is a valid ListenerName"),
        ),
        &recommended_labels,
        &LISTENER_VOLUME_NAME_PVC,
    )]);

    pb.add_container(container);

    if let Some(vector_log_config) = &rg.logging.vector_container {
        pb.add_container(vector_container(
            &VECTOR_CONTAINER_NAME,
            resolved_product_image,
            vector_log_config,
            &resource_names,
            &VOLUME_MOUNT_NAME_CONFIG_TYPED,
            &VOLUME_MOUNT_NAME_LOG_TYPED,
            EnvVarSet::new(),
        ));
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(rg.config.pod_overrides.clone());

    let sts_metadata = validated
        .object_meta(
            resource_names.stateful_set_name().to_string(),
            role_group_name,
        )
        .build();

    Ok(StatefulSet {
        metadata: sts_metadata,
        spec: Some(StatefulSetSpec {
            template: pod_template,
            volume_claim_templates,
            replicas: Some(i32::from(rg.config.replicas)),
            selector: LabelSelector {
                match_labels: Some(validated.role_group_selector(role_group_name).into()),
                ..LabelSelector::default()
            },
            ..StatefulSetSpec::default()
        }),
        ..StatefulSet::default()
    })
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

fn command_args(logdir: &ResolvedLogDir) -> Vec<String> {
    let mut command = vec![];

    if let Some(secret_dir) = logdir.credentials_mount_path() {
        command.extend(vec![
            format!("export AWS_ACCESS_KEY_ID=\"$(cat {secret_dir}/{ACCESS_KEY_ID})\""),
            format!("export AWS_SECRET_ACCESS_KEY=\"$(cat {secret_dir}/{SECRET_ACCESS_KEY})\""),
        ]);
    }

    if let Some(secret_name) = logdir.tls_secret_name() {
        command.push(format!("mkdir -p {STACKABLE_TRUST_STORE}"));
        command.push(tlscerts::convert_system_trust_store_to_pkcs12());
        command.push(tlscerts::import_truststore(secret_name));
    }

    command.extend(vec![
        format!("containerdebug --output={VOLUME_MOUNT_PATH_LOG}/containerdebug-state.json --loop &"),
        format!("/stackable/spark/sbin/start-history-server.sh --properties-file {VOLUME_MOUNT_PATH_CONFIG}/{SPARK_DEFAULTS_FILE_NAME}"),
    ]);
    vec![command.join("\n")]
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
