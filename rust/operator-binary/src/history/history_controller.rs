use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use product_config::{types::PropertyNameKind, writer::to_java_properties_string};
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
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        product_image_selection::{self, ResolvedProductImage},
        rbac::build_rbac_resources,
    },
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
        Resource, ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    kvp::{Labels, ObjectLabels},
    logging::controller::ReconcilerError,
    product_logging::{
        framework::{LoggingError, calculate_log_volume_size_limit, vector_container},
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    Ctx,
    crd::{
        constants::{
            ACCESS_KEY_ID, HISTORY_APP_NAME, HISTORY_CONTROLLER_NAME, HISTORY_ROLE_NAME,
            HISTORY_UI_PORT, JVM_SECURITY_PROPERTIES_FILE, LISTENER_VOLUME_DIR,
            LISTENER_VOLUME_NAME, MAX_SPARK_LOG_FILES_SIZE, METRICS_PORT, OPERATOR_NAME,
            SECRET_ACCESS_KEY, SPARK_DEFAULTS_FILE_NAME, SPARK_ENV_SH_FILE_NAME,
            SPARK_IMAGE_BASE_NAME, STACKABLE_TRUST_STORE, VOLUME_MOUNT_NAME_CONFIG,
            VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_CONFIG,
            VOLUME_MOUNT_PATH_LOG, VOLUME_MOUNT_PATH_LOG_CONFIG,
        },
        history::{self, HistoryConfig, SparkHistoryServerContainer, v1alpha1},
        listener_ext,
        logdir::ResolvedLogDir,
        tlscerts, to_spark_env_sh_string,
    },
    history::operations::pdb::add_pdbs,
    product_logging::{self},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to build spark history group listener"))]
    BuildListener {
        source: crate::crd::listener_ext::Error,
    },

    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("invalid config map {name}"))]
    InvalidConfigMap {
        source: stackable_operator::builder::configmap::Error,
        name: String,
    },

    #[snafu(display("invalid history container name"))]
    InvalidContainerName {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to update the history server stateful set"))]
    ApplyStatefulSet {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update history server config map"))]
    ApplyConfigMap {
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

    #[snafu(display("product config validation failed"))]
    ProductConfigValidation { source: crate::crd::history::Error },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::history::Error },

    #[snafu(display("number of cleaner rolegroups exceeds 1"))]
    TooManyCleanerRoleGroups,

    #[snafu(display("number of cleaner replicas exceeds 1"))]
    TooManyCleanerReplicas,

    #[snafu(display("failed to resolve the log dir configuration"))]
    LogDir { source: crate::crd::logdir::Error },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("cannot retrieve role group"))]
    CannotRetrieveRoleGroup { source: history::Error },

    #[snafu(display(
        "History server : failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for group {}",
        rolegroup
    ))]
    JvmSecurityProperties {
        source: product_config::writer::PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::history::operations::pdb::Error,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
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

    #[snafu(display("failed to merge environment config and/or overrides"))]
    MergeEnv { source: crate::crd::history::Error },

    #[snafu(display("failed to apply group listener"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },
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

    let mut cluster_resources = ClusterResources::new(
        HISTORY_APP_NAME,
        OPERATOR_NAME,
        HISTORY_CONTROLLER_NAME,
        &shs.object_ref(&()),
        ClusterResourceApplyStrategy::Default,
    )
    .context(CreateClusterResourcesSnafu)?;

    let resolved_product_image = shs
        .spec
        .image
        .resolve(SPARK_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION)
        .context(ResolveProductImageSnafu)?;
    let log_dir = ResolvedLogDir::resolve(
        &shs.spec.log_file_directory,
        shs.metadata.namespace.clone(),
        client,
    )
    .await
    .context(LogDirSnafu)?;

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

    // The role_name is always HISTORY_ROLE_NAME
    for (role_name, role_config) in shs
        .validated_role_config(&resolved_product_image, &ctx.product_config)
        .context(ProductConfigValidationSnafu)?
        .iter()
    {
        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rgr = RoleGroupRef {
                cluster: ObjectRef::from_obj(shs),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let merged_config = shs
                .merged_config(&rgr)
                .context(FailedToResolveConfigSnafu)?;

            let config_map = build_config_map(
                shs,
                rolegroup_config,
                &merged_config,
                &resolved_product_image.app_version_label_value,
                &rgr,
                &log_dir,
            )?;
            cluster_resources
                .add(client, config_map)
                .await
                .context(ApplyConfigMapSnafu)?;

            let sts = build_stateful_set(
                shs,
                &resolved_product_image,
                &rgr,
                &log_dir,
                &merged_config,
                &service_account,
            )?;
            cluster_resources
                .add(client, sts)
                .await
                .context(ApplyStatefulSetSnafu)?;
        }

        let rg_group_listener = build_group_listener(
            shs,
            &resolved_product_image,
            role_name,
            shs.node_listener_class().to_string(),
        )?;

        cluster_resources
            .add(client, rg_group_listener)
            .await
            .context(ApplyGroupListenerSnafu)?;

        let role_config = &shs.spec.nodes.role_config;
        add_pdbs(
            &role_config.common.pod_disruption_budget,
            shs,
            client,
            &mut cluster_resources,
        )
        .await
        .context(FailedToCreatePdbSnafu)?;
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    Ok(Action::await_change())
}

#[allow(clippy::result_large_err)]
fn build_group_listener(
    shs: &v1alpha1::SparkHistoryServer,
    resolved_product_image: &ResolvedProductImage,
    role: &str,
    listener_class: String,
) -> Result<listener::v1alpha1::Listener, Error> {
    let listener_name = group_listener_name(shs, role);

    let recommended_object_labels =
        labels(shs, &resolved_product_image.app_version_label_value, "none");

    let listener_ports = [listener::v1alpha1::ListenerPort {
        name: "http".to_string(),
        port: HISTORY_UI_PORT.into(),
        protocol: Some("TCP".to_string()),
    }];

    listener_ext::build_listener(
        shs,
        &listener_name,
        &listener_class,
        recommended_object_labels,
        &listener_ports,
    )
    .context(BuildListenerSnafu)
}

fn group_listener_name(shs: &v1alpha1::SparkHistoryServer, role: &str) -> String {
    format!("{cluster}-{role}", cluster = shs.name_any())
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
    shs: &v1alpha1::SparkHistoryServer,
    config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    merged_config: &HistoryConfig,
    app_version_label_value: &str,
    rolegroupref: &RoleGroupRef<v1alpha1::SparkHistoryServer>,
    log_dir: &ResolvedLogDir,
) -> Result<ConfigMap, Error> {
    let cm_name = rolegroupref.object_name();

    let spark_defaults = spark_defaults(shs, log_dir, rolegroupref)?;

    let jvm_sec_props: BTreeMap<String, Option<String>> = config
        .get(&PropertyNameKind::File(
            JVM_SECURITY_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect();

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(shs)
                .name(&cm_name)
                .ownerreference_from_resource(shs, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(labels(
                    shs,
                    app_version_label_value,
                    &rolegroupref.role_group,
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(SPARK_DEFAULTS_FILE_NAME, spark_defaults)
        .add_data(
            SPARK_ENV_SH_FILE_NAME,
            to_spark_env_sh_string(
                config
                    .get(&PropertyNameKind::File(SPARK_ENV_SH_FILE_NAME.to_string()))
                    .cloned()
                    .unwrap_or_default()
                    .iter(),
            ),
        )
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                JvmSecurityPropertiesSnafu {
                    rolegroup: rolegroupref.role_group.clone(),
                }
            })?,
        );

    product_logging::extend_config_map(
        rolegroupref,
        &merged_config.logging,
        SparkHistoryServerContainer::SparkHistory,
        SparkHistoryServerContainer::Vector,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu { cm_name: &cm_name })?;

    cm_builder
        .build()
        .context(InvalidConfigMapSnafu { name: cm_name })
}

#[allow(clippy::result_large_err)]
fn build_stateful_set(
    shs: &v1alpha1::SparkHistoryServer,
    resolved_product_image: &ResolvedProductImage,
    rolegroupref: &RoleGroupRef<v1alpha1::SparkHistoryServer>,
    log_dir: &ResolvedLogDir,
    merged_config: &HistoryConfig,
    serviceaccount: &ServiceAccount,
) -> Result<StatefulSet, Error> {
    let log_config_map = if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = merged_config
        .logging
        .containers
        .get(&SparkHistoryServerContainer::SparkHistory)
    {
        config_map.into()
    } else {
        rolegroupref.object_name()
    };

    let recommended_object_labels = labels(
        shs,
        &resolved_product_image.app_version_label_value,
        rolegroupref.role_group.as_ref(),
    );
    let recommended_labels =
        Labels::recommended(recommended_object_labels.clone()).context(LabelBuildSnafu)?;

    let pb_metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(recommended_object_labels.clone())
        .context(MetadataBuildSnafu)?
        .build();

    let mut pb = PodBuilder::new();

    let requested_secret_lifetime = merged_config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    pb.service_account_name(serviceaccount.name_unchecked())
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
                .with_config_map(rolegroupref.object_name())
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

    let role_group = shs
        .rolegroup(rolegroupref)
        .with_context(|_| CannotRetrieveRoleGroupSnafu)?;

    let merged_env = shs
        .merged_env(
            &rolegroupref.role_group,
            log_dir,
            role_group.config.env_overrides,
        )
        .context(MergeEnvSnafu)?;

    let container_name = "spark-history";
    let container = ContainerBuilder::new(container_name)
        .context(InvalidContainerNameSnafu)?
        .image_from_product_image(resolved_product_image)
        .resources(merged_config.resources.clone().into())
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
    let volume_claim_templates = Some(vec![
        ListenerOperatorVolumeSourceBuilder::new(
            &ListenerReference::ListenerName(group_listener_name(shs, &rolegroupref.role)),
            &recommended_labels,
        )
        .build_pvc(LISTENER_VOLUME_NAME.to_string())
        .context(BuildListenerVolumeSnafu)?,
    ]);

    pb.add_container(container);

    if merged_config.logging.enable_vector_agent {
        match &shs.spec.vector_aggregator_config_map_name {
            Some(vector_aggregator_config_map_name) => {
                pb.add_container(
                    vector_container(
                        resolved_product_image,
                        VOLUME_MOUNT_NAME_CONFIG,
                        VOLUME_MOUNT_NAME_LOG,
                        merged_config
                            .logging
                            .containers
                            .get(&SparkHistoryServerContainer::Vector),
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("500m")
                            .with_memory_request("128Mi")
                            .with_memory_limit("128Mi")
                            .build(),
                        vector_aggregator_config_map_name,
                    )
                    .context(ConfigureLoggingSnafu)?,
                );
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(shs.role().config.pod_overrides.clone());
    pod_template.merge_from(role_group.config.pod_overrides);

    let sts_metadata = ObjectMetaBuilder::new()
        .name_and_namespace(shs)
        .name(rolegroupref.object_name())
        .ownerreference_from_resource(shs, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(recommended_object_labels)
        .context(MetadataBuildSnafu)?
        .build();

    Ok(StatefulSet {
        metadata: sts_metadata,
        spec: Some(StatefulSetSpec {
            template: pod_template,
            volume_claim_templates,
            replicas: shs.replicas(rolegroupref),
            selector: LabelSelector {
                match_labels: Some(
                    Labels::role_group_selector(
                        shs,
                        HISTORY_APP_NAME,
                        &rolegroupref.role,
                        &rolegroupref.role_group,
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
fn spark_defaults(
    shs: &v1alpha1::SparkHistoryServer,
    log_dir: &ResolvedLogDir,
    rolegroupref: &RoleGroupRef<v1alpha1::SparkHistoryServer>,
) -> Result<String, Error> {
    let mut log_dir_settings = log_dir.history_server_spark_config().context(LogDirSnafu)?;

    // add cleaner spark settings if requested
    log_dir_settings.extend(cleaner_config(shs, rolegroupref)?);

    // add user provided configuration. These can overwrite everything.
    log_dir_settings.extend(shs.spec.spark_conf.clone());

    // stringify the spark configuration for the ConfigMap
    Ok(log_dir_settings
        .iter()
        .map(|(k, v)| format!("{k} {v}"))
        .collect::<Vec<String>>()
        .join("\n"))
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
        command.extend(vec![format!("mkdir -p {STACKABLE_TRUST_STORE}")]);
        command.extend(tlscerts::convert_system_trust_store_to_pkcs12());
        command.extend(tlscerts::import_truststore(secret_name));
    }

    command.extend(vec![
        format!("containerdebug --output={VOLUME_MOUNT_PATH_LOG}/containerdebug-state.json --loop &"),
        format!("/stackable/spark/sbin/start-history-server.sh --properties-file {VOLUME_MOUNT_PATH_CONFIG}/{SPARK_DEFAULTS_FILE_NAME}"),
    ]);
    vec![command.join("\n")]
}

fn labels<'a, T>(
    shs: &'a T,
    app_version_label_value: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner: shs,
        app_name: HISTORY_APP_NAME,
        app_version: app_version_label_value,
        operator_name: OPERATOR_NAME,
        controller_name: HISTORY_CONTROLLER_NAME,
        role: HISTORY_ROLE_NAME,
        role_group,
    }
}

/// Return the Spark properties for the cleaner role group (if any).
/// There should be only one role group with "cleaner=true" and this
/// group should have a replica count of 0 or 1.
#[allow(clippy::result_large_err)]
fn cleaner_config(
    shs: &v1alpha1::SparkHistoryServer,
    rolegroup_ref: &RoleGroupRef<v1alpha1::SparkHistoryServer>,
) -> Result<BTreeMap<String, String>, Error> {
    let mut result = BTreeMap::new();

    // all role groups with "cleaner=true"
    let cleaner_rolegroups = shs.cleaner_rolegroups();

    // should have max of one
    if cleaner_rolegroups.len() > 1 {
        return TooManyCleanerRoleGroupsSnafu.fail();
    }

    // check if cleaner is set for this rolegroup ref
    if cleaner_rolegroups.len() == 1 && cleaner_rolegroups[0].role_group == rolegroup_ref.role_group
    {
        if let Some(replicas) = shs.replicas(rolegroup_ref) {
            if replicas > 1 {
                return TooManyCleanerReplicasSnafu.fail();
            } else {
                result.insert(
                    "spark.history.fs.cleaner.enabled".to_string(),
                    "true".to_string(),
                );
            }
        }
    }

    Ok(result)
}
