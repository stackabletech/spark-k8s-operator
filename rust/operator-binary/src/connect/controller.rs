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
            container::ContainerBuilder, resources::ResourceRequirementsBuilder,
            volume::VolumeBuilder, PodBuilder,
        },
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, PodSecurityContext, Service, ServiceAccount, ServicePort, ServiceSpec,
            },
            rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
        DeepMerge,
    },
    kube::{
        core::{error_boundary, DeserializeGuard},
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    kvp::{Label, Labels, ObjectLabels},
    logging::controller::ReconcilerError,
    product_logging::{
        framework::{calculate_log_volume_size_limit, vector_container, LoggingError},
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    connect::crd::{
        v1alpha1, ConnectConfig, SparkConnectServerContainer, CONNECT_CONTAINER_NAME,
        CONNECT_CONTROLLER_NAME, CONNECT_ROLE_NAME,
    },
    connect::pdb::add_pdbs,
    crd::{
        constants::{
            ACCESS_KEY_ID, APP_NAME, JVM_SECURITY_PROPERTIES_FILE, MAX_SPARK_LOG_FILES_SIZE,
            METRICS_PORT, OPERATOR_NAME, SECRET_ACCESS_KEY, SPARK_CLUSTER_ROLE,
            SPARK_DEFAULTS_FILE_NAME, SPARK_ENV_SH_FILE_NAME, SPARK_IMAGE_BASE_NAME, SPARK_UID,
            STACKABLE_TRUST_STORE, VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_NAME_LOG,
            VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_CONFIG, VOLUME_MOUNT_PATH_LOG,
            VOLUME_MOUNT_PATH_LOG_CONFIG,
        },
        tlscerts, to_spark_env_sh_string,
    },
    product_logging::{self, resolve_vector_aggregator_address},
    Ctx,
};

use super::crd::{CONNECT_GRPC_PORT, CONNECT_UI_PORT};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("invalid config map {name}"))]
    InvalidConfigMap {
        source: stackable_operator::builder::configmap::Error,
        name: String,
    },

    #[snafu(display("invalid connect container name"))]
    InvalidContainerName {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to update the connect server deployment"))]
    ApplyDeployment {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update connect server config map"))]
    ApplyConfigMap {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update connect server service"))]
    ApplyService {
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
    ProductConfigValidation { source: crate::connect::crd::Error },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::connect::crd::Error },

    #[snafu(display("number of cleaner rolegroups exceeds 1"))]
    TooManyCleanerRoleGroups,

    #[snafu(display("number of cleaner replicas exceeds 1"))]
    TooManyCleanerReplicas,

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress { source: product_logging::Error },

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("cannot retrieve role group"))]
    CannotRetrieveRoleGroup { source: crate::connect::crd::Error },

    #[snafu(display(
        "Connect server : failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for group {}",
        rolegroup
    ))]
    JvmSecurityProperties {
        source: product_config::writer::PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb { source: crate::connect::pdb::Error },

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

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("SparkConnectServer object is invalid"))]
    InvalidSparkConnectServer {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to merge environment config and/or overrides"))]
    MergeEnv { source: crate::connect::crd::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}
/// Updates the status of the SparkApplication that started the pod.
pub async fn reconcile(
    scs: Arc<DeserializeGuard<v1alpha1::SparkConnectServer>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile connect server");

    let scs = scs
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidSparkConnectServerSnafu)?;

    let client = &ctx.client;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        CONNECT_CONTROLLER_NAME,
        &scs.object_ref(&()),
        ClusterResourceApplyStrategy::Default,
    )
    .context(CreateClusterResourcesSnafu)?;

    let resolved_product_image = scs
        .spec
        .image
        .resolve(SPARK_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);

    let vector_aggregator_address = resolve_vector_aggregator_address(
        client,
        scs.namespace()
            .as_deref()
            .context(ObjectHasNoNamespaceSnafu)?,
        scs.spec.vector_aggregator_config_map_name.as_deref(),
    )
    .await
    .context(ResolveVectorAggregatorAddressSnafu)?;

    // Use a dedicated service account for connect server pods.
    let (serviceaccount, rolebinding) =
        build_connect_role_serviceaccount(scs, &resolved_product_image.app_version_label)?;
    let serviceaccount = cluster_resources
        .add(client, serviceaccount)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    // The role_name is always connect_ROLE_NAME
    for (role_name, role_config) in scs
        .validated_role_config(&resolved_product_image, &ctx.product_config)
        .context(ProductConfigValidationSnafu)?
        .iter()
    {
        let service = build_service(
            scs,
            &resolved_product_image.app_version_label,
            role_name,
            None,
        )?;
        cluster_resources
            .add(client, service)
            .await
            .context(ApplyServiceSnafu)?;

        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rgr = RoleGroupRef {
                cluster: ObjectRef::from_obj(scs),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let merged_config = scs
                .merged_config(&rgr)
                .context(FailedToResolveConfigSnafu)?;

            let service = build_service(
                scs,
                &resolved_product_image.app_version_label,
                role_name,
                Some(&rgr),
            )?;
            cluster_resources
                .add(client, service)
                .await
                .context(ApplyServiceSnafu)?;

            let config_map = build_config_map(
                scs,
                rolegroup_config,
                &merged_config,
                &resolved_product_image.app_version_label,
                &rgr,
                vector_aggregator_address.as_deref(),
            )?;
            cluster_resources
                .add(client, config_map)
                .await
                .context(ApplyConfigMapSnafu)?;

            let sts = build_stateful_set(
                scs,
                &resolved_product_image,
                &rgr,
                &merged_config,
                &serviceaccount,
            )?;
            cluster_resources
                .add(client, sts)
                .await
                .context(ApplyDeploymentSnafu)?;
        }

        let role_config = &scs.spec.nodes.role_config;
        add_pdbs(
            &role_config.pod_disruption_budget,
            scs,
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

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::SparkConnectServer>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidSparkConnectServer { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

#[allow(clippy::result_large_err)]
fn build_config_map(
    scs: &v1alpha1::SparkConnectServer,
    config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    merged_config: &ConnectConfig,
    app_version_label: &str,
    rolegroupref: &RoleGroupRef<v1alpha1::SparkConnectServer>,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap, Error> {
    let cm_name = rolegroupref.object_name();

    let spark_defaults = spark_defaults(scs, rolegroupref)?;

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
                .name_and_namespace(scs)
                .name(&cm_name)
                .ownerreference_from_resource(scs, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(labels(scs, app_version_label, &rolegroupref.role_group))
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
        vector_aggregator_address,
        &merged_config.logging,
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
fn build_stateful_set(
    scs: &v1alpha1::SparkConnectServer,
    resolved_product_image: &ResolvedProductImage,
    rolegroupref: &RoleGroupRef<v1alpha1::SparkConnectServer>,
    merged_config: &ConnectConfig,
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
        .get(&SparkConnectServerContainer::SparkConnect)
    {
        config_map.into()
    } else {
        rolegroupref.object_name()
    };

    let metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(labels(
            scs,
            &resolved_product_image.app_version_label,
            &rolegroupref.role_group,
        ))
        .context(MetadataBuildSnafu)?
        .build();

    let mut pb = PodBuilder::new();

    pb.service_account_name(serviceaccount.name_unchecked())
        .metadata(metadata)
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
        .security_context(PodSecurityContext {
            run_as_user: Some(SPARK_UID),
            run_as_group: Some(0),
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        });

    let role_group = scs
        .rolegroup(rolegroupref)
        .with_context(|_| CannotRetrieveRoleGroupSnafu)?;

    let merged_env = scs
        .merged_env(&rolegroupref.role_group, role_group.config.env_overrides)
        .context(MergeEnvSnafu)?;

    let container = ContainerBuilder::new(CONNECT_CONTAINER_NAME)
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
        .args(command_args(resolved_product_image))
        .add_container_port("grpc", CONNECT_GRPC_PORT)
        .add_container_port("http", CONNECT_UI_PORT)
        .add_container_port("metrics", METRICS_PORT.into())
        .add_env_vars(merged_env)
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_PATH_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_LOG_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
        .context(AddVolumeMountSnafu)?
        .build();
    pb.add_container(container);

    if merged_config.logging.enable_vector_agent {
        pb.add_container(
            vector_container(
                resolved_product_image,
                VOLUME_MOUNT_NAME_CONFIG,
                VOLUME_MOUNT_NAME_LOG,
                merged_config
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

    let mut pod_template = pb.build_template();
    pod_template.merge_from(scs.role().config.pod_overrides.clone());
    pod_template.merge_from(role_group.config.pod_overrides);

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(scs)
            .name(rolegroupref.object_name())
            .ownerreference_from_resource(scs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(
                scs,
                &resolved_product_image.app_version_label,
                rolegroupref.role_group.as_ref(),
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(StatefulSetSpec {
            template: pod_template,
            replicas: scs.replicas(rolegroupref),
            selector: LabelSelector {
                match_labels: Some(
                    Labels::role_group_selector(
                        scs,
                        APP_NAME,
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
fn build_service(
    shs: &v1alpha1::SparkConnectServer,
    app_version_label: &str,
    role: &str,
    group: Option<&RoleGroupRef<v1alpha1::SparkConnectServer>>,
) -> Result<Service, Error> {
    let group_name = match group {
        Some(rgr) => rgr.role_group.clone(),
        None => "global".to_owned(),
    };

    let (service_name, service_type, service_cluster_ip) = match group {
        Some(rgr) => (
            rgr.object_name(),
            "ClusterIP".to_string(),
            Some("None".to_string()),
        ),
        None => (
            format!("{}-{}", shs.metadata.name.as_ref().unwrap(), role),
            shs.spec.cluster_config.listener_class.k8s_service_type(),
            None,
        ),
    };

    let selector = match group {
        Some(rgr) => Labels::role_group_selector(shs, APP_NAME, &rgr.role, &rgr.role_group)
            .context(LabelBuildSnafu)?
            .into(),
        None => Labels::role_selector(shs, APP_NAME, role)
            .context(LabelBuildSnafu)?
            .into(),
    };

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .name(service_name)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(shs, app_version_label, &group_name))
            .context(MetadataBuildSnafu)?
            .with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?)
            .build(),
        spec: Some(ServiceSpec {
            type_: Some(service_type),
            cluster_ip: service_cluster_ip,
            ports: Some(vec![
                ServicePort {
                    name: Some(String::from("http")),
                    port: 18080,
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

// TODO: This function should be replaced with operator-rs build_rbac_resources.
// See: https://github.com/stackabletech/spark-k8s-operator/issues/499
#[allow(clippy::result_large_err)]
fn build_connect_role_serviceaccount(
    shs: &v1alpha1::SparkConnectServer,
    app_version_label: &str,
) -> Result<(ServiceAccount, RoleBinding)> {
    let sa = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(shs, app_version_label, connect_CONTROLLER_NAME))
            .context(MetadataBuildSnafu)?
            .build(),
        ..ServiceAccount::default()
    };
    let binding = RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(shs, app_version_label, connect_CONTROLLER_NAME))
            .context(MetadataBuildSnafu)?
            .build(),
        role_ref: RoleRef {
            api_group: <ClusterRole as stackable_operator::k8s_openapi::Resource>::GROUP // need to fully qualify because of "Resource" name clash
                .to_string(),
            kind: <ClusterRole as stackable_operator::k8s_openapi::Resource>::KIND.to_string(),
            name: SPARK_CLUSTER_ROLE.to_string(),
        },
        subjects: Some(vec![Subject {
            api_group: Some(
                <ServiceAccount as stackable_operator::k8s_openapi::Resource>::GROUP.to_string(),
            ),
            kind: <ServiceAccount as stackable_operator::k8s_openapi::Resource>::KIND.to_string(),
            name: sa.name_any(),
            namespace: sa.namespace(),
        }]),
    };
    Ok((sa, binding))
}

// TODO: revisit this
fn spark_defaults(
    _cs: &v1alpha1::SparkConnectServer,
    _rolegroupref: &RoleGroupRef<v1alpha1::SparkConnectServer>,
) -> Result<String, Error> {
    Ok("".to_string())
}

fn command_args(pi: &ResolvedProductImage) -> Vec<String> {
    let mut command = vec![];

    let spark_version = pi.product_version.clone();
    command.extend(vec![
        format!("containerdebug --output={VOLUME_MOUNT_PATH_LOG}/containerdebug-state.json --loop &"),
        format!("/stackable/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:{spark_version} --properties-file {VOLUME_MOUNT_PATH_CONFIG}/{SPARK_DEFAULTS_FILE_NAME}"),
    ]);
    vec![command.join("\n")]
}

fn labels<'a, T>(
    shs: &'a T,
    app_version_label: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner: shs,
        app_name: APP_NAME,
        app_version: app_version_label,
        operator_name: OPERATOR_NAME,
        controller_name: CONNECT_CONTROLLER_NAME,
        role: CONNECT_ROLE_NAME,
        role_group,
    }
}
