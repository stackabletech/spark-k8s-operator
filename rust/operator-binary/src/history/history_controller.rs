use crate::history::operations::pdb::add_pdbs;
use crate::product_logging::{self, resolve_vector_aggregator_address};
use crate::Ctx;
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder},
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::product_image_selection::ResolvedProductImage,
    duration::Duration,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EnvVar, PodSecurityContext, Service, ServiceAccount, ServicePort,
                ServiceSpec,
            },
            rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    labels::{role_group_selector_labels, role_selector_labels, ObjectLabels},
    product_config::{types::PropertyNameKind, writer::to_java_properties_string},
    product_logging::{
        framework::{calculate_log_volume_size_limit, vector_container},
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
};
use stackable_spark_k8s_crd::{
    constants::{
        ACCESS_KEY_ID, APP_NAME, HISTORY_CONTROLLER_NAME, HISTORY_ROLE_NAME,
        JVM_SECURITY_PROPERTIES_FILE, LOG4J2_CONFIG_FILE, MAX_SPARK_LOG_FILES_SIZE, OPERATOR_NAME,
        SECRET_ACCESS_KEY, SPARK_CLUSTER_ROLE, SPARK_DEFAULTS_FILE_NAME, SPARK_IMAGE_BASE_NAME,
        SPARK_UID, STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE, VOLUME_MOUNT_NAME_CONFIG,
        VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_CONFIG,
        VOLUME_MOUNT_PATH_LOG, VOLUME_MOUNT_PATH_LOG_CONFIG,
    },
    history,
    history::{HistoryConfig, SparkHistoryServer, SparkHistoryServerContainer},
    s3logdir::S3LogDir,
    tlscerts,
};
use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::builder::resources::ResourceRequirementsBuilder;
use stackable_operator::k8s_openapi::DeepMerge;
use stackable_operator::logging::controller::ReconcilerError;
use strum::{EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("invalid config map {name}"))]
    InvalidConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("invalid history container name"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update the history server deployment"))]
    ApplyDeployment {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update history server config map"))]
    ApplyConfigMap {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update history server service"))]
    ApplyService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply role ServiceAccount"))]
    ApplyServiceAccount {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply global RoleBinding"))]
    ApplyRoleBinding {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("product config validation failed"))]
    ProductConfigValidation {
        source: stackable_spark_k8s_crd::history::Error,
    },
    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig {
        source: stackable_spark_k8s_crd::history::Error,
    },
    #[snafu(display("number of cleaner rolegroups exceeds 1"))]
    TooManyCleanerRoleGroups,
    #[snafu(display("number of cleaner replicas exceeds 1"))]
    TooManyCleanerReplicas,
    #[snafu(display("failed to resolve the s3 log dir confirguration"))]
    S3LogDir {
        source: stackable_spark_k8s_crd::s3logdir::Error,
    },
    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress { source: product_logging::Error },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: product_logging::Error,
        cm_name: String,
    },
    #[snafu(display("cannot retrieve role group"))]
    CannotRetrieveRoleGroup { source: history::Error },
    #[snafu(display(
        "History server : failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for group {}",
        rolegroup
    ))]
    JvmSecurityProperties {
        source: stackable_operator::product_config::writer::PropertiesWriterError,
        rolegroup: String,
    },
    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::history::operations::pdb::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}
/// Updates the status of the SparkApplication that started the pod.
pub async fn reconcile(shs: Arc<SparkHistoryServer>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile history server");

    let client = &ctx.client;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        HISTORY_CONTROLLER_NAME,
        &shs.object_ref(&()),
        ClusterResourceApplyStrategy::Default,
    )
    .context(CreateClusterResourcesSnafu)?;

    let resolved_product_image = shs
        .spec
        .image
        .resolve(SPARK_IMAGE_BASE_NAME, crate::built_info::CARGO_PKG_VERSION);
    let s3_log_dir = S3LogDir::resolve(
        Some(&shs.spec.log_file_directory),
        shs.metadata.namespace.clone(),
        client,
    )
    .await
    .context(S3LogDirSnafu)?;

    let vector_aggregator_address = resolve_vector_aggregator_address(
        client,
        shs.namespace()
            .as_deref()
            .context(ObjectHasNoNamespaceSnafu)?,
        shs.spec.vector_aggregator_config_map_name.as_deref(),
    )
    .await
    .context(ResolveVectorAggregatorAddressSnafu)?;

    // Use a dedicated service account for history server pods.
    let (serviceaccount, rolebinding) =
        build_history_role_serviceaccount(&shs, &resolved_product_image.app_version_label)?;
    let serviceaccount = cluster_resources
        .add(client, serviceaccount)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    // The role_name is always HISTORY_ROLE_NAME
    for (role_name, role_config) in shs
        .validated_role_config(&resolved_product_image, &ctx.product_config)
        .context(ProductConfigValidationSnafu)?
        .iter()
    {
        let service = build_service(
            &shs,
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
                cluster: ObjectRef::from_obj(&*shs),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let merged_config = shs
                .merged_config(&rgr)
                .context(FailedToResolveConfigSnafu)?;

            let service = build_service(
                &shs,
                &resolved_product_image.app_version_label,
                role_name,
                Some(&rgr),
            )?;
            cluster_resources
                .add(client, service)
                .await
                .context(ApplyServiceSnafu)?;

            let config_map = build_config_map(
                &shs,
                rolegroup_config,
                &merged_config,
                &resolved_product_image.app_version_label,
                &rgr,
                s3_log_dir.as_ref().unwrap(),
                vector_aggregator_address.as_deref(),
            )?;
            cluster_resources
                .add(client, config_map)
                .await
                .context(ApplyConfigMapSnafu)?;

            let sts = build_stateful_set(
                &shs,
                &resolved_product_image,
                &rgr,
                s3_log_dir.as_ref().unwrap(),
                &merged_config,
                &serviceaccount,
            )?;
            cluster_resources
                .add(client, sts)
                .await
                .context(ApplyDeploymentSnafu)?;
        }

        let role_config = &shs.spec.nodes.role_config;
        add_pdbs(
            &role_config.pod_disruption_budget,
            &shs,
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

pub fn error_policy(_obj: Arc<SparkHistoryServer>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(*Duration::from_secs(5))
}

fn build_config_map(
    shs: &SparkHistoryServer,
    config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    merged_config: &HistoryConfig,
    app_version_label: &str,
    rolegroupref: &RoleGroupRef<SparkHistoryServer>,
    s3_log_dir: &S3LogDir,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap, Error> {
    let cm_name = rolegroupref.object_name();

    let spark_defaults = spark_defaults(shs, s3_log_dir, rolegroupref)?;

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
                .with_recommended_labels(labels(shs, app_version_label, &rolegroupref.role_group))
                .build(),
        )
        .add_data(SPARK_DEFAULTS_FILE_NAME, spark_defaults)
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
        SparkHistoryServerContainer::SparkHistory,
        SparkHistoryServerContainer::Vector,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu { cm_name: &cm_name })?;

    cm_builder
        .build()
        .context(InvalidConfigMapSnafu { name: cm_name })
}

fn build_stateful_set(
    shs: &SparkHistoryServer,
    resolved_product_image: &ResolvedProductImage,
    rolegroupref: &RoleGroupRef<SparkHistoryServer>,
    s3_log_dir: &S3LogDir,
    config: &HistoryConfig,
    serviceaccount: &ServiceAccount,
) -> Result<StatefulSet, Error> {
    let log_config_map = if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = config
        .logging
        .containers
        .get(&SparkHistoryServerContainer::SparkHistory)
    {
        config_map.into()
    } else {
        rolegroupref.object_name()
    };

    let mut pb = PodBuilder::new();

    pb.service_account_name(serviceaccount.name_unchecked())
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
                .with_config_map(rolegroupref.object_name())
                .build(),
        )
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG_CONFIG)
                .with_config_map(log_config_map)
                .build(),
        )
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG)
                .with_empty_dir(
                    None::<String>,
                    Some(calculate_log_volume_size_limit(&[MAX_SPARK_LOG_FILES_SIZE])),
                )
                .build(),
        )
        .add_volumes(s3_log_dir.volumes())
        .metadata_builder(|m| {
            m.with_recommended_labels(labels(
                shs,
                &resolved_product_image.app_version_label,
                &rolegroupref.role_group,
            ))
        })
        .security_context(PodSecurityContext {
            run_as_user: Some(SPARK_UID),
            run_as_group: Some(0),
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        });

    let container_name = "spark-history";
    let container = ContainerBuilder::new(container_name)
        .context(InvalidContainerNameSnafu)?
        .image_from_product_image(resolved_product_image)
        .resources(config.resources.clone().into())
        .command(vec!["/bin/bash".to_string()])
        .args(command_args(s3_log_dir))
        .add_container_port("http", 18080)
        .add_env_vars(env_vars(s3_log_dir))
        .add_volume_mounts(s3_log_dir.volume_mounts())
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_PATH_CONFIG)
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_LOG_CONFIG)
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
        .build();
    pb.add_container(container);

    if config.logging.enable_vector_agent {
        pb.add_container(vector_container(
            resolved_product_image,
            VOLUME_MOUNT_NAME_CONFIG,
            VOLUME_MOUNT_NAME_LOG,
            config
                .logging
                .containers
                .get(&SparkHistoryServerContainer::Vector),
            ResourceRequirementsBuilder::new()
                .with_cpu_request("250m")
                .with_cpu_limit("500m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        ));
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(shs.role().config.pod_overrides.clone());
    let role_group = shs
        .rolegroup(rolegroupref)
        .with_context(|_| CannotRetrieveRoleGroupSnafu)?;

    pod_template.merge_from(role_group.config.pod_overrides);

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .name(rolegroupref.object_name())
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(
                shs,
                &resolved_product_image.app_version_label,
                rolegroupref.role_group.as_ref(),
            ))
            .build(),
        spec: Some(StatefulSetSpec {
            template: pod_template,
            replicas: shs.replicas(rolegroupref),
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    shs,
                    APP_NAME,
                    &rolegroupref.role,
                    &rolegroupref.role_group,
                )),
                ..LabelSelector::default()
            },
            ..StatefulSetSpec::default()
        }),
        ..StatefulSet::default()
    })
}

fn build_service(
    shs: &SparkHistoryServer,
    app_version_label: &str,
    role: &str,
    group: Option<&RoleGroupRef<SparkHistoryServer>>,
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
        Some(rgr) => role_group_selector_labels(shs, APP_NAME, &rgr.role, &rgr.role_group),
        None => role_selector_labels(shs, APP_NAME, role),
    };

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .name(service_name)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(shs, app_version_label, &group_name))
            .build(),
        spec: Some(ServiceSpec {
            type_: Some(service_type),
            cluster_ip: service_cluster_ip,
            ports: Some(vec![ServicePort {
                name: Some(String::from("http")),
                port: 18080,
                ..ServicePort::default()
            }]),
            selector: Some(selector),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

fn build_history_role_serviceaccount(
    shs: &SparkHistoryServer,
    app_version_label: &str,
) -> Result<(ServiceAccount, RoleBinding)> {
    let sa = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(shs, app_version_label, HISTORY_CONTROLLER_NAME))
            .build(),
        ..ServiceAccount::default()
    };
    let binding = RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(shs, app_version_label, HISTORY_CONTROLLER_NAME))
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

fn spark_defaults(
    shs: &SparkHistoryServer,
    s3_log_dir: &S3LogDir,
    rolegroupref: &RoleGroupRef<SparkHistoryServer>,
) -> Result<String, Error> {
    let mut log_dir_settings = s3_log_dir.history_server_spark_config();

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

fn command_args(s3logdir: &S3LogDir) -> Vec<String> {
    let mut command = vec![];

    if let Some(secret_dir) = s3logdir.credentials_mount_path() {
        command.extend(vec![
            format!("export AWS_ACCESS_KEY_ID=\"$(cat {secret_dir}/{ACCESS_KEY_ID})\""),
            format!("export AWS_SECRET_ACCESS_KEY=\"$(cat {secret_dir}/{SECRET_ACCESS_KEY})\""),
        ]);
    }

    if let Some(secret_name) = tlscerts::tls_secret_name(&s3logdir.bucket.connection) {
        command.extend(vec![format!("mkdir -p {STACKABLE_TRUST_STORE}")]);
        command.extend(tlscerts::convert_system_trust_store_to_pkcs12());
        command.extend(tlscerts::import_truststore(secret_name));
    }

    command.extend(vec![
        format!("/stackable/spark/sbin/start-history-server.sh --properties-file {VOLUME_MOUNT_PATH_CONFIG}/{SPARK_DEFAULTS_FILE_NAME}"),
    ]);

    vec![String::from("-c"), command.join(" && ")]
}

fn env_vars(s3logdir: &S3LogDir) -> Vec<EnvVar> {
    let mut vars: Vec<EnvVar> = vec![];

    // This env var prevents the history server from detaching itself from the
    // start script because this leads to the Pod terminating immediately.
    vars.push(EnvVar {
        name: "SPARK_NO_DAEMONIZE".to_string(),
        value: Some("true".into()),
        value_from: None,
    });
    vars.push(EnvVar {
        name: "SPARK_DAEMON_CLASSPATH".to_string(),
        value: Some("/stackable/spark/extra-jars/*".into()),
        value_from: None,
    });

    let mut history_opts = vec![
        format!("-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"),
        format!(
            "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
        ),
    ];
    if tlscerts::tls_secret_name(&s3logdir.bucket.connection).is_some() {
        history_opts.extend(
            vec![
                format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"),
                format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"),
                format!("-Djavax.net.ssl.trustStoreType=pkcs12"),
            ]
            .into_iter(),
        );
    }

    vars.push(EnvVar {
        name: "SPARK_HISTORY_OPTS".to_string(),
        value: Some(history_opts.join(" ")),
        value_from: None,
    });
    // if TLS is enabled build truststore
    vars
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
        controller_name: HISTORY_CONTROLLER_NAME,
        role: HISTORY_ROLE_NAME,
        role_group,
    }
}

/// Return the Spark properties for the cleaner role group (if any).
/// There should be only one role group with "cleaner=true" and this
/// group should have a replica count of 0 or 1.
fn cleaner_config(
    shs: &SparkHistoryServer,
    rolegroup_ref: &RoleGroupRef<SparkHistoryServer>,
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
