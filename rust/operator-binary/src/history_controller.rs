use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder},
    cluster_resources::ClusterResources,
    commons::{
        product_image_selection::ResolvedProductImage,
        resources::{NoRuntimeLimits, Resources},
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, PodSecurityContext, Service, ServiceAccount, ServicePort, ServiceSpec,
            },
            rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        Resource,
    },
    labels::{role_group_selector_labels, role_selector_labels, ObjectLabels},
    product_config::ProductConfigManager,
    role_utils::RoleGroupRef,
};
use stackable_spark_k8s_crd::{
    constants::*,
    history::{HistoryStorageConfig, SparkHistoryServer},
    s3logdir::S3LogDir,
};
use std::time::Duration;
use std::{collections::BTreeMap, sync::Arc};

use snafu::{ResultExt, Snafu};
use stackable_operator::logging::controller::ReconcilerError;
use strum::{EnumDiscriminants, IntoStaticStr};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("invalid config map {name}"))]
    InvalidConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("invalid history container name {name}"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
        name: String,
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
    )
    .context(CreateClusterResourcesSnafu)?;

    let resolved_product_image = shs.spec.image.resolve(HISTORY_IMAGE_BASE_NAME);
    let s3_log_dir = S3LogDir::resolve(
        Some(&shs.spec.log_file_directory),
        shs.metadata.namespace.clone(),
        client,
    )
    .await
    .context(S3LogDirSnafu)?;

    // Use a dedicated service account for history pods.
    let (serviceaccount, rolebinding) =
        build_history_role_serviceaccount(&shs, &resolved_product_image.app_version_label)?;
    cluster_resources
        .add(client, &serviceaccount)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, &rolebinding)
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
            .add(client, &service)
            .await
            .context(ApplyServiceSnafu)?;

        for (rolegroup_name, _rolegroup_config) in role_config.iter() {
            let rgr = RoleGroupRef {
                cluster: ObjectRef::from_obj(&*shs),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let config = shs
                .merged_config(&rgr)
                .context(FailedToResolveConfigSnafu)?;

            let service = build_service(
                &shs,
                &resolved_product_image.app_version_label,
                role_name,
                Some(&rgr),
            )?;
            cluster_resources
                .add(client, &service)
                .await
                .context(ApplyServiceSnafu)?;

            let config_map = build_config_map(
                &shs,
                &resolved_product_image.app_version_label,
                &rgr,
                s3_log_dir.as_ref().unwrap(),
            )?;
            cluster_resources
                .add(client, &config_map)
                .await
                .context(ApplyConfigMapSnafu)?;

            let sts = build_stateful_set(
                &shs,
                &resolved_product_image,
                &rgr,
                s3_log_dir.as_ref().unwrap(),
                &config.resources,
            )?;
            cluster_resources
                .add(client, &sts)
                .await
                .context(ApplyDeploymentSnafu)?;
        }
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    Ok(Action::await_change())
}

pub fn error_policy(_obj: Arc<SparkHistoryServer>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

fn build_config_map(
    shs: &SparkHistoryServer,
    app_version_label: &str,
    rolegroupref: &RoleGroupRef<SparkHistoryServer>,
    s3_log_dir: &S3LogDir,
) -> Result<ConfigMap, Error> {
    let spark_config = spark_config(shs, s3_log_dir, rolegroupref)?;

    let result = ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(shs)
                .name(rolegroupref.object_name())
                .ownerreference_from_resource(shs, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(labels(shs, app_version_label, &rolegroupref.role_group))
                .build(),
        )
        .add_data(HISTORY_CONFIG_FILE_NAME, spark_config)
        .build()
        .context(InvalidConfigMapSnafu {
            name: String::from("spark-history-config"),
        })?;

    Ok(result)
}

fn build_stateful_set(
    shs: &SparkHistoryServer,
    resolved_product_image: &ResolvedProductImage,
    rolegroupref: &RoleGroupRef<SparkHistoryServer>,
    s3_log_dir: &S3LogDir,
    resources: &Resources<HistoryStorageConfig, NoRuntimeLimits>,
) -> Result<StatefulSet, Error> {
    let container_name = "spark-history";
    let container = ContainerBuilder::new(container_name)
        .context(InvalidContainerNameSnafu {
            name: String::from(container_name),
        })?
        .image_from_product_image(&resolved_product_image)
        .resources(resources.clone().into())
        .command(vec!["/bin/bash".to_string()])
        .args(command_args(s3_log_dir))
        .add_container_port("http", 18080)
        // This env var prevents the history server from detaching itself from the
        // start script because this leads to the Pod terminating immediately.
        .add_env_var("SPARK_NO_DAEMONIZE", "true")
        .add_volume_mounts(s3_log_dir.credentials_volume_mount().into_iter())
        .add_volume_mount("config", "/stackable/spark/conf")
        .build();

    let template = PodBuilder::new()
        .add_container(container)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(
            VolumeBuilder::new("config")
                .with_config_map(rolegroupref.object_name())
                .build(),
        )
        .add_volumes(s3_log_dir.credentials_volume().into_iter().collect())
        .metadata_builder(|m| {
            m.with_recommended_labels(labels(
                shs,
                &resolved_product_image.app_version_label,
                &rolegroupref.role_group,
            ))
        })
        .security_context(PodSecurityContext {
            run_as_user: Some(1000),
            run_as_group: Some(1000),
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        })
        .build_template();

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(
                shs,
                &resolved_product_image.app_version_label,
                rolegroupref.role_group.as_ref(),
            ))
            .build(),
        spec: Some(StatefulSetSpec {
            template,
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

    let service_name = match group {
        Some(rgr) => rgr.object_name(),
        None => format!(
            "{}-{}",
            shs.metadata.name.as_ref().unwrap_or(&APP_NAME.to_string()),
            role
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
            ports: Some(vec![ServicePort {
                name: Some(String::from("http")),
                port: 18080,
                ..ServicePort::default()
            }]),
            selector: Some(selector),
            type_: Some("NodePort".to_string()),
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

fn spark_config(
    shs: &SparkHistoryServer,
    s3_log_dir: &S3LogDir,
    rolegroupref: &RoleGroupRef<SparkHistoryServer>,
) -> Result<String, Error> {
    let empty = BTreeMap::new();

    let mut log_dir_settings = s3_log_dir.history_server_spark_config();

    // add cleaner spark settings if requested
    log_dir_settings.extend(cleaner_config(shs, rolegroupref)?);

    // add user provided configuration. These can overwrite everything.
    log_dir_settings.extend(shs.spec.spark_conf.clone().unwrap_or_default());

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
            format!("export AWS_ACCESS_KEY_ID=$(cat {secret_dir}/{ACCESS_KEY_ID})"),
            "&&".to_string(),
            format!("export AWS_SECRET_ACCESS_KEY=$(cat {secret_dir}/{SECRET_ACCESS_KEY})"),
            "&&".to_string(),
        ]);
    }
    command.extend(vec![
        "/stackable/spark/sbin/start-history-server.sh".to_string(),
        "--properties-file".to_string(),
        HISTORY_CONFIG_FILE_NAME_FULL.to_string(),
    ]);

    vec![String::from("-c"), command.join(" ")]
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
