use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMap, Service, ServicePort, ServiceSpec},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::runtime::{controller::Action, reflector::ObjectRef},
    labels::{role_group_selector_labels, role_selector_labels},
    product_config::{types::PropertyNameKind, ProductConfigManager},
    role_utils::RoleGroupRef,
};
use stackable_spark_k8s_crd::{
    constants::{
        APP_NAME, HISTORY_CONFIG_FILE_NAME, HISTORY_CONTROLLER_NAME, HISTORY_IMAGE_BASE_NAME,
    },
    history::SparkHistoryServer,
};
use std::{collections::BTreeMap, sync::Arc};
use std::{collections::HashMap, time::Duration};

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
    #[snafu(display("product config validation failed"))]
    ProductConfigValidation {
        source: stackable_spark_k8s_crd::history::Error,
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

    let resolved_product_image = shs.spec.image.resolve(HISTORY_IMAGE_BASE_NAME);

    // TODO: (RBAC) need to use a dedicated service account, role

    // The role_name is always "history"
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
        ctx.client
            .apply_patch(HISTORY_CONTROLLER_NAME, &service, &service)
            .await
            .context(ApplyServiceSnafu)?;

        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rgr = RoleGroupRef {
                cluster: ObjectRef::from_obj(&*shs),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let service = build_service(
                &shs,
                &resolved_product_image.app_version_label,
                role_name,
                Some(&rgr),
            )?;
            ctx.client
                .apply_patch(HISTORY_CONTROLLER_NAME, &service, &service)
                .await
                .context(ApplyServiceSnafu)?;

            let config_map = build_config_map(
                &shs,
                &resolved_product_image.app_version_label,
                &rgr,
                rolegroup_config,
            )?;
            ctx.client
                .apply_patch(HISTORY_CONTROLLER_NAME, &config_map, &config_map)
                .await
                .context(ApplyConfigMapSnafu)?;

            let deployment = build_stateful_set(&shs, &resolved_product_image, &rgr)?;
            ctx.client
                .apply_patch(HISTORY_CONTROLLER_NAME, &deployment, &deployment)
                .await
                .context(ApplyDeploymentSnafu)?;
        }
    }

    Ok(Action::await_change())
}

pub fn error_policy(_obj: Arc<SparkHistoryServer>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

fn build_config_map(
    shs: &SparkHistoryServer,
    app_version_label: &str,
    rolegroupref: &RoleGroupRef<SparkHistoryServer>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<ConfigMap, Error> {
    let empty_map = BTreeMap::new();

    let spark_defaults_data = rolegroup_config
        .get(&PropertyNameKind::File(HISTORY_CONFIG_FILE_NAME.to_owned()))
        .unwrap_or(&empty_map)
        .iter()
        .map(|(k, v)| format!("{k} {v}"))
        .collect::<Vec<String>>()
        .join("\n");

    let result = ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(shs)
                .name(rolegroupref.object_name())
                .ownerreference_from_resource(shs, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(shs.labels(app_version_label, &rolegroupref.role_group))
                .build(),
        )
        .add_data(HISTORY_CONFIG_FILE_NAME, spark_defaults_data)
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
) -> Result<StatefulSet, Error> {
    let container_name = "spark-history";
    let container = ContainerBuilder::new(container_name)
        .context(InvalidContainerNameSnafu {
            name: String::from(container_name),
        })?
        .image(resolved_product_image.image.clone())
        // TODO: add resources
        //.resources(resources.clone().into())
        .command(vec!["/bin/bash".to_string()])
        .args(shs.command_args())
        .add_container_port("http", 18080)
        .add_volume_mount("config", "/stackable/spark/conf")
        // This env var prevents the history server from detaching it's self from the
        // start script because this leads to the Pod terminating immediately.
        .add_env_var("SPARK_NO_DAEMONIZE", "true")
        .build();

    let template = PodBuilder::new()
        .add_container(container)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(
            VolumeBuilder::new("config")
                .with_config_map(rolegroupref.object_name())
                .build(),
        )
        .metadata_builder(|m| {
            m.with_recommended_labels(shs.labels(
                &resolved_product_image.app_version_label,
                &rolegroupref.role_group,
            ))
        })
        .build_template();

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(shs.labels(
                &resolved_product_image.app_version_label,
                rolegroupref.role_group.as_ref(),
            ))
            .build(),
        spec: Some(StatefulSetSpec {
            template,
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
            .with_recommended_labels(shs.labels(app_version_label, &group_name))
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
