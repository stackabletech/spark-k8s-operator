use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder},
    k8s_openapi::api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::{ConfigMap, Pod, Service, ServicePort, ServiceSpec},
    },
    kube::runtime::controller::Action,
    labels::role_selector_labels,
};
use stackable_spark_k8s_crd::{
    constants::{APP_NAME, HISTORY_CONTROLLER_NAME, HISTORY_ROLE_NAME},
    history::SparkHistoryServer,
};
use std::sync::Arc;
use std::time::Duration;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::logging::controller::ReconcilerError;
use strum::{EnumDiscriminants, IntoStaticStr};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
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
    #[snafu(display("object is missing image"))]
    MissingImage,
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

    // TODO: (RBAC) need to use a dedicated service account, role
    let config_map = build_config_map(&shs)?;
    ctx.client
        .apply_patch(HISTORY_CONTROLLER_NAME, &config_map, &config_map)
        .await
        .context(ApplyConfigMapSnafu)?;

    let service = build_service(&shs)?;
    ctx.client
        .apply_patch(HISTORY_CONTROLLER_NAME, &service, &service)
        .await
        .context(ApplyServiceSnafu)?;

    let deployment = build_deployment(&shs)?;
    ctx.client
        .apply_patch(HISTORY_CONTROLLER_NAME, &deployment, &deployment)
        .await
        .context(ApplyDeploymentSnafu)?;

    Ok(Action::await_change())
}

pub fn error_policy(_obj: Arc<SparkHistoryServer>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

fn build_config_map(shs: &SparkHistoryServer) -> Result<ConfigMap, Error> {
    let result = ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(shs)
                .name("spark-history-config")
                .ownerreference_from_resource(shs, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(shs.deployment_labels())
                .build(),
        )
        .add_data("spark-defaults.conf", shs.config())
        .build()
        .context(InvalidConfigMapSnafu {
            name: String::from("spark-history-config"),
        })?;

    Ok(result)
}

fn build_deployment(shs: &SparkHistoryServer) -> Result<Deployment, Error> {
    let container_name = "spark-history";
    let container = ContainerBuilder::new(container_name)
        .context(InvalidContainerNameSnafu {
            name: String::from(container_name),
        })?
        .image(shs.spec.image.as_ref().context(MissingImageSnafu)?)
        // TODO: add resources
        //.resources(resources.clone().into())
        .command(vec!["/bin/bash".to_string()])
        .args(vec![
            String::from("-c"),
            String::from("/stackable/spark/sbin/start-history-server.sh"),
            String::from("--properties-file"),
            String::from("/stackable/spark/conf/spark-defaults.conf"),
        ])
        .add_container_port("http", 18080)
        .add_volume_mount("config", "/stackable/spark/conf")
        .build();

    let template = PodBuilder::new()
        .add_container(container)
        .add_volume(
            VolumeBuilder::new("config")
                .with_config_map("spark-history-config")
                .build(),
        )
        .build_template();

    Ok(Deployment {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(shs.deployment_labels())
            .build(),
        spec: Some(DeploymentSpec {
            template,
            ..DeploymentSpec::default()
        }),
        ..Deployment::default()
    })
}

fn build_service(shs: &SparkHistoryServer) -> Result<Service, Error> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .name("spark-history")
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(shs.deployment_labels())
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some(String::from("http")),
                port: 18080,
                ..ServicePort::default()
            }]),
            selector: Some(role_selector_labels(shs, APP_NAME, HISTORY_ROLE_NAME)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}
