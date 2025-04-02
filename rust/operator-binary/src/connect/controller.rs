use std::sync::Arc;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::rbac::build_rbac_resources,
    kube::{
        core::{error_boundary, DeserializeGuard},
        runtime::controller::Action,
        Resource, ResourceExt,
    },
    logging::controller::ReconcilerError,
    status::condition::{
        compute_conditions, deployment::DeploymentConditionBuilder,
        operations::ClusterOperationsConditionBuilder,
    },
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use super::crd::{v1alpha1, CONNECT_CONTROLLER_NAME};
use crate::{
    connect::{common, crd::SparkConnectServerStatus, executor, server},
    crd::constants::{APP_NAME, OPERATOR_NAME, SPARK_IMAGE_BASE_NAME},
    product_logging::{self, resolve_vector_aggregator_address},
    Ctx,
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to serialize connect properties"))]
    SerializeProperties { source: common::Error },

    #[snafu(display("failed to build connect executor properties"))]
    ExecutorProperties { source: executor::Error },

    #[snafu(display("failed to build connect server properties"))]
    ServerProperties { source: server::Error },

    #[snafu(display("failed to build server jvm arguments"))]
    ServerJvmArgs { source: common::Error },

    #[snafu(display("failed to build spark connect service"))]
    BuildService { source: server::Error },

    #[snafu(display("failed to build spark connect executor config map for {name}"))]
    BuildExecutorConfigMap {
        source: executor::Error,
        name: String,
    },

    #[snafu(display("failed to build spark connect server config map for {name}"))]
    BuildServerConfigMap { source: server::Error, name: String },

    #[snafu(display("failed to build spark connect deployment"))]
    BuildServerDeployment { source: server::Error },

    #[snafu(display("failed to update status of spark connect server {name}"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("failed to update executor pod template"))]
    ApplyExecutorPodTemplate {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("spark connect object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to update the connect server deployment"))]
    ApplyDeployment {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update connect executor config map for {name}"))]
    ApplyExecutorConfigMap {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },

    #[snafu(display("failed to update connect server config map for {name}"))]
    ApplyServerConfigMap {
        source: stackable_operator::cluster_resources::Error,
        name: String,
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

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("SparkConnectServer object is invalid"))]
    InvalidSparkConnectServer {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },
    #[snafu(display("failed to build connect server configuration"))]
    ServerConfig { source: crate::connect::crd::Error },

    #[snafu(display("failed to build connect executor configuration"))]
    ExecutorConfig { source: crate::connect::crd::Error },

    #[snafu(display("failed to build connect executor pod template"))]
    ExecutorPodTemplate {
        source: crate::connect::executor::Error,
    },

    #[snafu(display("failed to serialize executor pod template"))]
    ExecutorPodTemplateSerde { source: serde_yaml::Error },
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

    let server_config = scs.server_config().context(ServerConfigSnafu)?;
    let executor_config = scs.executor_config().context(ExecutorConfigSnafu)?;

    let client = &ctx.client;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        CONNECT_CONTROLLER_NAME,
        &scs.object_ref(&()),
        ClusterResourceApplyStrategy::from(&scs.spec.cluster_operation),
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
    let (service_account, role_binding) = build_rbac_resources(
        scs,
        APP_NAME,
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

    // Expose connect server to the outside world
    let service = server::build_service(scs, &resolved_product_image.app_version_label, None)
        .context(BuildServiceSnafu)?;
    cluster_resources
        .add(client, service.clone())
        .await
        .context(ApplyServiceSnafu)?;

    // Headless service used by executors connect back to the driver
    let service = server::build_service(
        scs,
        &resolved_product_image.app_version_label,
        Some("None".to_string()),
    )
    .context(BuildServiceSnafu)?;

    cluster_resources
        .add(client, service.clone())
        .await
        .context(ApplyServiceSnafu)?;

    // ========================================
    // Server config map

    let spark_props = common::spark_properties(&[
        server::server_properties(
            scs,
            &server_config,
            &service,
            &service_account,
            &resolved_product_image,
        )
        .context(ServerPropertiesSnafu)?,
        executor::executor_properties(scs, &executor_config, &resolved_product_image)
            .context(ExecutorPropertiesSnafu)?,
    ])
    .context(SerializePropertiesSnafu)?;

    // ========================================
    // Executor config map and pod template
    let executor_config_map = executor::executor_config_map(
        scs,
        &executor_config,
        &resolved_product_image,
        vector_aggregator_address.as_deref(),
    )
    .context(BuildExecutorConfigMapSnafu {
        name: scs.name_unchecked(),
    })?;
    cluster_resources
        .add(client, executor_config_map.clone())
        .await
        .context(ApplyExecutorConfigMapSnafu {
            name: scs.name_unchecked(),
        })?;

    let executor_pod_template = serde_yaml::to_string(
        &executor::executor_pod_template(scs, &executor_config, &executor_config_map)
            .context(ExecutorPodTemplateSnafu)?,
    )
    .context(ExecutorPodTemplateSerdeSnafu)?;

    // ========================================
    // Server config map
    let server_config_map = server::server_config_map(
        scs,
        &server_config,
        &resolved_product_image,
        vector_aggregator_address.as_deref(),
        &spark_props,
        &executor_pod_template,
    )
    .context(BuildServerConfigMapSnafu {
        name: scs.name_unchecked(),
    })?;
    cluster_resources
        .add(client, server_config_map.clone())
        .await
        .context(ApplyServerConfigMapSnafu {
            name: scs.name_unchecked(),
        })?;

    let args = server::command_args(&resolved_product_image.product_version);
    let deployment = server::build_deployment(
        scs,
        &server_config,
        &resolved_product_image,
        &service_account,
        &server_config_map,
        args,
    )
    .context(BuildServerDeploymentSnafu)?;

    let mut ss_cond_builder = DeploymentConditionBuilder::default();

    ss_cond_builder.add(
        cluster_resources
            .add(client, deployment)
            .await
            .context(ApplyDeploymentSnafu)?,
    );

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    // ========================================
    // Spark connect server status
    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&scs.spec.cluster_operation);

    let status = SparkConnectServerStatus {
        conditions: compute_conditions(scs, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };
    client
        .apply_patch_status(OPERATOR_NAME, scs, &status)
        .await
        .context(ApplyStatusSnafu {
            name: scs.name_any(),
        })?;

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
