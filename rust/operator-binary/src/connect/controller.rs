use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cluster_resources::ClusterResourceApplyStrategy,
    commons::rbac::build_rbac_resources,
    crd::listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{
        ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::cluster_resources::cluster_resources_new,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use super::crd::{CONNECT_APP_NAME, v1alpha1};
use crate::{Ctx, connect::crd::SparkConnectServerStatus, crd::constants::OPERATOR_NAME};

pub mod build;
pub mod dereference;
pub mod validate;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to build the Kubernetes resources"))]
    BuildResources { source: build::Error },

    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
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

    #[snafu(display("failed to update status of spark connect server {name}"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
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

    #[snafu(display("failed to dereference SparkConnectServer"))]
    DereferenceSparkConnectServer { source: dereference::Error },

    #[snafu(display("failed to validate SparkConnectServer"))]
    ValidateSparkConnectServer { source: validate::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

/// Every Kubernetes resource produced by the build step for a SparkConnectServer.
///
/// Built without a Kubernetes client: all references are already dereferenced and validated by
/// this point, so the only errors possible during assembly are resource-construction failures.
pub struct SparkConnectResources {
    pub service_account: ServiceAccount,
    pub role_binding: RoleBinding,
    /// The headless Service (for executors to reach the driver) and the metrics Service.
    pub services: Vec<Service>,
    /// The executor and server ConfigMaps.
    pub config_maps: Vec<ConfigMap>,
    pub listener: listener::v1alpha1::Listener,
    pub stateful_set: StatefulSet,
}

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

    let dereferenced = dereference::dereference(client, scs)
        .await
        .context(DereferenceSparkConnectServerSnafu)?;

    let validated = validate::validate(scs, dereferenced, &ctx.operator_environment)
        .context(ValidateSparkConnectServerSnafu)?;

    tracing::debug!(
        name = %validated.name,
        namespace = %validated.namespace,
        uid = %validated.uid,
        "Validated SparkConnectServer identity"
    );

    let mut cluster_resources = cluster_resources_new(
        &validate::product_name(),
        &validate::operator_name(),
        &validate::controller_name(),
        &validated.name,
        &validated.namespace,
        &validated.uid,
        ClusterResourceApplyStrategy::from(&scs.spec.cluster_operation),
        &scs.spec.object_overrides,
    );

    // Use a dedicated service account for connect server pods. Building the RBAC resources needs
    // the cluster-resource labels, so it stays in the reconcile step; the built objects (whose
    // names are deterministic) are handed to the client-free build step.
    let (service_account, role_binding) = build_rbac_resources(
        scs,
        CONNECT_APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(GetRequiredLabelsSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    let resources = build::build(&validated, service_account, role_binding, &scs.spec.args)
        .context(BuildResourcesSnafu)?;

    // Apply order: ServiceAccount and RoleBinding first, then the Services, ConfigMaps and
    // Listener, and finally the StatefulSet (it mounts the ConfigMaps and runs under the SA, so
    // they must exist first).
    cluster_resources
        .add(client, resources.service_account)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, resources.role_binding)
        .await
        .context(ApplyRoleBindingSnafu)?;
    for service in resources.services {
        cluster_resources
            .add(client, service)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for config_map in resources.config_maps {
        cluster_resources
            .add(client, config_map)
            .await
            .context(ApplyResourceSnafu)?;
    }
    cluster_resources
        .add(client, resources.listener)
        .await
        .context(ApplyResourceSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    ss_cond_builder.add(
        cluster_resources
            .add(client, resources.stateful_set)
            .await
            .context(ApplyResourceSnafu)?,
    );

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    // ========================================
    // Spark connect server status
    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&scs.spec.cluster_operation);

    // TODO: This StatefulSet only contains the driver. We should probably also
    // consider the state of the executors to determine if the
    // SparkConnectServer is ready. This depends on the availability and
    // resilience properties of Spark and could e.g. be "driver and more than
    // 75% of the executors ready". Special care needs to be taken about
    // auto-scaling executors in this case (if/once supported).
    let status = SparkConnectServerStatus {
        conditions: compute_conditions(scs, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };
    client
        .apply_patch_status(OPERATOR_NAME, scs, &status)
        .await
        .context(ApplyStatusSnafu {
            name: validated.name_any(),
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
