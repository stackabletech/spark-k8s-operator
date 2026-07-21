use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cluster_resources::ClusterResourceApplyStrategy,
    crd::listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        policy::v1::PodDisruptionBudget,
        rbac::v1::RoleBinding,
    },
    kube::{
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
    v2::cluster_resources::cluster_resources_new,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{Ctx, crd::history::v1alpha1};

pub mod build;
pub mod dereference;
pub mod validate;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to build SparkHistoryServer resources"))]
    BuildSparkHistoryServer { source: build::Error },

    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
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

    #[snafu(display("SparkHistoryServer object is invalid"))]
    InvalidSparkHistoryServer {
        // boxed because otherwise Clippy warns about a large enum variant
        #[snafu(source(from(error_boundary::InvalidObject, Box::new)))]
        source: Box<error_boundary::InvalidObject>,
    },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

/// Every Kubernetes resource produced by the build step for a SparkHistoryServer.
///
/// Built without a Kubernetes client: all references are already dereferenced and validated by
/// this point, so the only errors possible during assembly are resource-construction failures.
pub struct SparkHistoryResources {
    pub service_account: ServiceAccount,
    pub role_binding: RoleBinding,
    /// One ConfigMap, metrics Service and StatefulSet per role group.
    pub config_maps: Vec<ConfigMap>,
    pub metrics_services: Vec<Service>,
    pub stateful_sets: Vec<StatefulSet>,
    pub listener: listener::v1alpha1::Listener,
    pub pod_disruption_budget: Option<PodDisruptionBudget>,
}

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

    let resources = build::build(&validated).context(BuildSparkHistoryServerSnafu)?;

    // Apply order: ServiceAccount and RoleBinding first, then the ConfigMaps, metrics Services,
    // Listener and PodDisruptionBudget, and finally the StatefulSets (they mount the ConfigMaps
    // and run under the SA, so those must exist first).
    cluster_resources
        .add(client, resources.service_account)
        .await
        .context(ApplyResourceSnafu)?;
    cluster_resources
        .add(client, resources.role_binding)
        .await
        .context(ApplyResourceSnafu)?;
    for config_map in resources.config_maps {
        cluster_resources
            .add(client, config_map)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for metrics_service in resources.metrics_services {
        cluster_resources
            .add(client, metrics_service)
            .await
            .context(ApplyResourceSnafu)?;
    }
    cluster_resources
        .add(client, resources.listener)
        .await
        .context(ApplyResourceSnafu)?;
    if let Some(pdb) = resources.pod_disruption_budget {
        cluster_resources
            .add(client, pdb)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for stateful_set in resources.stateful_sets {
        cluster_resources
            .add(client, stateful_set)
            .await
            .context(ApplyResourceSnafu)?;
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    Ok(Action::await_change())
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
