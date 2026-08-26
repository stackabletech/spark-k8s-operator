use std::{marker::PhantomData, sync::Arc};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cluster_resources::ClusterResourceApplyStrategy,
    crd::listener::v1alpha1::Listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{
        Resource,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use super::crd::v1alpha1;
use crate::{
    Ctx,
    connect::controller::{apply::Applier, update_status::update_status},
};

pub mod apply;
pub mod build;
pub mod dereference;
pub mod update_status;
pub mod validate;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to apply the Kubernetes resources"))]
    ApplyResources { source: apply::Error },

    #[snafu(display("failed to update the cluster status"))]
    UpdateStatus { source: update_status::Error },

    #[snafu(display("failed to build the Kubernetes resources"))]
    BuildResources { source: build::Error },

    #[snafu(display("SparkConnectServer object is invalid"))]
    InvalidSparkConnectServer {
        source: error_boundary::InvalidObject,
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

/// Marker for prepared Kubernetes resources which are not applied yet.
pub struct Prepared;

/// Marker for applied Kubernetes resources.
pub struct Applied;

/// Every Kubernetes resource produced by the build step for a SparkConnectServer.
///
/// Built without a Kubernetes client: all references are already dereferenced and validated by
/// this point, so the only errors possible during assembly are resource-construction failures.
pub struct SparkConnectResources<T> {
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
    /// The headless Service (for executors to reach the driver) and the metrics Service.
    pub services: Vec<Service>,
    /// The executor and server ConfigMaps.
    pub config_maps: Vec<ConfigMap>,
    pub listeners: Vec<Listener>,
    pub stateful_sets: Vec<StatefulSet>,
    pub status: PhantomData<T>,
}

pub async fn reconcile(
    scs: Arc<DeserializeGuard<v1alpha1::SparkConnectServer>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile connect server");

    if scs.meta().deletion_timestamp.is_some() {
        return Ok(Action::await_change());
    }

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

    let resources = build::build(&validated, &scs.spec.args).context(BuildResourcesSnafu)?;

    let applier = Applier::new(
        client,
        &validated,
        ClusterResourceApplyStrategy::from(&scs.spec.cluster_operation),
        &scs.spec.object_overrides,
    );

    let applied = applier
        .apply(resources)
        .await
        .context(ApplyResourcesSnafu)?;

    update_status(client, scs, &applied)
        .await
        .context(UpdateStatusSnafu)?;

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

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;
    use crate::test_support::assert_reconcile_exits_early;

    /// A `SparkConnectServer` marked for deletion must be reconciled without any API call. The
    /// invalid spec additionally pins the early return above the [`DeserializeGuard`] unwrap.
    #[test]
    fn reconcile_exits_early_for_deleted_cluster() {
        assert_reconcile_exits_early(
            indoc! {r#"
                apiVersion: spark.stackable.tech/v1alpha1
                kind: SparkConnectServer
                metadata:
                  name: spark-connect
                  namespace: default
                  deletionTimestamp: "2026-08-14T12:00:00Z"
                spec: {}
            "#},
            reconcile,
        );
    }
}
