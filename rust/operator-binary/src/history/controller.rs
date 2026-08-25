use std::{marker::PhantomData, sync::Arc};

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
        Resource,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{Ctx, crd::history::v1alpha1, history::controller::apply::Applier};

pub mod apply;
pub mod build;
pub mod dereference;
pub mod validate;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to apply the Kubernetes resources"))]
    ApplyResources { source: apply::Error },

    #[snafu(display("failed to build SparkHistoryServer resources"))]
    BuildSparkHistoryServer { source: build::Error },

    #[snafu(display("failed to dereference SparkHistoryServer"))]
    DereferenceSparkHistoryServer { source: dereference::Error },

    #[snafu(display("failed to validate SparkHistoryServer"))]
    ValidateSparkHistoryServer { source: validate::Error },

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

/// Marker for prepared Kubernetes resources which are not applied yet.
pub struct Prepared;

/// Marker for applied Kubernetes resources.
pub struct Applied;

/// Every Kubernetes resource produced by the build step for a SparkHistoryServer.
///
/// Built without a Kubernetes client: all references are already dereferenced and validated by
/// this point, so the only errors possible during assembly are resource-construction failures.
pub struct SparkHistoryResources<T> {
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
    /// One ConfigMap, metrics Service and StatefulSet per role group.
    pub config_maps: Vec<ConfigMap>,
    pub metrics_services: Vec<Service>,
    pub stateful_sets: Vec<StatefulSet>,
    pub listeners: Vec<listener::v1alpha1::Listener>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
    pub status: PhantomData<T>,
}

pub async fn reconcile(
    shs: Arc<DeserializeGuard<v1alpha1::SparkHistoryServer>>,
    ctx: Arc<Ctx>,
) -> Result<Action, Error> {
    tracing::info!("Starting reconcile history server");

    if shs.meta().deletion_timestamp.is_some() {
        return Ok(Action::await_change());
    }

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

    let resources = build::build(&validated).context(BuildSparkHistoryServerSnafu)?;

    Applier::new(
        client,
        &validated,
        ClusterResourceApplyStrategy::Default,
        &shs.spec.object_overrides,
    )
    .apply(resources)
    .await
    .context(ApplyResourcesSnafu)?;

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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::{
        cli::OperatorEnvironmentOptions,
        client::Client,
        commons::networking::DomainName,
        kube::{Client as KubeClient, Config},
        utils::cluster_info::KubernetesClusterInfo,
    };

    use super::*;

    /// The client points at a closed port, so any API call would fail the reconciliation: an `Ok`
    /// proves that a cluster being deleted returns before the reconciler touches the Kubernetes
    /// API, and because the spec is invalid, before the [`DeserializeGuard`] is unwrapped.
    #[test]
    fn reconcile_exits_early_for_deleted_cluster() {
        let object = serde_yaml::from_str(
            r#"
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkHistoryServer
metadata:
  name: spark-history
  namespace: default
  deletionTimestamp: "2026-08-14T12:00:00Z"
spec: {}
"#,
        )
        .expect("YAML parses; the invalid spec is captured inside the DeserializeGuard");

        let action = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread tokio runtime")
            .block_on(async {
                let ctx = Arc::new(Ctx {
                    client: Client::new(
                        KubeClient::try_from(Config::new(
                            "http://127.0.0.1:1".parse().expect("valid static URI"),
                        ))
                        .expect("client from static config"),
                        None,
                        "default".to_owned(),
                        KubernetesClusterInfo {
                            cluster_domain: DomainName::from_str("cluster.local")
                                .expect("valid cluster domain"),
                        },
                    ),
                    operator_environment: OperatorEnvironmentOptions {
                        operator_namespace: "stackable-operators".to_owned(),
                        operator_service_name: "spark-k8s-operator".to_owned(),
                        image_repository: "oci.stackable.tech/sdp".to_owned(),
                    },
                });

                reconcile(Arc::new(object), ctx).await
            })
            .expect("a deleted cluster reconciles without any API call");

        assert_eq!(action, Action::await_change());
    }
}
