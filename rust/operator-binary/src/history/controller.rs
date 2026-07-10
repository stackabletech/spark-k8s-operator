use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{self},
    cluster_resources::ClusterResourceApplyStrategy,
    commons::rbac::build_rbac_resources,
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
    v2::{cluster_resources::cluster_resources_new, config_file_writer::PropertiesWriterError},
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    Ctx,
    crd::{
        constants::{HISTORY_APP_NAME, JVM_SECURITY_PROPERTIES_FILE},
        history::v1alpha1,
    },
};

pub mod build;
pub mod dereference;
pub mod validate;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("invalid config map {name}"))]
    InvalidConfigMap {
        source: stackable_operator::builder::configmap::Error,
        name: String,
    },

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

    #[snafu(display("failed to dereference SparkHistoryServer"))]
    DereferenceSparkHistoryServer { source: dereference::Error },

    #[snafu(display("failed to validate SparkHistoryServer"))]
    ValidateSparkHistoryServer { source: validate::Error },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display(
        "History server : failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for group {}",
        rolegroup
    ))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("failed to create the log dir volumes specification"))]
    CreateLogDirVolumesSpec { source: crate::crd::logdir::Error },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("SparkHistoryServer object is invalid"))]
    InvalidSparkHistoryServer {
        // boxed because otherwise Clippy warns about a large enum variant
        #[snafu(source(from(error_boundary::InvalidObject, Box::new)))]
        source: Box<error_boundary::InvalidObject>,
    },

    #[snafu(display("failed to serialize Spark default properties"))]
    InvalidSparkDefaults { source: PropertiesWriterError },
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

    // Use a dedicated service account for history server pods. Building the RBAC resources needs
    // the cluster-resource labels, so it stays in the reconcile step; the built objects (whose
    // names are deterministic) are handed to the client-free build step.
    let (service_account, role_binding) = build_rbac_resources(
        shs,
        HISTORY_APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(GetRequiredLabelsSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    let resources = build::build(&validated, service_account, role_binding)?;

    // Apply order: ServiceAccount and RoleBinding first, then the ConfigMaps, metrics Services,
    // Listener and PodDisruptionBudget, and finally the StatefulSets (they mount the ConfigMaps
    // and run under the SA, so those must exist first).
    cluster_resources
        .add(client, resources.service_account)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, resources.role_binding)
        .await
        .context(ApplyRoleBindingSnafu)?;
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
