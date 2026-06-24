use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{self},
    cluster_resources::ClusterResourceApplyStrategy,
    commons::rbac::build_rbac_resources,
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
        constants::{HISTORY_APP_NAME, HISTORY_ROLE_NAME, JVM_SECURITY_PROPERTIES_FILE},
        history::v1alpha1,
    },
    history::controller::build::resource::{
        config_map::build_config_map, listener::build_group_listener, pdb::build_pdb,
        service::build_rolegroup_metrics_service, statefulset::build_stateful_set,
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

    #[snafu(display("failed to update the history server stateful set"))]
    ApplyStatefulSet {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update history server config map"))]
    ApplyConfigMap {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update history server metrics service"))]
    ApplyMetricsService {
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

    #[snafu(display("failed to apply PodDisruptionBudget"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
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

    #[snafu(display("failed to apply group listener"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to serialize Spark default properties"))]
    InvalidSparkDefaults { source: PropertiesWriterError },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}
/// Updates the status of the SparkApplication that started the pod.
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

    let log_dir = &validated.cluster_config.log_dir;

    // Use a dedicated service account for history server pods.
    let (service_account, role_binding) = build_rbac_resources(
        shs,
        HISTORY_APP_NAME,
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

    for (role_group_name, rg) in &validated.role_groups {
        let config_map = build_config_map(&validated, role_group_name, rg)?;

        let metrics_service = build_rolegroup_metrics_service(&validated, role_group_name);

        let sts = build_stateful_set(&validated, role_group_name, rg, log_dir, &service_account)?;

        cluster_resources
            .add(client, config_map)
            .await
            .context(ApplyConfigMapSnafu)?;
        cluster_resources
            .add(client, metrics_service)
            .await
            .context(ApplyMetricsServiceSnafu)?;
        cluster_resources
            .add(client, sts)
            .await
            .context(ApplyStatefulSetSnafu)?;
    }

    let rg_group_listener = build_group_listener(
        &validated,
        HISTORY_ROLE_NAME,
        validated.role_config.listener_class.clone(),
    );

    cluster_resources
        .add(client, rg_group_listener)
        .await
        .context(ApplyGroupListenerSnafu)?;

    if let Some(pdb) = build_pdb(&validated.role_config.pdb, &validated) {
        cluster_resources
            .add(client, pdb)
            .await
            .context(ApplyPdbSnafu)?;
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
