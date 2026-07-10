use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{self},
    k8s_openapi::api::{
        batch::v1::Job,
        core::v1::{ConfigMap, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{
        ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
    v2::config_file_writer::PropertiesWriterError,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    Ctx,
    crd::{constants::*, roles::SparkApplicationRole, v1alpha1},
};

pub mod build;
pub mod dereference;
pub mod validate;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to dereference SparkApplication"))]
    DereferenceSparkApplication { source: dereference::Error },

    #[snafu(display("failed to validate SparkApplication"))]
    ValidateSparkApplication { source: validate::Error },

    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to apply role ServiceAccount"))]
    ApplyServiceAccount {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to apply global RoleBinding"))]
    ApplyRoleBinding {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to apply Job"))]
    ApplyApplication {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to build stark-submit command"))]
    BuildCommand { source: crate::crd::Error },

    #[snafu(display("failed to build the pod template config map"))]
    PodTemplateConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("pod template serialization"))]
    PodTemplateSerde { source: serde_yaml::Error },

    #[snafu(display("failed to resolve and merge config"))]
    FailedToResolveConfig { source: crate::crd::Error },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

    #[snafu(display("failed to validate the logging configuration"))]
    ValidateLoggingConfig {
        source: stackable_operator::v2::product_logging::framework::Error,
    },

    #[snafu(display("failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {}", role))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        role: SparkApplicationRole,
    },

    #[snafu(display("invalid submit config"))]
    SubmitConfig { source: crate::crd::Error },

    #[snafu(display("failed to create Volumes for SparkApplication"))]
    CreateVolumes { source: crate::crd::Error },

    #[snafu(display("Failed to update status for application {name:?}"))]
    ApplySparkApplicationStatus {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("SparkApplication object is invalid"))]
    InvalidSparkApplication {
        // boxed because otherwise Clippy warns about a large enum variant
        #[snafu(source(from(error_boundary::InvalidObject, Box::new)))]
        source: Box<error_boundary::InvalidObject>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

/// Every Kubernetes resource produced by the build step for a SparkApplication.
///
/// Built without a Kubernetes client: all references are already dereferenced and validated by
/// this point, so the only errors possible during assembly are resource-construction failures.
pub struct SparkResources {
    pub service_account: ServiceAccount,
    pub role_binding: RoleBinding,
    /// Driver pod-template, executor pod-template, and submit-job ConfigMaps (in that order).
    pub config_maps: Vec<ConfigMap>,
    pub job: Job,
}

pub async fn reconcile(
    spark_application: Arc<DeserializeGuard<v1alpha1::SparkApplication>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let spark_application = spark_application
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidSparkApplicationSnafu)?;

    let client = &ctx.client;

    if spark_application.k8s_job_has_been_created() {
        tracing::info!(
            spark_application = spark_application.name_any(),
            "Skipped reconciling SparkApplication with non empty status"
        );
        return Ok(Action::await_change());
    }

    // It is important to do this at the top of the reconciliation function to ensure
    // all referenced resources and configuration are merged before any of them are created.
    let dereferenced = dereference::dereference(client, spark_application)
        .await
        .context(DereferenceSparkApplicationSnafu)?;

    let validated = validate::validate(dereferenced, &ctx.operator_environment)
        .context(ValidateSparkApplicationSnafu)?;

    let spark_application = &validated.spark_application;
    // This is the final version of the spark app to reconcile.
    // No more mutating operations after this point (except for status).
    tracing::debug!("reconciling spark application [{spark_application:?}]");

    let resources = build::build(&validated)?;

    // Apply the ServiceAccount and RoleBinding first, then the ConfigMaps, and finally the Job:
    // the Job runs under the ServiceAccount and mounts the ConfigMaps, so they must exist first.
    client
        .apply_patch(
            SPARK_CONTROLLER_NAME,
            &resources.service_account,
            &resources.service_account,
        )
        .await
        .context(ApplyServiceAccountSnafu)?;
    client
        .apply_patch(
            SPARK_CONTROLLER_NAME,
            &resources.role_binding,
            &resources.role_binding,
        )
        .await
        .context(ApplyRoleBindingSnafu)?;
    for config_map in &resources.config_maps {
        client
            .apply_patch(SPARK_CONTROLLER_NAME, config_map, config_map)
            .await
            .context(ApplyApplicationSnafu)?;
    }
    client
        .apply_patch(SPARK_CONTROLLER_NAME, &resources.job, &resources.job)
        .await
        .context(ApplyApplicationSnafu)?;

    // Fix for #457
    // Update the status of the SparkApplication immediately after creating the Job
    // to ensure the Job is not created again after being recycled by Kubernetes.
    client
        .apply_patch_status(
            SPARK_CONTROLLER_NAME,
            spark_application,
            &v1alpha1::SparkApplicationStatus {
                phase: "Unknown".to_string(),
                resolved_template_ref: validated.cluster_config.resolved_template_refs.clone(),
            },
        )
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: spark_application.name_any(),
        })?;

    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::SparkApplication>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidSparkApplication { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}
