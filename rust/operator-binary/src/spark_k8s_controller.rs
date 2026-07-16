use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{self},
    kube::{
        Resource, ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::{
            controller::Action,
            events::{Event, EventType, Recorder},
        },
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
    let opt_s3conn = &validated.cluster_config.s3_connection;
    let logdir = &validated.cluster_config.log_dir;
    let resolved_product_image = &validated.resolved_product_image;
    // This is the final version of the spark app to reconcile.
    // No more mutating operations after this point (except for status).
    tracing::debug!("reconciling spark application [{spark_application:?}]");

    let (serviceaccount, rolebinding) =
        build::resource::serviceaccount::build_spark_role_serviceaccount(&validated)?;
    client
        .apply_patch(SPARK_CONTROLLER_NAME, &serviceaccount, &serviceaccount)
        .await
        .context(ApplyServiceAccountSnafu)?;
    client
        .apply_patch(SPARK_CONTROLLER_NAME, &rolebinding, &rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let env_vars = spark_application.env(opt_s3conn, logdir);

    let driver_config = spark_application
        .driver_config()
        .context(FailedToResolveConfigSnafu)?;

    let driver_config_overrides = spark_application
        .spec
        .driver
        .as_ref()
        .map(|driver| driver.config_overrides.clone())
        .unwrap_or_default();

    let driver_pod_template_config_map = build::resource::config_map::pod_template_config_map(
        &validated,
        SparkApplicationRole::Driver,
        &driver_config,
        &driver_config_overrides,
        &env_vars,
        &serviceaccount,
    )?;
    client
        .apply_patch(
            SPARK_CONTROLLER_NAME,
            &driver_pod_template_config_map,
            &driver_pod_template_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let executor_config = spark_application
        .executor_config()
        .context(FailedToResolveConfigSnafu)?;

    let executor_config_overrides = spark_application
        .spec
        .executor
        .as_ref()
        .map(|executor| executor.config.config_overrides.clone())
        .unwrap_or_default();

    let executor_pod_template_config_map = build::resource::config_map::pod_template_config_map(
        &validated,
        SparkApplicationRole::Executor,
        &executor_config,
        &executor_config_overrides,
        &env_vars,
        &serviceaccount,
    )?;
    client
        .apply_patch(
            SPARK_CONTROLLER_NAME,
            &executor_pod_template_config_map,
            &executor_pod_template_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    // Warn if the OpenLineage job name falls back to `metadata.name` — only matters when
    // OpenLineage is enabled.
    if spark_application.spec.open_lineage.is_some()
        && spark_application
            .resolved_openlineage_app_name()
            .context(BuildCommandSnafu)?
            .from_metadata_name
    {
        publish_openlineage_app_name_warning(&ctx.event_recorder, spark_application).await;
    }

    let job_commands = spark_application
        .build_command(opt_s3conn, logdir, &resolved_product_image.image)
        .context(BuildCommandSnafu)?;

    let submit_config = spark_application
        .submit_config()
        .context(SubmitConfigSnafu)?;

    let submit_config_overrides = spark_application
        .spec
        .job
        .as_ref()
        .map(|job| job.config_overrides.clone())
        .unwrap_or_default();

    let submit_job_config_map =
        build::resource::config_map::submit_job_config_map(&validated, &submit_config_overrides)?;
    client
        .apply_patch(
            SPARK_CONTROLLER_NAME,
            &submit_job_config_map,
            &submit_job_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job = build::resource::job::spark_job(
        &validated,
        &serviceaccount,
        &env_vars,
        &job_commands,
        &submit_config,
    )?;
    client
        .apply_patch(SPARK_CONTROLLER_NAME, &job, &job)
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

/// Emits a Kubernetes warning event that the OpenLineage job name fell back to `metadata.name`,
/// which fragments backend run history when that name is unique per run. Event-publishing failures
/// are logged, not propagated — they must not fail reconciliation.
async fn publish_openlineage_app_name_warning(
    event_recorder: &Recorder,
    spark_application: &v1alpha1::SparkApplication,
) {
    let name = spark_application.name_any();
    let publish_result = event_recorder
        .publish(
            &Event {
                type_: EventType::Warning,
                reason: "OpenLineageAppNameFallback".into(),
                note: Some(format!(
                    "OpenLineage job name falls back to metadata.name ({name:?}) because neither \
                     spec.openLineage.appName nor spark.app.name is set. If metadata.name is unique \
                     per run (e.g. an orchestrator-generated -<timestamp> suffix), backend run \
                     history will be fragmented into a new job per run. Set \
                     spec.openLineage.appName to a stable value to avoid this."
                )),
                action: "ResolveOpenLineageAppName".into(),
                secondary: None,
            },
            &spark_application.object_ref(&()),
        )
        .await;

    if let Err(error) = publish_result {
        tracing::warn!(
            ?error,
            "failed to publish OpenLineage app-name fallback warning event"
        );
    }
}
