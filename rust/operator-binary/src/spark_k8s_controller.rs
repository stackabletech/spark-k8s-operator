use std::{marker::PhantomData, sync::Arc};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::{
        batch::v1::Job,
        core::v1::{ConfigMap, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{
        Resource, ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    Ctx,
    crd::v1alpha1,
    spark_k8s_controller::{apply::Applier, update_status::update_status},
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

    #[snafu(display("failed to update the SparkApplication status"))]
    UpdateStatus { source: update_status::Error },

    #[snafu(display("failed to dereference SparkApplication"))]
    DereferenceSparkApplication { source: dereference::Error },

    #[snafu(display("failed to validate SparkApplication"))]
    ValidateSparkApplication { source: validate::Error },

    #[snafu(display("failed to build SparkApplication resources"))]
    BuildSparkApplication { source: build::Error },

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

/// Marker for prepared Kubernetes resources which are not applied yet.
pub struct Prepared;

/// Marker for applied Kubernetes resources.
pub struct Applied;

/// Every Kubernetes resource produced by the build step for a SparkApplication.
///
/// Built without a Kubernetes client: all references are already dereferenced and validated by
/// this point, so the only errors possible during assembly are resource-construction failures.
pub struct SparkResources<T> {
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
    /// Driver pod-template, executor pod-template, and submit-job ConfigMaps (in that order).
    pub config_maps: Vec<ConfigMap>,
    pub jobs: Vec<Job>,
    pub status: PhantomData<T>,
}

pub async fn reconcile(
    spark_application: Arc<DeserializeGuard<v1alpha1::SparkApplication>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    if spark_application.meta().deletion_timestamp.is_some() {
        return Ok(Action::await_change());
    }

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

    let resources = build::build(&validated).context(BuildSparkApplicationSnafu)?;

    let applied = Applier::new(client)
        .apply(resources)
        .await
        .context(ApplyResourcesSnafu)?;

    update_status(client, &validated, &applied)
        .await
        .context(UpdateStatusSnafu)?;

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

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;
    use crate::test_support::assert_reconcile_exits_early;

    /// A `SparkApplication` marked for deletion must be reconciled without any API call. The
    /// invalid spec additionally pins the early return above the [`DeserializeGuard`] unwrap.
    #[test]
    fn reconcile_exits_early_for_deleted_cluster() {
        assert_reconcile_exits_early(
            indoc! {r#"
                apiVersion: spark.stackable.tech/v1alpha1
                kind: SparkApplication
                metadata:
                  name: spark-app
                  namespace: default
                  deletionTimestamp: "2026-08-14T12:00:00Z"
                spec: {}
            "#},
            reconcile,
        );
    }
}
