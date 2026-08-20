pub mod pod;
pub mod resource;

use std::{marker::PhantomData, str::FromStr};

use resource::{config_map, job};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    constant,
    kvp::Labels,
    v2::{builder::meta::ownerreference_from_resource, kvp::label, types::operator::RoleName},
};

use crate::{
    crd::roles::SparkApplicationRole,
    spark_k8s_controller::{
        Prepared, SparkResources,
        validate::{CONTROLLER_NAME, OPERATOR_NAME, PRODUCT_NAME, ValidatedSparkApplication},
    },
};

// The `app.kubernetes.io/component` label values of the resources built by this controller. A
// SparkApplication has no Stackable roles, so these are free-form component names rather than
// role names.
constant!(pub(crate) SPARK_COMPONENT_NAME: RoleName = "spark");
constant!(pub(crate) SPARK_JOB_COMPONENT_NAME: RoleName = "spark-job");
constant!(pub(crate) SPARK_JOB_TEMPLATE_COMPONENT_NAME: RoleName = "spark-job-template");
constant!(pub(crate) POD_TEMPLATES_COMPONENT_NAME: RoleName = "pod-templates");
constant!(pub(crate) SPARK_SUBMIT_COMPONENT_NAME: RoleName = "spark-submit");

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve and merge config"))]
    FailedToResolveConfig { source: crate::crd::Error },

    #[snafu(display("failed to build the environment variables"))]
    BuildEnvVars { source: crate::crd::Error },

    #[snafu(display("failed to build stark-submit command"))]
    BuildCommand { source: crate::crd::Error },

    #[snafu(display("invalid submit config"))]
    SubmitConfig { source: crate::crd::Error },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap { source: config_map::Error },

    #[snafu(display("failed to build Job"))]
    BuildJob { source: job::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds every Kubernetes resource for the given validated SparkApplication.
pub fn build(validated: &ValidatedSparkApplication) -> Result<SparkResources<Prepared>> {
    let spark_application = &validated.spark_application;
    let opt_s3conn = &validated.cluster_config.s3_connection;
    let logdir = &validated.cluster_config.log_dir;
    let resolved_product_image = &validated.resolved_product_image;

    let (service_account, role_binding) =
        resource::serviceaccount::build_spark_role_serviceaccount(validated);

    let env_vars = spark_application
        .env(opt_s3conn, logdir)
        .context(BuildEnvVarsSnafu)?;

    let driver_config = spark_application
        .driver_config()
        .context(FailedToResolveConfigSnafu)?;

    let driver_config_overrides = spark_application
        .spec
        .driver
        .as_ref()
        .map(|driver| driver.config_overrides.clone())
        .unwrap_or_default();

    let driver_pod_template_config_map = resource::config_map::pod_template_config_map(
        validated,
        SparkApplicationRole::Driver,
        &driver_config,
        &driver_config_overrides,
        &env_vars,
        &service_account,
    )
    .context(BuildConfigMapSnafu)?;

    let executor_config = spark_application
        .executor_config()
        .context(FailedToResolveConfigSnafu)?;

    let executor_config_overrides = spark_application
        .spec
        .executor
        .as_ref()
        .map(|executor| executor.config.config_overrides.clone())
        .unwrap_or_default();

    let executor_pod_template_config_map = resource::config_map::pod_template_config_map(
        validated,
        SparkApplicationRole::Executor,
        &executor_config,
        &executor_config_overrides,
        &env_vars,
        &service_account,
    )
    .context(BuildConfigMapSnafu)?;

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
        resource::config_map::submit_job_config_map(validated, &submit_config_overrides)
            .context(BuildConfigMapSnafu)?;

    let job = resource::job::spark_job(
        validated,
        &service_account,
        &env_vars,
        &job_commands,
        &submit_config,
    )
    .context(BuildJobSnafu)?;

    Ok(SparkResources {
        service_accounts: vec![service_account],
        role_bindings: vec![role_binding],
        config_maps: vec![
            driver_pod_template_config_map,
            executor_pod_template_config_map,
            submit_job_config_map,
        ],
        jobs: vec![job],
        status: PhantomData,
    })
}

/// Object metadata for a child resource named `name`, owned by the SparkApplication and
/// carrying the recommended labels for the given component. Returns the builder so callers can
/// add extra labels before building.
pub(crate) fn object_meta(
    validated: &ValidatedSparkApplication,
    name: impl Into<String>,
    component_name: &RoleName,
) -> ObjectMetaBuilder {
    let mut builder = ObjectMetaBuilder::new();
    builder
        .namespace(validated.namespace.clone())
        .name(name)
        .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
        .with_labels(recommended_labels_for_component_resources(
            validated,
            component_name,
        ));
    builder
}

/// Recommended labels for resources shared by the whole SparkApplication, like the RBAC
/// resources.
pub(crate) fn recommended_labels_for_cluster_resources(
    validated: &ValidatedSparkApplication,
) -> Labels {
    label::recommended_labels_for_cluster_resources(
        &validated.name,
        &PRODUCT_NAME,
        &validated.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
    )
}

/// Recommended labels for resources fulfilling the given component within the SparkApplication.
///
/// A SparkApplication has no Stackable roles or role groups, so the `app.kubernetes.io/component`
/// label carries a free-form component name and there is no role group label.
pub(crate) fn recommended_labels_for_component_resources(
    validated: &ValidatedSparkApplication,
    component_name: &RoleName,
) -> Labels {
    label::recommended_labels_for_role_resources(
        &validated.name,
        &PRODUCT_NAME,
        &validated.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        component_name,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *POD_TEMPLATES_COMPONENT_NAME;
        let _ = *SPARK_COMPONENT_NAME;
        let _ = *SPARK_JOB_COMPONENT_NAME;
        let _ = *SPARK_JOB_TEMPLATE_COMPONENT_NAME;
        let _ = *SPARK_SUBMIT_COMPONENT_NAME;
    }
}
