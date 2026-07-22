pub mod pod;
pub mod resource;

use resource::{config_map, job};
use snafu::{ResultExt, Snafu};

use crate::{
    crd::roles::SparkApplicationRole,
    spark_k8s_controller::{SparkResources, validate::ValidatedSparkApplication},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve and merge config"))]
    FailedToResolveConfig { source: crate::crd::Error },

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
pub fn build(validated: &ValidatedSparkApplication) -> Result<SparkResources> {
    let spark_application = &validated.spark_application;
    let opt_s3conn = &validated.cluster_config.s3_connection;
    let opt_open_lineage_conn = validated.cluster_config.open_lineage_connection.as_ref();
    let logdir = &validated.cluster_config.log_dir;
    let resolved_product_image = &validated.resolved_product_image;

    let (service_account, role_binding) =
        resource::serviceaccount::build_spark_role_serviceaccount(validated);

    let env_vars = spark_application.env(opt_s3conn, logdir, opt_open_lineage_conn);

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
        .build_command(
            opt_s3conn,
            logdir,
            &resolved_product_image.image,
            &resolved_product_image.product_version,
            opt_open_lineage_conn,
        )
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
        service_account,
        role_binding,
        config_maps: vec![
            driver_pod_template_config_map,
            executor_pod_template_config_map,
            submit_job_config_map,
        ],
        job,
    })
}
