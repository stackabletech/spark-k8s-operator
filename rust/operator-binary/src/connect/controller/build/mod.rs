//! Builders that turn a `ValidatedSparkConnectServer` into Kubernetes resources.
//!
//! These are grouped by role (`server`, `executor`) rather than by resource kind: each Spark
//! Connect role bundles a cohesive set of builders — its ConfigMap, StatefulSet/pod template,
//! Spark properties, environment variables and JVM arguments — so keeping a role's builders
//! together in one module is clearer than scattering them across per-kind modules.

pub(crate) mod executor;
pub(crate) mod server;
pub(crate) mod service;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::{core::v1::ServiceAccount, rbac::v1::RoleBinding},
    kube::ResourceExt,
};

use crate::connect::{
    common,
    controller::{SparkConnectResources, validate::ValidatedSparkConnectServer},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build connect server S3 properties"))]
    S3SparkProperties { source: crate::connect::s3::Error },

    #[snafu(display("failed to build connect server properties"))]
    ServerProperties { source: server::Error },

    #[snafu(display("failed to build connect executor properties"))]
    ExecutorProperties { source: executor::Error },

    #[snafu(display("failed to serialize connect properties"))]
    SerializeProperties { source: common::Error },

    #[snafu(display("failed to build spark connect executor config map for {name}"))]
    BuildExecutorConfigMap {
        source: executor::Error,
        name: String,
    },

    #[snafu(display("failed to build connect executor pod template"))]
    ExecutorPodTemplate { source: executor::Error },

    #[snafu(display("failed to serialize executor pod template"))]
    ExecutorPodTemplateSerde { source: serde_yaml::Error },

    #[snafu(display("failed to build spark connect server config map for {name}"))]
    BuildServerConfigMap { source: server::Error, name: String },

    #[snafu(display("failed to build spark connect stateful set"))]
    BuildServerStatefulSet { source: server::Error },
}

/// Builds every Kubernetes resource for the given validated SparkConnectServer.
pub(crate) fn build(
    validated: &ValidatedSparkConnectServer,
    service_account: ServiceAccount,
    role_binding: RoleBinding,
    user_args: &[String],
) -> Result<SparkConnectResources, Error> {
    let resolved_s3 = &validated.cluster_config.resolved_s3;

    // Headless service used by executors to connect back to the driver, plus the metrics service.
    let headless_service = service::build_headless_service(validated);
    let metrics_service = service::build_metrics_service(validated);

    let spark_props = common::spark_properties(&[
        resolved_s3
            .spark_properties()
            .context(S3SparkPropertiesSnafu)?,
        server::server_properties(validated, &headless_service, &service_account)
            .context(ServerPropertiesSnafu)?,
        executor::executor_properties(validated).context(ExecutorPropertiesSnafu)?,
    ])
    .context(SerializePropertiesSnafu)?;

    let executor_config_map =
        executor::executor_config_map(validated).context(BuildExecutorConfigMapSnafu {
            name: validated.name_any(),
        })?;

    let executor_pod_template = serde_yaml::to_string(
        &executor::executor_pod_template(validated, &executor_config_map)
            .context(ExecutorPodTemplateSnafu)?,
    )
    .context(ExecutorPodTemplateSerdeSnafu)?;

    let server_config_map =
        server::server_config_map(validated, &spark_props, &executor_pod_template).context(
            BuildServerConfigMapSnafu {
                name: validated.name_any(),
            },
        )?;

    let listener = server::build_listener(validated);

    let args = server::command_args(user_args);
    let stateful_set = server::build_stateful_set(
        validated,
        &service_account,
        &server_config_map,
        &listener.name_any(),
        args,
    )
    .context(BuildServerStatefulSetSnafu)?;

    Ok(SparkConnectResources {
        service_account,
        role_binding,
        services: vec![headless_service, metrics_service],
        config_maps: vec![executor_config_map, server_config_map],
        listener,
        stateful_set,
    })
}
