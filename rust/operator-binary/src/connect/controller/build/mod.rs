//! Builders that turn a `ValidatedSparkConnectServer` into Kubernetes resources.
//!
//! These are grouped by role (`server`, `executor`) rather than by resource kind: each Spark
//! Connect role bundles a cohesive set of builders — its ConfigMap, StatefulSet/pod template,
//! Spark properties, environment variables and JVM arguments — so keeping a role's builders
//! together in one module is clearer than scattering them across per-kind modules.

pub(crate) mod executor;
pub(crate) mod rbac;
pub(crate) mod server;
pub(crate) mod service;

use snafu::{ResultExt, Snafu};
use stackable_operator::kube::ResourceExt;

use crate::connect::{
    common,
    controller::{
        SparkConnectResources,
        build::rbac::{build_role_binding, build_service_account},
        validate::ValidatedSparkConnectServer,
    },
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
        server::server_properties(validated, &headless_service).context(ServerPropertiesSnafu)?,
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
    let stateful_set =
        server::build_stateful_set(validated, &server_config_map, &listener.name_any(), args)
            .context(BuildServerStatefulSetSnafu)?;

    Ok(SparkConnectResources {
        service_account: build_service_account(validated),
        role_binding: build_role_binding(validated),
        services: vec![headless_service, metrics_service],
        config_maps: vec![executor_config_map, server_config_map],
        listener,
        stateful_set,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use indoc::indoc;
    use stackable_operator::{cli::OperatorEnvironmentOptions, utils::yaml_from_str_singleton_map};

    use super::build;
    use crate::connect::{
        controller::{
            dereference::DereferencedSparkConnectServer,
            validate::{ValidatedSparkConnectServer, validate},
        },
        crd::v1alpha1,
        s3::ResolvedS3,
    };

    /// Minimal S3-free `SparkConnectServer` fixture, keeping the dereference step client-free;
    /// the `uid` allows owner references to be derived from it.
    const CONNECT_YAML: &str = indoc! {r#"
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkConnectServer
        metadata:
          name: my-connect
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 4.1.2
        "#};

    /// Runs the real validate step against the minimal fixture.
    fn minimal_validated_cluster() -> ValidatedSparkConnectServer {
        let scs: v1alpha1::SparkConnectServer = yaml_from_str_singleton_map(CONNECT_YAML)
            .expect("invalid test SparkConnectServer YAML");
        validate(
            &scs,
            DereferencedSparkConnectServer {
                resolved_s3: ResolvedS3::none(),
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "stackable-operators".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.example.org/sdp".to_string(),
            },
        )
        .expect("validate should succeed for the test fixture")
    }

    /// Locks the RBAC resource names, the roleRef, and the recommended label set against
    /// accidental drift. The fixture's cluster name deliberately differs from the product name so
    /// that swapped `name`/`instance` label values cannot pass unnoticed.
    #[test]
    fn build_produces_rbac() {
        let resources = build(&minimal_validated_cluster(), &[]).expect("build succeeds");

        assert_eq!(
            resources.service_account.metadata.name.as_deref(),
            Some("my-connect-serviceaccount")
        );
        assert_eq!(
            resources.role_binding.metadata.name.as_deref(),
            Some("my-connect-rolebinding")
        );

        let expected_labels = BTreeMap::from(
            [
                ("app.kubernetes.io/component", "none"),
                ("app.kubernetes.io/instance", "my-connect"),
                (
                    "app.kubernetes.io/managed-by",
                    "spark.stackable.tech_connect",
                ),
                ("app.kubernetes.io/name", "spark-connect"),
                ("app.kubernetes.io/role-group", "none"),
                ("app.kubernetes.io/version", "4.1.2-stackable0.0.0-dev"),
                ("stackable.tech/vendor", "Stackable"),
            ]
            .map(|(key, value)| (key.to_string(), value.to_string())),
        );
        assert_eq!(
            resources.service_account.metadata.labels,
            Some(expected_labels.clone())
        );
        assert_eq!(
            resources.role_binding.metadata.labels,
            Some(expected_labels)
        );
        assert_eq!(
            resources.role_binding.role_ref.name,
            "spark-connect-clusterrole"
        );
    }
}
