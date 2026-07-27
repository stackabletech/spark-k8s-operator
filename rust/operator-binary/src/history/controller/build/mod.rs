pub mod resource;

use snafu::{ResultExt, Snafu};

use crate::{
    crd::constants::HISTORY_ROLE_NAME,
    history::controller::{
        SparkHistoryResources,
        build::resource::{
            config_map::{self, build_config_map},
            listener::build_group_listener,
            pdb::build_pdb,
            rbac::{build_role_binding, build_service_account},
            service::build_rolegroup_metrics_service,
            statefulset::{self, build_stateful_set},
        },
        validate::ValidatedSparkHistoryServer,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap { source: config_map::Error },

    #[snafu(display("failed to build StatefulSet"))]
    BuildStatefulSet { source: statefulset::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds every Kubernetes resource for the given validated SparkHistoryServer.
pub fn build(validated: &ValidatedSparkHistoryServer) -> Result<SparkHistoryResources> {
    let log_dir = &validated.cluster_config.log_dir;

    let mut config_maps = vec![];
    let mut metrics_services = vec![];
    let mut stateful_sets = vec![];

    for (role_group_name, rg) in &validated.role_groups {
        config_maps
            .push(build_config_map(validated, role_group_name, rg).context(BuildConfigMapSnafu)?);
        metrics_services.push(build_rolegroup_metrics_service(validated, role_group_name));
        stateful_sets.push(
            build_stateful_set(validated, role_group_name, rg, log_dir)
                .context(BuildStatefulSetSnafu)?,
        );
    }

    let listener = build_group_listener(
        validated,
        HISTORY_ROLE_NAME,
        validated.role_config.listener_class.clone(),
    );

    let pod_disruption_budget = build_pdb(&validated.role_config.pdb, validated);

    Ok(SparkHistoryResources {
        service_account: build_service_account(validated),
        role_binding: build_role_binding(validated),
        config_maps,
        metrics_services,
        stateful_sets,
        listener,
        pod_disruption_budget,
    })
}

#[cfg(test)]
pub(crate) mod test_support {
    use indoc::indoc;
    use stackable_operator::{cli::OperatorEnvironmentOptions, utils::yaml_from_str_singleton_map};

    use crate::{
        crd::{history::v1alpha1, logdir::ResolvedLogDir},
        history::controller::{
            dereference::DereferencedSparkHistoryServer,
            validate::{ValidatedSparkHistoryServer, validate},
        },
    };

    /// Minimal custom-log-dir `SparkHistoryServer` fixture. The custom log directory keeps the
    /// dereference step client-free; the `uid` allows owner references to be derived from it.
    ///
    /// The cluster name (`my-history`) deliberately differs from the product name
    /// (`spark-history`), so tests asserting recommended labels catch swapped `name`/`instance`
    /// values.
    pub const HISTORY_YAML: &str = indoc! {r#"
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkHistoryServer
        metadata:
          name: my-history
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 3.5.8
          logFileDirectory:
            customLogDirectory: file:///stackable/spark/logs
          nodes:
            roleGroups:
              default:
                replicas: 1
        "#};

    /// Runs the real validate step against the minimal fixture.
    pub fn minimal_validated_cluster() -> ValidatedSparkHistoryServer {
        let shs: v1alpha1::SparkHistoryServer = yaml_from_str_singleton_map(HISTORY_YAML)
            .expect("invalid test SparkHistoryServer YAML");
        validate(
            &shs,
            DereferencedSparkHistoryServer {
                log_dir: ResolvedLogDir::Custom("file:///stackable/spark/logs".to_string()),
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "stackable-operators".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.example.org/sdp".to_string(),
            },
        )
        .expect("validate should succeed for the test fixture")
    }
}
