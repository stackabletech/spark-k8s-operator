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
    use indoc::indoc;
    use stackable_operator::{
        cli::OperatorEnvironmentOptions,
        k8s_openapi::api::core::v1::{PodSpec, PodTemplateSpec},
    };

    use super::*;
    use crate::{
        crd::{
            constants::{POD_TEMPLATE_FILE, VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_PATH_CONFIG},
            v1alpha1,
        },
        spark_k8s_controller::{dereference::DereferencedSparkApplication, validate::validate},
    };

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *POD_TEMPLATES_COMPONENT_NAME;
        let _ = *SPARK_COMPONENT_NAME;
        let _ = *SPARK_JOB_COMPONENT_NAME;
        let _ = *SPARK_JOB_TEMPLATE_COMPONENT_NAME;
        let _ = *SPARK_SUBMIT_COMPONENT_NAME;
    }

    /// The Pod specs of the submit Job and of the driver and executor pod templates, each with the
    /// name of the resource it was taken from.
    fn pod_specs(enable_vector_agent: bool) -> Vec<(String, PodSpec)> {
        let yaml = format!(
            indoc! {r#"
                apiVersion: spark.stackable.tech/v1alpha1
                kind: SparkApplication
                metadata:
                  name: spark-example
                  namespace: default
                  uid: 12345678-1234-1234-1234-123456789012
                spec:
                  mode: cluster
                  mainApplicationFile: test.py
                  sparkImage:
                    productVersion: 1.2.3
                  image: oci.example.org/jobs/spark-example:1.0.0
                  vectorAggregatorConfigMapName: vector-aggregator-discovery
                  deps:
                    requirements:
                      - tabulate==0.8.9
                    packages:
                      - org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
                  driver:
                    config:
                      logging:
                        enableVectorAgent: {enable_vector_agent}
                  executor:
                    config:
                      logging:
                        enableVectorAgent: {enable_vector_agent}
            "#},
            enable_vector_agent = enable_vector_agent
        );
        let deserializer = serde_yaml::Deserializer::from_str(&yaml);
        let spark_application: v1alpha1::SparkApplication =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
                .expect("invalid test SparkApplication YAML");

        let validated = validate(
            DereferencedSparkApplication {
                spark_application,
                resolved_template_refs: Vec::new(),
                s3_connection: None,
                log_dir: None,
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "stackable-operators".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.example.org/sdp".to_string(),
            },
        )
        .expect("the fixture validates");

        let resources = build(&validated).expect("the resources can be built");

        let mut pod_specs = vec![(
            "spark-submit Job".to_string(),
            resources.jobs[0]
                .spec
                .clone()
                .expect("the Job has a spec")
                .template
                .spec
                .expect("the Job has a pod spec"),
        )];
        for config_map in &resources.config_maps {
            let Some(template) = config_map
                .data
                .as_ref()
                .and_then(|data| data.get(POD_TEMPLATE_FILE))
            else {
                continue;
            };
            let template: PodTemplateSpec =
                serde_yaml::from_str(template).expect("the pod template deserializes");
            pod_specs.push((
                config_map.metadata.name.clone().unwrap_or_default(),
                template.spec.expect("the pod template has a spec"),
            ));
        }

        assert_eq!(pod_specs.len(), 3);

        let vector_containers = pod_specs
            .iter()
            .flat_map(|(_, pod_spec)| &pod_spec.containers)
            .filter(|container| container.name == "vector")
            .count();
        assert_eq!(vector_containers, if enable_vector_agent { 2 } else { 0 });

        pod_specs
    }

    #[test]
    fn every_declared_volume_is_mounted() {
        for enable_vector_agent in [false, true] {
            for (name, pod_spec) in pod_specs(enable_vector_agent) {
                let PodSpec {
                    containers,
                    init_containers,
                    volumes,
                    ..
                } = pod_spec;
                let mounted: Vec<&str> = containers
                    .iter()
                    .chain(init_containers.iter().flatten())
                    .flat_map(|container| container.volume_mounts.iter().flatten())
                    .map(|volume_mount| volume_mount.name.as_str())
                    .collect();
                let unmounted: Vec<&str> = volumes
                    .iter()
                    .flatten()
                    .map(|volume| volume.name.as_str())
                    .filter(|volume_name| !mounted.contains(volume_name))
                    .collect();

                assert!(
                    unmounted.is_empty(),
                    "{name} declares volumes that no container mounts: {unmounted:?}"
                );
            }
        }
    }

    #[test]
    fn spark_containers_mount_the_config_volume() {
        for enable_vector_agent in [false, true] {
            for (name, pod_spec) in pod_specs(enable_vector_agent) {
                let spark_container = pod_spec
                    .containers
                    .iter()
                    .find(|container| container.name == "spark" || container.name == "spark-submit")
                    .unwrap_or_else(|| panic!("{name} has a Spark container"));
                let mount_path = spark_container
                    .volume_mounts
                    .iter()
                    .flatten()
                    .find(|volume_mount| volume_mount.name == VOLUME_MOUNT_NAME_CONFIG.as_ref())
                    .map(|volume_mount| volume_mount.mount_path.as_str());

                assert_eq!(
                    mount_path,
                    Some(VOLUME_MOUNT_PATH_CONFIG),
                    "the Spark container of {name} must mount the config Volume at {VOLUME_MOUNT_PATH_CONFIG}"
                );
            }
        }
    }
}
