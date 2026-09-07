use std::str::FromStr;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder, resources::ResourceRequirementsBuilder, security::PodSecurityContextBuilder,
        },
    },
    constant,
    k8s_openapi::{
        DeepMerge,
        api::core::v1::{Container, PodSecurityContext, PodTemplateSpec, ServiceAccount, Volume},
    },
    kube::ResourceExt,
    product_logging::{
        framework::{capture_shell_output, create_vector_shutdown_file_command},
        spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
    },
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::container::{EnvVarName, EnvVarSet, new_container_builder},
            service::{Scraping, prometheus_labels},
        },
        product_logging::framework::{
            VectorContainerLogConfig, validate_logging_configuration_for_container,
            vector_container,
        },
        role_group_utils::ResourceNames,
        types::operator::RoleGroupName,
    },
};

use crate::{
    crd::{
        STACKABLE_PRE_HOOK,
        constants::*,
        roles::{RoleConfig, SparkApplicationRole, SparkContainer},
        tlscerts,
    },
    spark_k8s_controller::{
        build::{SPARK_COMPONENT_NAME, recommended_labels_for_component_resources},
        validate,
    },
};

// `_STACKABLE_POST_HOOK` is evaluated by the entrypoint script (run-spark.sh) in the Spark images
// after the actual JVM process has finished; the operator uses it to give Vector time to gather
// the logs and to shut it down afterwards.
constant!(STACKABLE_POST_HOOK: EnvVarName = "_STACKABLE_POST_HOOK");

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to validate the logging configuration"))]
    ValidateLoggingConfig {
        source: stackable_operator::v2::product_logging::framework::Error,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,
}

type Result<T, E = Error> = std::result::Result<T, E>;

fn init_containers(
    validated: &validate::ValidatedSparkApplication,
    logging: &Logging<SparkContainer>,
) -> Result<Vec<Container>> {
    let spark_application = &validated.spark_application;
    let s3conn = &validated.cluster_config.s3_connection;
    let logdir = &validated.cluster_config.log_dir;
    let spark_image = &validated.resolved_product_image;
    let mut jcb = new_container_builder(&SparkContainer::Job.to_container_name());
    let job_container = match &spark_application.spec.image {
        Some(job_image) => {
            let mut args = Vec::new();
            if let Some(ContainerLogConfig {
                choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
            }) = logging.containers.get(&SparkContainer::Job)
            {
                args.push(capture_shell_output(
                    VOLUME_MOUNT_PATH_LOG,
                    &SparkContainer::Job.to_string(),
                    log_config,
                ));
            };
            args.push(format!("echo Copying job files to {VOLUME_MOUNT_PATH_JOB}"));
            args.push(format!("cp /jobs/* {VOLUME_MOUNT_PATH_JOB}"));
            // Wait until the log file is written.
            args.push("sleep 1".into());

            Some(
                jcb.image(job_image)
                    .command(vec![
                        "/bin/bash".to_string(),
                        "-x".to_string(),
                        "-euo".to_string(),
                        "pipefail".to_string(),
                        "-c".to_string(),
                    ])
                    .args(vec![args.join("\n")])
                    .add_volume_mount(VOLUME_MOUNT_NAME_JOB.as_ref(), VOLUME_MOUNT_PATH_JOB)
                    .expect(
                        "The mount paths are statically defined and there should be no duplicates.",
                    )
                    .add_volume_mount(VOLUME_MOUNT_NAME_LOG.as_ref(), VOLUME_MOUNT_PATH_LOG)
                    .expect(
                        "The mount paths are statically defined and there should be no duplicates.",
                    )
                    .resources(
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("500m")
                            .with_memory_request("128Mi")
                            .with_memory_limit("128Mi")
                            .build(),
                    )
                    .build(),
            )
        }
        None => None,
    };

    let mut rcb = new_container_builder(&SparkContainer::Requirements.to_container_name());
    let requirements_container = match spark_application.requirements() {
        Some(req) => {
            let mut args = Vec::new();
            if let Some(ContainerLogConfig {
                choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
            }) = logging.containers.get(&SparkContainer::Requirements)
            {
                args.push(capture_shell_output(
                    VOLUME_MOUNT_PATH_LOG,
                    &SparkContainer::Requirements.to_string(),
                    log_config,
                ));
            };
            args.push(format!(
                "echo Installing requirements to {VOLUME_MOUNT_PATH_REQ}: {req}"
            ));
            args.push(format!(
                "pip install --target={VOLUME_MOUNT_PATH_REQ} {req}"
            ));

            rcb.image(&spark_image.image)
                .command(vec![
                    "/bin/bash".to_string(),
                    "-x".to_string(),
                    "-euo".to_string(),
                    "pipefail".to_string(),
                    "-c".to_string(),
                ])
                .args(vec![args.join("\n")])
                .add_volume_mount(VOLUME_MOUNT_NAME_REQ.as_ref(), VOLUME_MOUNT_PATH_REQ)
                .expect("The mount paths are statically defined and there should be no duplicates.")
                .add_volume_mount(VOLUME_MOUNT_NAME_LOG.as_ref(), VOLUME_MOUNT_PATH_LOG)
                .expect("The mount paths are statically defined and there should be no duplicates.")
                .image_pull_policy(&spark_image.image_pull_policy);

            rcb.resources(
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("250m")
                    .with_cpu_limit("1000m")
                    .with_memory_request("1024Mi")
                    .with_memory_limit("1024Mi")
                    .build(),
            );

            Some(rcb.build())
        }
        None => None,
    };

    // if TLS is enabled, build TrustStore and put secret inside.
    let mut tcb = new_container_builder(&SparkContainer::Tls.to_container_name());
    let mut args = Vec::new();

    let tls_container = match tlscerts::tls_secret_names(s3conn, logdir) {
        Some(cert_secrets) => {
            // The static truststore mount is added first; the certificate mounts below embed the
            // user-supplied SecretClass names and therefore stay fallible.
            tcb.add_volume_mount(STACKABLE_TRUST_STORE_NAME, STACKABLE_TRUST_STORE)
                .expect(
                    "The mount paths are statically defined and there should be no duplicates.",
                );
            args.push(tlscerts::convert_system_trust_store_to_pkcs12());
            for cert_secret in cert_secrets {
                args.push(tlscerts::import_truststore(cert_secret));
                tcb.add_volume_mount(
                    cert_secret,
                    format!("{STACKABLE_MOUNT_PATH_TLS}/{cert_secret}"),
                )
                .context(AddVolumeMountSnafu)?;
            }
            Some(
                tcb.image(&spark_image.image)
                    .command(vec![
                        "/bin/bash".to_string(),
                        "-x".to_string(),
                        "-euo".to_string(),
                        "pipefail".to_string(),
                        "-c".to_string(),
                    ])
                    .args(vec![args.join("\n")])
                    .resources(
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("1000m")
                            .with_memory_request("1024Mi")
                            .with_memory_limit("1024Mi")
                            .build(),
                    )
                    .build(),
            )
        }
        None => None,
    };

    Ok(vec![job_container, requirements_container, tls_container]
        .into_iter()
        .flatten()
        .collect())
}

/// Ensure that hook and command are put together without breaking the hook.
fn append_to_hook(hook: &str, command: &str) -> String {
    let hook = hook.trim_end();
    match hook.chars().last() {
        None => command.to_owned(),
        Some('&' | ';') => format!("{hook} {command}"),
        Some(_) => format!("{hook}; {command}"),
    }
}

pub(crate) fn pod_template(
    validated: &validate::ValidatedSparkApplication,
    role: SparkApplicationRole,
    config: &RoleConfig,
    volumes: &[Volume],
    env: &EnvVarSet,
    service_account: &ServiceAccount,
) -> Result<PodTemplateSpec> {
    let spark_application = &validated.spark_application;
    let s3conn = &validated.cluster_config.s3_connection;
    let logdir = &validated.cluster_config.log_dir;
    let spark_image = &validated.resolved_product_image;
    let container_name = SparkContainer::Spark.to_string();
    let mut cb = new_container_builder(&SparkContainer::Spark.to_container_name());

    let mut env = env.clone();

    let pre_hook = env
        .get(&STACKABLE_PRE_HOOK)
        .and_then(|env_var| env_var.value.clone())
        .unwrap_or_default();
    // SPARK_ENV_LOADED keeps Spark from sourcing spark-env.sh again and overwriting the values.
    env = env.with_value(
        &STACKABLE_PRE_HOOK,
        append_to_hook(
            &pre_hook,
            &format!(
                ". {VOLUME_MOUNT_PATH_CONFIG}/{SPARK_ENV_SH_FILE_NAME}; export SPARK_ENV_LOADED=1"
            ),
        ),
    );

    if config.logging.enable_vector_agent {
        env = env.with_value(
            &STACKABLE_POST_HOOK,
            [
                // Wait for Vector to gather the logs.
                "sleep 10",
                &create_vector_shutdown_file_command(VOLUME_MOUNT_PATH_LOG),
            ]
            .join("; "),
        );
    }
    // The env overrides are merged in last so that they override any operator-set environment
    // variable.
    let merged_env = spark_application.merged_env(role.clone(), env);

    cb.add_volume_mount(VOLUME_MOUNT_NAME_CONFIG.as_ref(), VOLUME_MOUNT_PATH_CONFIG)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        // These mounts include the user-supplied `volumeMounts`, so this add stays fallible.
        .add_volume_mounts(config.volume_mounts(spark_application, s3conn, logdir))
        .context(AddVolumeMountSnafu)?
        .add_env_vars(merged_env)
        .resources(config.resources.clone().into())
        .image_from_product_image(spark_image);

    let mut omb = ObjectMetaBuilder::new();
    omb.name(&container_name)
        // this reference is not pointing to a controller but only provides a UID that can used to clean up resources
        // cleanly (specifically driver pods and related config maps) when the spark application is deleted.
        .ownerreference(ownerreference_from_resource(validated, None, None))
        .with_labels(recommended_labels_for_component_resources(
            validated,
            &SPARK_COMPONENT_NAME,
        ));

    // Only the driver pod should be scraped by Prometheus
    // because the executor metrics are also available via /metrics/executors/prometheus/
    if role == SparkApplicationRole::Driver {
        omb.with_labels(prometheus_labels(&Scraping::Enabled));
    }

    let mut metadata = omb.build();

    // We explicitly remove the application owner reference from driver and executor pods.
    //
    // The executors then only have the driver as owner and Kubernetes can garbage collect them
    // early when the driver pod or the spark-submit job is deleted.
    // Drivers must not have the SparkApplication as owner because this prevents proper cleanup
    // when the application is finished.
    // The submit pod doesn't use this function right now, but we keep the "if" below for future
    // sanity.
    if role == SparkApplicationRole::Executor || role == SparkApplicationRole::Driver {
        metadata.owner_references = None;
    }

    let mut pb = PodBuilder::new();
    pb.metadata(metadata)
        .add_container(cb.build())
        // These volumes include the user-supplied `volumes`, so this add stays fallible.
        .add_volumes(volumes.to_vec())
        .context(AddVolumeSnafu)?
        .security_context(security_context())
        .image_pull_secrets_from_product_image(spark_image)
        .affinity(&config.affinity)
        .service_account_name(service_account.name_any());

    let init_containers = init_containers(validated, &config.logging)?;

    for init_container in init_containers {
        pb.add_init_container(init_container.clone());
    }

    if config.logging.enable_vector_agent {
        let vector_aggregator_config_map_name = spark_application
            .spec
            .vector_aggregator_config_map_name
            .clone()
            .context(VectorAggregatorConfigMapMissingSnafu)?;
        let vector_log_config = VectorContainerLogConfig {
            log_config: validate_logging_configuration_for_container(
                &config.logging,
                &SparkContainer::Vector,
            )
            .context(ValidateLoggingConfigSnafu)?,
            vector_aggregator_config_map_name,
        };
        // These resource names are constructed SOLELY to provide the Vector sidecar with its
        // `CLUSTER_NAME`/`ROLE_NAME`/`ROLE_GROUP_NAME` log-metadata env vars. They do NOT affect
        // resource naming. A SparkApplication has no Stackable role groups, so the role group name
        // is a placeholder; the role name reflects the pod's Spark role (driver/executor).
        let vector_resource_names = ResourceNames {
            cluster_name: validated.name.clone(),
            role_name: role.role_name(),
            role_group_name: RoleGroupName::from_str("default")
                .expect("\"default\" is a valid role group name"),
        };
        pb.add_container(vector_container(
            &SparkContainer::Vector.to_container_name(),
            spark_image,
            &vector_log_config,
            &vector_resource_names,
            &VOLUME_MOUNT_NAME_CONFIG,
            &VOLUME_MOUNT_NAME_LOG,
            EnvVarSet::new(),
        ));
    }

    let mut pod_template = pb.build_template();
    if let Some(pod_overrides) = spark_application.pod_overrides(role) {
        pod_template.merge_from(pod_overrides);
    }
    Ok(pod_template)
}

pub(crate) fn security_context() -> PodSecurityContext {
    PodSecurityContextBuilder::with_stackable_defaults()
        .fs_group(1000)
        .build()
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use rstest::rstest;
    use stackable_operator::{
        cli::OperatorEnvironmentOptions,
        commons::{
            secret_class::SecretClassVolume,
            tls_verification::{
                CaCert, Tls, TlsClientDetails, TlsServerVerification, TlsVerification,
            },
        },
        crd::s3,
        k8s_openapi::{api::core::v1::EnvVar, apimachinery::pkg::apis::meta::v1::ObjectMeta},
    };

    use super::*;
    use crate::{
        crd::v1alpha1,
        spark_k8s_controller::{dereference::DereferencedSparkApplication, validate::validate},
    };

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *STACKABLE_POST_HOOK;
    }

    #[rstest]
    #[case("containerdebug --loop &", "containerdebug --loop & . spark-env.sh")]
    #[case("containerdebug --loop & ", "containerdebug --loop & . spark-env.sh")]
    #[case("fetch-truststore", "fetch-truststore; . spark-env.sh")]
    #[case("fetch-truststore;", "fetch-truststore; . spark-env.sh")]
    #[case("", ". spark-env.sh")]
    fn append_to_hook_separates_commands(#[case] hook: &str, #[case] expected: &str) {
        assert_eq!(append_to_hook(hook, ". spark-env.sh"), expected)
    }

    /// `envOverrides` must be applied after all operator-set environment variables, so a user
    /// override replaces the operator-set value instead of duplicating it or being ignored.
    #[test]
    fn env_overrides_override_operator_set_env_vars() {
        let yaml = indoc! {r#"
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
              driver:
                envOverrides:
                  CONTAINERDEBUG_LOG_DIRECTORY: /custom/log/dir
        "#};
        let deserializer = serde_yaml::Deserializer::from_str(yaml);
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

        let driver_config = validated
            .spark_application
            .driver_config()
            .expect("the driver config resolves");
        let env = validated
            .spark_application
            .env(&None, &None)
            .expect("the base environment can be built");
        let service_account = ServiceAccount {
            metadata: ObjectMeta {
                name: Some("spark-example".to_string()),
                ..ObjectMeta::default()
            },
            ..ServiceAccount::default()
        };

        let template = pod_template(
            &validated,
            SparkApplicationRole::Driver,
            &driver_config,
            &[],
            &env,
            &service_account,
        )
        .expect("the driver pod template can be built");

        let env: Vec<EnvVar> = template
            .spec
            .expect("the pod template has a spec")
            .containers
            .iter()
            .find(|container| container.name == "spark")
            .expect("the spark container exists")
            .env
            .clone()
            .expect("the spark container has env vars");

        let matching: Vec<&EnvVar> = env
            .iter()
            .filter(|env_var| env_var.name == "CONTAINERDEBUG_LOG_DIRECTORY")
            .collect();

        // The override must replace the operator-set value, not duplicate it.
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].value.as_deref(), Some("/custom/log/dir"));
    }

    /// Runs the real validate step against the given SparkApplication YAML, optionally with a
    /// dereferenced S3 connection.
    fn validated_from_yaml(
        yaml: &str,
        s3_connection: Option<s3::v1alpha1::ConnectionSpec>,
    ) -> validate::ValidatedSparkApplication {
        let deserializer = serde_yaml::Deserializer::from_str(yaml);
        let spark_application: v1alpha1::SparkApplication =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
                .expect("invalid test SparkApplication YAML");

        validate(
            DereferencedSparkApplication {
                spark_application,
                resolved_template_refs: Vec::new(),
                s3_connection,
                log_dir: None,
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "stackable-operators".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.example.org/sdp".to_string(),
            },
        )
        .expect("the fixture validates")
    }

    fn test_service_account() -> ServiceAccount {
        ServiceAccount {
            metadata: ObjectMeta {
                name: Some("spark-example".to_string()),
                ..ObjectMeta::default()
            },
            ..ServiceAccount::default()
        }
    }

    /// The static mounts are added with `expect` before the user-supplied `volumeMounts`, so a
    /// user mount colliding with an operator mount path must surface as an error, never as a
    /// panic.
    #[test]
    fn user_volume_mount_colliding_with_config_path_is_an_error() {
        let yaml = indoc! {r#"
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
              driver:
                config:
                  volumeMounts:
                    - name: user-conf
                      mountPath: /stackable/spark/conf
              volumes:
                - name: user-conf
                  emptyDir: {}
        "#};
        let validated = validated_from_yaml(yaml, None);
        let driver_config = validated
            .spark_application
            .driver_config()
            .expect("the driver config resolves");
        let env = validated
            .spark_application
            .env(&None, &None)
            .expect("the base environment can be built");

        let result = pod_template(
            &validated,
            SparkApplicationRole::Driver,
            &driver_config,
            &[],
            &env,
            &test_service_account(),
        );

        assert!(
            matches!(result, Err(Error::AddVolumeMount { .. })),
            "expected Err(AddVolumeMount), got {result:?}"
        );
    }

    /// The `tls` init container only exists when an S3 connection verifies TLS with a SecretClass
    /// CA. Its static truststore mount is added with `expect` and must therefore precede the
    /// SecretClass-derived certificate mounts, which stay fallible. The same fixture exercises the
    /// SecretClass-derived Volumes of `SparkApplication::volumes`.
    #[test]
    fn tls_init_container_mounts_truststore_before_certificate_secrets() {
        let yaml = indoc! {r#"
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
        "#};
        let s3_connection = s3::v1alpha1::ConnectionSpec {
            host: "my-s3".to_string().try_into().expect("valid host"),
            port: None,
            access_style: Default::default(),
            credentials: Some(SecretClassVolume::new("s3-creds".to_string(), None)),
            tls: TlsClientDetails {
                tls: Some(Tls {
                    verification: TlsVerification::Server(TlsServerVerification {
                        ca_cert: CaCert::SecretClass("s3-ca".to_string()),
                    }),
                }),
            },
            region: Default::default(),
        };
        let validated = validated_from_yaml(yaml, Some(s3_connection));
        let s3conn = &validated.cluster_config.s3_connection;
        let driver_config = validated
            .spark_application
            .driver_config()
            .expect("the driver config resolves");
        let env = validated
            .spark_application
            .env(s3conn, &None)
            .expect("the base environment can be built");
        let requested_secret_lifetime = driver_config
            .requested_secret_lifetime
            .expect("the default secret lifetime is set");

        let volumes =
            validated
                .spark_application
                .volumes(s3conn, &None, None, &requested_secret_lifetime);
        let volume_names: Vec<&str> = volumes.iter().map(|volume| volume.name.as_str()).collect();
        assert!(volume_names.contains(&"s3-creds"), "{volume_names:?}");
        assert!(volume_names.contains(&"s3-ca"), "{volume_names:?}");
        assert!(
            volume_names.contains(&STACKABLE_TRUST_STORE_NAME),
            "{volume_names:?}"
        );

        let template = pod_template(
            &validated,
            SparkApplicationRole::Driver,
            &driver_config,
            &volumes,
            &env,
            &test_service_account(),
        )
        .expect("the driver pod template can be built");

        let tls_container = template
            .spec
            .expect("the pod template has a spec")
            .init_containers
            .into_iter()
            .flatten()
            .find(|container| container.name == "tls")
            .expect("the tls init container exists");
        let mount_names: Vec<String> = tls_container
            .volume_mounts
            .into_iter()
            .flatten()
            .map(|volume_mount| volume_mount.name)
            .collect();

        assert_eq!(mount_names, vec![STACKABLE_TRUST_STORE_NAME, "s3-ca"]);
    }
}
