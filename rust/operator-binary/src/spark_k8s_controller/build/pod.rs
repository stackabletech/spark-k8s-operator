use std::str::FromStr;

use snafu::{OptionExt, ResultExt};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, container::ContainerBuilder, resources::ResourceRequirementsBuilder},
    },
    k8s_openapi::{
        DeepMerge,
        api::core::v1::{
            Container, EnvVar, PodSecurityContext, PodTemplateSpec, ServiceAccount, Volume,
        },
    },
    kube::ResourceExt,
    product_logging::{
        framework::{capture_shell_output, create_vector_shutdown_file_command},
        spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
    },
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::container::EnvVarSet,
            service::{Scraping, prometheus_labels},
        },
        product_logging::framework::{
            VectorContainerLogConfig, validate_logging_configuration_for_container,
            vector_container,
        },
        role_group_utils::ResourceNames,
        types::{
            kubernetes::ConfigMapName,
            operator::{RoleGroupName, RoleName},
        },
    },
};

use crate::{
    crd::{
        constants::*,
        roles::{RoleConfig, SparkApplicationRole, SparkContainer},
        tlscerts,
    },
    spark_k8s_controller::{
        AddVolumeMountSnafu, AddVolumeSnafu, IllegalContainerNameSnafu,
        ParseVectorAggregatorConfigMapNameSnafu, Result, ValidateLoggingConfigSnafu,
        VectorAggregatorConfigMapMissingSnafu, validate,
    },
};

fn init_containers(
    validated: &validate::ValidatedSparkApplication,
    logging: &Logging<SparkContainer>,
) -> Result<Vec<Container>> {
    let spark_application = &validated.spark_application;
    let s3conn = &validated.cluster_config.s3_connection;
    let logdir = &validated.cluster_config.log_dir;
    let spark_image = &validated.resolved_product_image;
    let mut jcb = ContainerBuilder::new(&SparkContainer::Job.to_string())
        .context(IllegalContainerNameSnafu)?;
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
                    .add_volume_mount(VOLUME_MOUNT_NAME_JOB, VOLUME_MOUNT_PATH_JOB)
                    .context(AddVolumeMountSnafu)?
                    .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
                    .context(AddVolumeMountSnafu)?
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

    let mut rcb = ContainerBuilder::new(&SparkContainer::Requirements.to_string())
        .context(IllegalContainerNameSnafu)?;
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
                .add_volume_mount(VOLUME_MOUNT_NAME_REQ, VOLUME_MOUNT_PATH_REQ)
                .context(AddVolumeMountSnafu)?
                .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
                .context(AddVolumeMountSnafu)?
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
    let mut tcb = ContainerBuilder::new(&SparkContainer::Tls.to_string())
        .context(IllegalContainerNameSnafu)?;
    let mut args = Vec::new();

    let tls_container = match tlscerts::tls_secret_names(s3conn, logdir) {
        Some(cert_secrets) => {
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
                    .add_volume_mount(STACKABLE_TRUST_STORE_NAME, STACKABLE_TRUST_STORE)
                    .context(AddVolumeMountSnafu)?
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

pub(crate) fn pod_template(
    validated: &validate::ValidatedSparkApplication,
    role: SparkApplicationRole,
    config: &RoleConfig,
    volumes: &[Volume],
    env: &[EnvVar],
    service_account: &ServiceAccount,
) -> Result<PodTemplateSpec> {
    let spark_application = &validated.spark_application;
    let s3conn = &validated.cluster_config.s3_connection;
    let logdir = &validated.cluster_config.log_dir;
    let spark_image = &validated.resolved_product_image;
    let container_name = SparkContainer::Spark.to_string();
    let mut cb = ContainerBuilder::new(&container_name).context(IllegalContainerNameSnafu)?;
    let merged_env = spark_application.merged_env(role.clone(), env);

    cb.add_volume_mounts(config.volume_mounts(spark_application, s3conn, logdir))
        .context(AddVolumeMountSnafu)?
        .add_env_vars(merged_env)
        .resources(config.resources.clone().into())
        .image_from_product_image(spark_image);

    if config.logging.enable_vector_agent {
        cb.add_env_var(
            "_STACKABLE_POST_HOOK",
            [
                // Wait for Vector to gather the logs.
                "sleep 10",
                &create_vector_shutdown_file_command(VOLUME_MOUNT_PATH_LOG),
            ]
            .join("; "),
        );
    }

    let mut omb = ObjectMetaBuilder::new();
    omb.name(&container_name)
        // this reference is not pointing to a controller but only provides a UID that can used to clean up resources
        // cleanly (specifically driver pods and related config maps) when the spark application is deleted.
        .ownerreference(ownerreference_from_resource(validated, None, None))
        .with_labels(validated.recommended_labels(&container_name));

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
            .as_ref()
            .context(VectorAggregatorConfigMapMissingSnafu)?;
        let vector_log_config = VectorContainerLogConfig {
            log_config: validate_logging_configuration_for_container(
                &config.logging,
                &SparkContainer::Vector,
            )
            .context(ValidateLoggingConfigSnafu)?,
            vector_aggregator_config_map_name: ConfigMapName::from_str(
                vector_aggregator_config_map_name,
            )
            .context(ParseVectorAggregatorConfigMapNameSnafu)?,
        };
        // These resource names are constructed SOLELY to provide the Vector sidecar with its
        // `CLUSTER_NAME`/`ROLE_NAME`/`ROLE_GROUP_NAME` log-metadata env vars. They do NOT affect
        // resource naming. A SparkApplication has no Stackable role groups, so the role group name
        // is a placeholder; the role name reflects the pod's Spark role (driver/executor).
        let vector_resource_names = ResourceNames {
            cluster_name: validated.name.clone(),
            role_name: RoleName::from_str(&role.to_string())
                .expect("a SparkApplicationRole serializes to a valid role name"),
            role_group_name: RoleGroupName::from_str("default")
                .expect("\"default\" is a valid role group name"),
        };
        pb.add_container(vector_container(
            &VECTOR_CONTAINER_NAME,
            spark_image,
            &vector_log_config,
            &vector_resource_names,
            &VOLUME_MOUNT_NAME_CONFIG_TYPED,
            &VOLUME_MOUNT_NAME_LOG_TYPED,
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
    PodSecurityContext {
        fs_group: Some(1000),
        ..PodSecurityContext::default()
    }
}
