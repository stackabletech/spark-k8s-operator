use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    vec,
};

use product_config::{types::PropertyNameKind, writer::to_java_properties_string};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder, container::ContainerBuilder, resources::ResourceRequirementsBuilder,
            volume::VolumeBuilder,
        },
    },
    client::Client,
    commons::product_image_selection::ResolvedProductImage,
    crd::s3,
    k8s_openapi::{
        DeepMerge, Resource,
        api::{
            batch::v1::{Job, JobSpec},
            core::v1::{
                ConfigMap, Container, EnvVar, PodSecurityContext, PodTemplateSpec, Service,
                ServiceAccount, ServicePort, ServiceSpec, Volume,
            },
            rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
    },
    kube::{
        ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    kvp::Label,
    logging::controller::ReconcilerError,
    product_logging::{
        framework::{
            LoggingError, capture_shell_output, create_vector_shutdown_file_command,
            vector_container,
        },
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig, Logging,
        },
    },
    role_utils::RoleGroupRef,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    Ctx,
    crd::{
        constants::*,
        logdir::ResolvedLogDir,
        roles::{RoleConfig, SparkApplicationRole, SparkContainer},
        tlscerts, to_spark_env_sh_string, v1alpha2,
    },
    product_logging::{self},
};

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

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

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

    #[snafu(display("illegal container name"))]
    IllegalContainerName {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {}", role))]
    JvmSecurityProperties {
        source: product_config::writer::PropertiesWriterError,
        role: SparkApplicationRole,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

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
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("SparkApplication [{name}] has no namespace"))]
    SparkApplicationHasNoNamespace { name: String },

    #[snafu(display("failed to get driver Job for SparkApplication [{name}]"))]
    GetDriverJob {
        source: stackable_operator::client::Error,
        name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile(
    spark_application: Arc<DeserializeGuard<v1alpha2::SparkApplication>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let spark_application = spark_application
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidSparkApplicationSnafu)?;

    let client = &ctx.client;

    // Once the driver Job has been created, we must not create it again (see #457). On every
    // subsequent reconcile - triggered by the owned driver Job's status changing - we instead
    // derive the SparkApplication status from that Job and stop.
    if spark_application.k8s_job_has_been_created() {
        tracing::info!(
            spark_application = spark_application.name_any(),
            "Updating SparkApplication status from driver Job"
        );
        return update_status_from_driver_job(client, spark_application).await;
    }

    // It is important to do this at the top of the reconciliation function to ensure
    // all referenced resources and configuration are merged before any of them are created.
    let dereferenced = dereference::dereference(client, spark_application)
        .await
        .context(DereferenceSparkApplicationSnafu)?;

    let validated =
        validate::validate(dereferenced, &ctx.operator_environment, &ctx.product_config)
            .context(ValidateSparkApplicationSnafu)?;

    let spark_application = &validated.spark_application;
    let opt_s3conn = &validated.s3_connection;
    let logdir = &validated.log_dir;
    let resolved_product_image = &validated.resolved_product_image;
    let validated_product_config = &validated.product_config;

    // This is the final version of the spark app to reconcile.
    // No more mutating operations after this point (except for status).
    tracing::debug!("reconciling spark application [{spark_application:?}]");

    let (serviceaccount, rolebinding) =
        build_spark_role_serviceaccount(spark_application, resolved_product_image)?;
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

    let driver_product_config: Option<&HashMap<PropertyNameKind, BTreeMap<String, String>>> =
        validated_product_config
            .get(&SparkApplicationRole::Driver.to_string())
            .and_then(|r| r.get(&"default".to_string()));

    let driver_pod_template_config_map = pod_template_config_map(
        spark_application,
        SparkApplicationRole::Driver,
        &driver_config,
        driver_product_config,
        &env_vars,
        opt_s3conn,
        logdir,
        resolved_product_image,
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

    let executor_product_config: Option<&HashMap<PropertyNameKind, BTreeMap<String, String>>> =
        validated_product_config
            .get(&SparkApplicationRole::Executor.to_string())
            .and_then(|r| r.get(&"default".to_string()));

    let executor_pod_template_config_map = pod_template_config_map(
        spark_application,
        SparkApplicationRole::Executor,
        &executor_config,
        executor_product_config,
        &env_vars,
        opt_s3conn,
        logdir,
        resolved_product_image,
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

    let job_commands = spark_application
        .build_command(opt_s3conn, logdir, &resolved_product_image.image)
        .context(BuildCommandSnafu)?;

    // The driver runs in client mode, so executors connect back to it via a headless Service that
    // selects the driver pod of this application.
    let driver_service = driver_service(spark_application, resolved_product_image)?;
    client
        .apply_patch(SPARK_CONTROLLER_NAME, &driver_service, &driver_service)
        .await
        .context(ApplyApplicationSnafu)?;

    // The driver itself now runs directly as a Kubernetes Job (no separate spark-submit process).
    // Its pod is built from `spec.driver`.
    let job = driver_job(
        spark_application,
        &driver_config,
        &env_vars,
        &job_commands,
        opt_s3conn,
        logdir,
        resolved_product_image,
        &serviceaccount,
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
            &crate::crd::SparkApplicationStatus {
                phase: "Unknown".to_string(),
                resolved_template_ref: validated.resolved_template_refs.clone(),
            },
        )
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: spark_application.name_any(),
        })?;

    Ok(Action::await_change())
}

/// Derives the SparkApplication status from its driver Job and patches it.
///
/// The driver Job carries the same name as the SparkApplication. Once it has finished, Kubernetes
/// garbage collects it via `ttlSecondsAfterFinished`; from then on there is nothing left to derive a
/// status from, so we keep the last known (terminal) status. We must never (re)create the Job here
/// (see #457).
async fn update_status_from_driver_job(
    client: &Client,
    spark_application: &v1alpha2::SparkApplication,
) -> Result<Action> {
    let name = spark_application.name_any();
    let namespace = spark_application
        .metadata
        .namespace
        .as_ref()
        .context(SparkApplicationHasNoNamespaceSnafu { name: name.clone() })?;

    let Some(job) = client
        .get_opt::<Job>(&name, namespace)
        .await
        .context(GetDriverJobSnafu { name: name.clone() })?
    else {
        // The driver Job was already garbage collected. Keep the last known status.
        return Ok(Action::await_change());
    };

    let phase = driver_job_phase(&job);

    client
        .apply_patch_status(
            SPARK_CONTROLLER_NAME,
            spark_application,
            &crate::crd::SparkApplicationStatus {
                phase,
                resolved_template_ref: spark_application
                    .status
                    .as_ref()
                    .map(|s| s.resolved_template_ref.clone())
                    .unwrap_or_default(),
            },
        )
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu { name })?;

    Ok(Action::await_change())
}

/// Maps the driver Job's status to a SparkApplication phase.
///
/// The phase values mirror the pod phases reported previously (`Running`, `Succeeded`, `Failed`,
/// `Unknown`) so that the externally visible status does not change. A Job's `active` count includes
/// both pending and running pods, so a scheduled-but-not-yet-running driver also reports `Running`.
fn driver_job_phase(job: &Job) -> String {
    let Some(status) = job.status.as_ref() else {
        return "Unknown".to_string();
    };

    // Terminal conditions take precedence over the pod counters.
    if let Some(conditions) = status.conditions.as_ref() {
        for condition in conditions {
            if condition.status == "True" {
                match condition.type_.as_str() {
                    "Complete" | "SuccessCriteriaMet" => return "Succeeded".to_string(),
                    "Failed" => return "Failed".to_string(),
                    _ => {}
                }
            }
        }
    }

    if status.active.unwrap_or(0) > 0 {
        "Running".to_string()
    } else if status.succeeded.unwrap_or(0) > 0 {
        "Succeeded".to_string()
    } else if status.failed.unwrap_or(0) > 0 {
        "Failed".to_string()
    } else {
        "Unknown".to_string()
    }
}

fn init_containers(
    spark_application: &v1alpha2::SparkApplication,
    logging: &Logging<SparkContainer>,
    s3conn: &Option<s3::v1alpha1::ConnectionSpec>,
    logdir: &Option<ResolvedLogDir>,
    spark_image: &ResolvedProductImage,
) -> Result<Vec<Container>> {
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

#[allow(clippy::too_many_arguments)]
fn pod_template(
    spark_application: &v1alpha2::SparkApplication,
    role: SparkApplicationRole,
    config: &RoleConfig,
    volumes: &[Volume],
    env: &[EnvVar],
    s3conn: &Option<s3::v1alpha1::ConnectionSpec>,
    logdir: &Option<ResolvedLogDir>,
    spark_image: &ResolvedProductImage,
    service_account: &ServiceAccount,
    // When set, the spark container runs this command (the spark-submit client invocation) as the
    // container `args`, leaving `command` unset so the image entrypoint (run-spark.sh) is used. This
    // is the driver pod, which the operator launches directly as a Kubernetes Job. When `None`
    // (executor pod template), the container also relies on the image entrypoint, since Spark sets
    // the args on the executor pods it creates.
    command: Option<&[String]>,
) -> Result<PodTemplateSpec> {
    let container_name = SparkContainer::Spark.to_string();
    let mut cb = ContainerBuilder::new(&container_name).context(IllegalContainerNameSnafu)?;
    let merged_env = spark_application.merged_env(role.clone(), env);

    cb.add_volume_mounts(config.volume_mounts(spark_application, s3conn, logdir))
        .context(AddVolumeMountSnafu)?
        .add_env_vars(merged_env)
        .resources(config.resources.clone().into())
        .image_from_product_image(spark_image);

    if let Some(command) = command {
        // The driver mounts the executor pod template so that Spark's Kubernetes backend can create
        // executor pods from it.
        cb.add_volume_mount(
            VOLUME_MOUNT_NAME_EXECUTOR_POD_TEMPLATES,
            VOLUME_MOUNT_PATH_EXECUTOR_POD_TEMPLATES,
        )
        .context(AddVolumeMountSnafu)?;

        // The SPARK_SUBMIT_OPTS env var configures the JVM settings of the spark-submit/driver
        // process: it points the JVM to our logging configuration and, if S3 (data or Spark
        // History) is used, to the trust store.
        let mut spark_submit_opts = vec![format!(
            "-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"
        )];
        if tlscerts::tls_secret_names(s3conn, logdir).is_some() {
            spark_submit_opts.push(format!(
                "-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"
            ));
            spark_submit_opts.push(format!(
                "-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"
            ));
        }

        // We pass the spark-submit invocation as container `args` and leave `command` unset so the
        // image entrypoint (run-spark.sh) runs it. The entrypoint evaluates `_STACKABLE_PRE_HOOK`
        // (which starts containerdebug) before, and `_STACKABLE_POST_HOOK` (which writes the Vector
        // shutdown file) after the spark-submit process exits. The latter is what lets the Vector
        // agent terminate so the driver Job pod can complete. run-spark.sh delegates to Spark's
        // entrypoint.sh, which runs our `/bin/bash -c ...` in pass-through mode.
        cb.args(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
            command.join("\n"),
        ])
        .add_env_var("SPARK_SUBMIT_OPTS", spark_submit_opts.join(" "))
        // TODO: move this to the image
        .add_env_var("SPARK_CONF_DIR", "/stackable/spark/conf");
    }

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
        .ownerreference_from_resource(spark_application, None, None)
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(
            &spark_application
                .build_recommended_labels(&spark_image.app_version_label_value, &container_name),
        )
        .context(MetadataBuildSnafu)?;

    // Only the driver pod should be scraped by Prometheus
    // because the executor metrics are also available via /metrics/executors/prometheus/
    if role == SparkApplicationRole::Driver {
        omb.with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?);
    }

    // The actual driver pod (the Job pod, identified by `command` being set) needs the `spark-role`
    // label so the headless driver Service can select it and the operator can identify it.
    if command.is_some() {
        omb.with_label(
            Label::try_from((SPARK_ROLE_LABEL, SPARK_ROLE_DRIVER)).context(LabelBuildSnafu)?,
        );
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

    let init_containers = init_containers(
        spark_application,
        &config.logging,
        s3conn,
        logdir,
        spark_image,
    )
    // TODO (@NickLarsenNZ): Explain this unwrap. Either convert to expect, or gracefully handle the error.
    .unwrap();

    for init_container in init_containers {
        pb.add_init_container(init_container.clone());
    }

    if config.logging.enable_vector_agent {
        match &spark_application.spec.vector_aggregator_config_map_name {
            Some(vector_aggregator_config_map_name) => {
                pb.add_container(
                    vector_container(
                        spark_image,
                        VOLUME_MOUNT_NAME_CONFIG,
                        VOLUME_MOUNT_NAME_LOG,
                        config.logging.containers.get(&SparkContainer::Vector),
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("500m")
                            .with_memory_request("128Mi")
                            .with_memory_limit("128Mi")
                            .build(),
                        vector_aggregator_config_map_name,
                    )
                    .context(ConfigureLoggingSnafu)?,
                );
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }

    let mut pod_template = pb.build_template();
    if let Some(pod_overrides) = spark_application.pod_overrides(role) {
        pod_template.merge_from(pod_overrides);
    }
    Ok(pod_template)
}

#[allow(clippy::too_many_arguments)]
fn pod_template_config_map(
    spark_application: &v1alpha2::SparkApplication,
    role: SparkApplicationRole,
    merged_config: &RoleConfig,
    product_config: Option<&HashMap<PropertyNameKind, BTreeMap<String, String>>>,
    env: &[EnvVar],
    s3conn: &Option<s3::v1alpha1::ConnectionSpec>,
    logdir: &Option<ResolvedLogDir>,
    spark_image: &ResolvedProductImage,
    service_account: &ServiceAccount,
) -> Result<ConfigMap> {
    let cm_name = spark_application.pod_template_config_map_name(role.clone());

    let log_config_map = if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = merged_config.logging.containers.get(&SparkContainer::Spark)
    {
        config_map.into()
    } else {
        cm_name.clone()
    };

    let requested_secret_lifetime = merged_config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    let mut volumes = spark_application
        .volumes(
            s3conn,
            logdir,
            Some(&log_config_map),
            &requested_secret_lifetime,
        )
        .context(CreateVolumesSnafu)?;
    volumes.push(
        VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
            .with_config_map(&cm_name)
            .build(),
    );

    let template = pod_template(
        spark_application,
        role.clone(),
        merged_config,
        volumes.as_ref(),
        env,
        s3conn,
        logdir,
        spark_image,
        service_account,
        None,
    )?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(spark_application)
                .name(&cm_name)
                .ownerreference_from_resource(spark_application, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&spark_application.build_recommended_labels(
                    &spark_image.app_version_label_value,
                    "pod-templates",
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(
            POD_TEMPLATE_FILE,
            serde_yaml::to_string(&template).context(PodTemplateSerdeSnafu)?,
        );

    product_logging::extend_config_map(
        &RoleGroupRef {
            cluster: ObjectRef::from_obj(spark_application),
            role: String::new(),
            role_group: String::new(),
        },
        &merged_config.logging,
        SparkContainer::Spark,
        SparkContainer::Vector,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu { cm_name })?;

    if let Some(product_config) = product_config {
        cm_builder.add_data(
            SPARK_ENV_SH_FILE_NAME,
            to_spark_env_sh_string(
                product_config
                    .get(&PropertyNameKind::File(SPARK_ENV_SH_FILE_NAME.to_string()))
                    .cloned()
                    .unwrap_or_default()
                    .iter(),
            ),
        );

        let jvm_sec_props: BTreeMap<String, Option<String>> = product_config
            .get(&PropertyNameKind::File(
                JVM_SECURITY_PROPERTIES_FILE.to_string(),
            ))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .collect();

        cm_builder.add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(jvm_sec_props.iter())
                .with_context(|_| JvmSecurityPropertiesSnafu { role })?,
        );
    }
    cm_builder.build().context(PodTemplateConfigMapSnafu)
}

/// Headless Service that exposes the driver pod so executors can connect back to it in client mode.
fn driver_service(
    spark_application: &v1alpha2::SparkApplication,
    spark_image: &ResolvedProductImage,
) -> Result<Service> {
    // Select exactly the driver pod of this application.
    let selector = BTreeMap::from([
        (
            "app.kubernetes.io/instance".to_string(),
            spark_application.name_any(),
        ),
        (SPARK_ROLE_LABEL.to_string(), SPARK_ROLE_DRIVER.to_string()),
    ]);

    let service = Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_application)
            .name(spark_application.driver_service_name())
            .ownerreference_from_resource(spark_application, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                &spark_application
                    .build_recommended_labels(&spark_image.app_version_label_value, "driver"),
            )
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(ServiceSpec {
            // Headless: executors resolve the driver pod directly.
            cluster_ip: Some("None".to_string()),
            selector: Some(selector),
            // The driver must be reachable as soon as its pod has an IP, even before it is "ready".
            publish_not_ready_addresses: Some(true),
            ports: Some(vec![
                ServicePort {
                    name: Some("driver".to_string()),
                    port: i32::from(DRIVER_PORT),
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some("block-manager".to_string()),
                    port: i32::from(DRIVER_BLOCK_MANAGER_PORT),
                    ..ServicePort::default()
                },
            ]),
            ..ServiceSpec::default()
        }),
        status: None,
    };

    Ok(service)
}

/// The driver Job. Its pod runs `spark-submit` in client mode and therefore *is* the Spark driver.
/// The pod spec is built from `spec.driver`. Executors are created by the driver via Spark's
/// Kubernetes backend.
#[allow(clippy::too_many_arguments)]
fn driver_job(
    spark_application: &v1alpha2::SparkApplication,
    driver_config: &RoleConfig,
    env: &[EnvVar],
    job_commands: &[String],
    s3conn: &Option<s3::v1alpha1::ConnectionSpec>,
    logdir: &Option<ResolvedLogDir>,
    spark_image: &ResolvedProductImage,
    service_account: &ServiceAccount,
) -> Result<Job> {
    let cm_name = spark_application.pod_template_config_map_name(SparkApplicationRole::Driver);

    let log_config_map = if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = driver_config.logging.containers.get(&SparkContainer::Spark)
    {
        config_map.into()
    } else {
        cm_name.clone()
    };

    let requested_secret_lifetime = driver_config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;

    let mut volumes = spark_application
        .volumes(
            s3conn,
            logdir,
            Some(&log_config_map),
            &requested_secret_lifetime,
        )
        .context(CreateVolumesSnafu)?;
    // The driver's own config (spark-env.sh, security.properties, log4j) ConfigMap.
    volumes.push(
        VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
            .with_config_map(&cm_name)
            .build(),
    );
    // The executor pod template ConfigMap, read by the driver's Spark Kubernetes backend.
    volumes.push(
        VolumeBuilder::new(VOLUME_MOUNT_NAME_EXECUTOR_POD_TEMPLATES)
            .with_config_map(
                spark_application.pod_template_config_map_name(SparkApplicationRole::Executor),
            )
            .build(),
    );

    let mut template = pod_template(
        spark_application,
        SparkApplicationRole::Driver,
        driver_config,
        &volumes,
        env,
        s3conn,
        logdir,
        spark_image,
        service_account,
        Some(job_commands),
    )?;

    // A Job's pod must declare a restart policy.
    if let Some(spec) = template.spec.as_mut() {
        spec.restart_policy = Some("Never".to_string());
        // Give the driver pod a stable hostname ending in `-driver`. The Vector agent reports this
        // hostname as the `pod` field of every log event (see `log_schema.host_key: pod`), and log
        // aggregation/monitoring identifies driver logs by that `-driver` suffix. Without this the
        // Job controller's generated pod name (`<app>-<hash>`) would be used, which Spark previously
        // avoided by naming the cluster-mode driver pod `<app>-...-driver`.
        spec.hostname = Some(spark_application.driver_service_name());
    }

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_application)
            .ownerreference_from_resource(spark_application, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                &spark_application
                    .build_recommended_labels(&spark_image.app_version_label_value, "spark-job"),
            )
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(JobSpec {
            template,
            ttl_seconds_after_finished: Some(600),
            // The driver Job is not retried by default. `spec.job.retryOnFailureCount` configured the
            // old submit Job and is deprecated since v1alpha2.
            backoff_limit: Some(0),
            ..Default::default()
        }),
        status: None,
    };

    Ok(job)
}

/// For a given SparkApplication, we create a ServiceAccount with a RoleBinding to the ClusterRole
/// that allows the driver to create pods etc.
/// Both objects have an owner reference to the SparkApplication, as well as the same name as the app.
/// They are deleted when the job is deleted.
fn build_spark_role_serviceaccount(
    spark_app: &v1alpha2::SparkApplication,
    spark_image: &ResolvedProductImage,
) -> Result<(ServiceAccount, RoleBinding)> {
    // TODO (@NickLarsenNZ): Explain this unwrap. Either convert to expect, or gracefully handle the error.
    let sa_name = spark_app.metadata.name.as_ref().unwrap().to_string();
    let sa =
        ServiceAccount {
            metadata: ObjectMetaBuilder::new()
                .name_and_namespace(spark_app)
                .name(&sa_name)
                .ownerreference_from_resource(spark_app, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&spark_app.build_recommended_labels(
                    &spark_image.app_version_label_value,
                    "service-account",
                ))
                .context(MetadataBuildSnafu)?
                .build(),
            ..ServiceAccount::default()
        };
    let binding_name = &sa_name;
    let binding = RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_app)
            .name(binding_name)
            .ownerreference_from_resource(spark_app, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                &spark_app
                    .build_recommended_labels(&spark_image.app_version_label_value, "role-binding"),
            )
            .context(MetadataBuildSnafu)?
            .build(),
        role_ref: RoleRef {
            api_group: ClusterRole::GROUP.to_string(),
            kind: ClusterRole::KIND.to_string(),
            name: SPARK_CLUSTER_ROLE.to_string(),
        },
        subjects: Some(vec![Subject {
            api_group: Some(ServiceAccount::GROUP.to_string()),
            kind: ServiceAccount::KIND.to_string(),
            name: sa_name,
            namespace: sa.metadata.namespace.clone(),
        }]),
    };
    Ok((sa, binding))
}

fn security_context() -> PodSecurityContext {
    PodSecurityContext {
        fs_group: Some(1000),
        ..PodSecurityContext::default()
    }
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha2::SparkApplication>>,
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
    use stackable_operator::k8s_openapi::api::batch::v1::{Job, JobCondition, JobStatus};

    use super::driver_job_phase;

    fn job_with_status(status: Option<JobStatus>) -> Job {
        Job {
            status,
            ..Job::default()
        }
    }

    fn condition(type_: &str, status: &str) -> JobCondition {
        JobCondition {
            type_: type_.to_string(),
            status: status.to_string(),
            ..JobCondition::default()
        }
    }

    #[test]
    fn no_status_is_unknown() {
        assert_eq!(driver_job_phase(&job_with_status(None)), "Unknown");
    }

    #[test]
    fn empty_status_is_unknown() {
        assert_eq!(
            driver_job_phase(&job_with_status(Some(JobStatus::default()))),
            "Unknown"
        );
    }

    #[test]
    fn active_pod_is_running() {
        assert_eq!(
            driver_job_phase(&job_with_status(Some(JobStatus {
                active: Some(1),
                ..JobStatus::default()
            }))),
            "Running"
        );
    }

    #[test]
    fn succeeded_counter_is_succeeded() {
        assert_eq!(
            driver_job_phase(&job_with_status(Some(JobStatus {
                succeeded: Some(1),
                ..JobStatus::default()
            }))),
            "Succeeded"
        );
    }

    #[test]
    fn failed_counter_is_failed() {
        assert_eq!(
            driver_job_phase(&job_with_status(Some(JobStatus {
                failed: Some(1),
                ..JobStatus::default()
            }))),
            "Failed"
        );
    }

    #[test]
    fn complete_condition_is_succeeded() {
        assert_eq!(
            driver_job_phase(&job_with_status(Some(JobStatus {
                conditions: Some(vec![condition("Complete", "True")]),
                ..JobStatus::default()
            }))),
            "Succeeded"
        );
    }

    #[test]
    fn failed_condition_is_failed() {
        assert_eq!(
            driver_job_phase(&job_with_status(Some(JobStatus {
                conditions: Some(vec![condition("Failed", "True")]),
                ..JobStatus::default()
            }))),
            "Failed"
        );
    }

    #[test]
    fn terminal_condition_wins_over_active_counter() {
        // A Job can still report an active pod while its terminal condition is being set; the
        // terminal condition must take precedence so we don't report "Running" for a finished Job.
        assert_eq!(
            driver_job_phase(&job_with_status(Some(JobStatus {
                active: Some(1),
                conditions: Some(vec![condition("Complete", "True")]),
                ..JobStatus::default()
            }))),
            "Succeeded"
        );
    }

    #[test]
    fn condition_with_false_status_is_ignored() {
        assert_eq!(
            driver_job_phase(&job_with_status(Some(JobStatus {
                active: Some(1),
                conditions: Some(vec![condition("Failed", "False")]),
                ..JobStatus::default()
            }))),
            "Running"
        );
    }
}
