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
    commons::{
        product_image_selection::{self, ResolvedProductImage},
        tls_verification::{CaCert, TlsVerification},
    },
    crd::s3,
    k8s_openapi::{
        DeepMerge, Resource,
        api::{
            batch::v1::{Job, JobSpec},
            core::v1::{
                ConfigMap, Container, EnvVar, PodSecurityContext, PodSpec, PodTemplateSpec,
                ServiceAccount, Volume,
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
    product_config_utils::ValidatedRoleConfigByPropertyKind,
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
        roles::{RoleConfig, SparkApplicationRole, SparkContainer, SubmitConfig},
        tlscerts, to_spark_env_sh_string, v1alpha1,
    },
    product_logging::{self},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

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

    #[snafu(display("failed to configure S3 bucket"))]
    ConfigureS3Bucket {
        source: stackable_operator::crd::s3::v1alpha1::BucketError,
    },

    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: stackable_operator::crd::s3::v1alpha1::ConnectionError,
    },

    #[snafu(display("tls non-verification not supported"))]
    S3TlsNoVerificationNotSupported,

    #[snafu(display("ca-cert verification not supported"))]
    S3TlsCaVerificationNotSupported,

    #[snafu(display("failed to resolve and merge config"))]
    FailedToResolveConfig { source: crate::crd::Error },

    #[snafu(display("failed to recognise the container name"))]
    UnrecognisedContainerName,

    #[snafu(display("illegal container name"))]
    IllegalContainerName {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to resolve the log dir configuration"))]
    LogDir { source: crate::crd::logdir::Error },

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

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig { source: crate::crd::Error },

    #[snafu(display("invalid submit config"))]
    SubmitConfig { source: crate::crd::Error },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
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

    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile(
    spark_application: Arc<DeserializeGuard<v1alpha1::SparkApplication>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

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

    let opt_s3conn = match spark_application.spec.s3connection.as_ref() {
        Some(s3bd) => Some(
            s3bd.clone()
                .resolve(
                    client,
                    // TODO (@NickLarsenNZ): Explain this unwrap. Either convert to expect, or gracefully handle the error.
                    spark_application.metadata.namespace.as_deref().unwrap(),
                )
                .await
                .context(ConfigureS3ConnectionSnafu)?,
        ),
        _ => None,
    };

    // check early for valid verification options
    if let Some(conn) = opt_s3conn.as_ref() {
        if let Some(tls) = &conn.tls.tls {
            match &tls.verification {
                TlsVerification::None {} => return S3TlsNoVerificationNotSupportedSnafu.fail(),
                TlsVerification::Server(server_verification) => {
                    match &server_verification.ca_cert {
                        CaCert::WebPki {} => {}
                        CaCert::SecretClass(_) => {}
                    }
                }
            }
        }
    }

    let logdir = if let Some(log_file_dir) = &spark_application.spec.log_file_directory {
        Some(
            ResolvedLogDir::resolve(
                log_file_dir,
                spark_application.metadata.namespace.clone(),
                client,
            )
            .await
            .context(LogDirSnafu)?,
        )
    } else {
        None
    };

    let resolved_product_image = spark_application
        .spec
        .spark_image
        .resolve(SPARK_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION)
        .context(ResolveProductImageSnafu)?;

    let validated_product_config: ValidatedRoleConfigByPropertyKind = spark_application
        .validated_role_config(&resolved_product_image, &ctx.product_config)
        .context(InvalidProductConfigSnafu)?;

    let (serviceaccount, rolebinding) =
        build_spark_role_serviceaccount(spark_application, &resolved_product_image)?;
    client
        .apply_patch(SPARK_CONTROLLER_NAME, &serviceaccount, &serviceaccount)
        .await
        .context(ApplyServiceAccountSnafu)?;
    client
        .apply_patch(SPARK_CONTROLLER_NAME, &rolebinding, &rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let env_vars = spark_application.env(&opt_s3conn, &logdir);

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
        &opt_s3conn,
        &logdir,
        &resolved_product_image,
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
        &opt_s3conn,
        &logdir,
        &resolved_product_image,
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
        .build_command(&opt_s3conn, &logdir, &resolved_product_image.image)
        .context(BuildCommandSnafu)?;

    let submit_config = spark_application
        .submit_config()
        .context(SubmitConfigSnafu)?;

    let submit_product_config: Option<&HashMap<PropertyNameKind, BTreeMap<String, String>>> =
        validated_product_config
            .get(&SparkApplicationRole::Submit.to_string())
            .and_then(|r| r.get(&"default".to_string()));

    let submit_job_config_map = submit_job_config_map(
        spark_application,
        submit_product_config,
        &resolved_product_image,
    )?;
    client
        .apply_patch(
            SPARK_CONTROLLER_NAME,
            &submit_job_config_map,
            &submit_job_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job = spark_job(
        spark_application,
        &resolved_product_image,
        &serviceaccount,
        &env_vars,
        &job_commands,
        &opt_s3conn,
        &logdir,
        &submit_config,
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
            &v1alpha1::SparkApplicationStatus {
                phase: "Unknown".to_string(),
            },
        )
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: spark_application.name_any(),
        })?;

    Ok(Action::await_change())
}

fn init_containers(
    spark_application: &v1alpha1::SparkApplication,
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
    spark_application: &v1alpha1::SparkApplication,
    role: SparkApplicationRole,
    config: &RoleConfig,
    volumes: &[Volume],
    env: &[EnvVar],
    s3conn: &Option<s3::v1alpha1::ConnectionSpec>,
    logdir: &Option<ResolvedLogDir>,
    spark_image: &ResolvedProductImage,
    service_account: &ServiceAccount,
) -> Result<PodTemplateSpec> {
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
        .ownerreference_from_resource(spark_application, None, None)
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(
            spark_application
                .build_recommended_labels(&spark_image.app_version_label_value, &container_name),
        )
        .context(MetadataBuildSnafu)?;

    // Only the driver pod should be scraped by Prometheus
    // because the executor metrics are also available via /metrics/executors/prometheus/
    if role == SparkApplicationRole::Driver {
        omb.with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?);
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
    spark_application: &v1alpha1::SparkApplication,
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
    )?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(spark_application)
                .name(&cm_name)
                .ownerreference_from_resource(spark_application, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(spark_application.build_recommended_labels(
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

fn submit_job_config_map(
    spark_application: &v1alpha1::SparkApplication,
    product_config: Option<&HashMap<PropertyNameKind, BTreeMap<String, String>>>,
    spark_image: &ResolvedProductImage,
) -> Result<ConfigMap> {
    let cm_name = spark_application.submit_job_config_map_name();

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder.metadata(
        ObjectMetaBuilder::new()
            .name_and_namespace(spark_application)
            .name(&cm_name)
            .ownerreference_from_resource(spark_application, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                spark_application
                    .build_recommended_labels(&spark_image.app_version_label_value, "spark-submit"),
            )
            .context(MetadataBuildSnafu)?
            .build(),
    );

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
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                JvmSecurityPropertiesSnafu {
                    role: SparkApplicationRole::Submit,
                }
            })?,
        );
    }

    cm_builder.build().context(PodTemplateConfigMapSnafu)
}

#[allow(clippy::too_many_arguments)]
fn spark_job(
    spark_application: &v1alpha1::SparkApplication,
    spark_image: &ResolvedProductImage,
    serviceaccount: &ServiceAccount,
    env: &[EnvVar],
    job_commands: &[String],
    s3conn: &Option<s3::v1alpha1::ConnectionSpec>,
    logdir: &Option<ResolvedLogDir>,
    job_config: &SubmitConfig,
) -> Result<Job> {
    let mut cb = ContainerBuilder::new(&SparkContainer::SparkSubmit.to_string())
        .context(IllegalContainerNameSnafu)?;

    let merged_env = spark_application.merged_env(SparkApplicationRole::Submit, env);

    // The SPARK_SUBMIT_OPTS env var is used to configure the JVM settings of the spark-submit job.
    // Here we need to point the JVM to our logging configuration and if S3 is used for data or Spark History,
    // we also need to tell the JVM where the trust store is located.
    // The same properties are also set for the driver and executor pods via the pod template config maps.
    let mut spark_submit_opts_env = vec![format!(
        "-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"
    )];
    if tlscerts::tls_secret_names(s3conn, logdir).is_some() {
        spark_submit_opts_env.push(format!(
            "-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"
        ));
        spark_submit_opts_env.push(format!(
            "-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"
        ));
    }
    cb.image_from_product_image(spark_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![job_commands.join("\n")])
        .resources(job_config.resources.clone().into())
        .add_volume_mounts(spark_application.spark_job_volume_mounts(s3conn, logdir))
        .context(AddVolumeMountSnafu)?
        .add_env_vars(merged_env)
        .add_env_var("SPARK_SUBMIT_OPTS", spark_submit_opts_env.join(" "))
        // TODO: move this to the image
        .add_env_var("SPARK_CONF_DIR", "/stackable/spark/conf");

    let mut volumes = vec![
        VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
            .with_config_map(spark_application.submit_job_config_map_name())
            .build(),
        VolumeBuilder::new(VOLUME_MOUNT_NAME_DRIVER_POD_TEMPLATES)
            .with_config_map(
                spark_application.pod_template_config_map_name(SparkApplicationRole::Driver),
            )
            .build(),
        VolumeBuilder::new(VOLUME_MOUNT_NAME_EXECUTOR_POD_TEMPLATES)
            .with_config_map(
                spark_application.pod_template_config_map_name(SparkApplicationRole::Executor),
            )
            .build(),
    ];
    let requested_secret_lifetime = job_config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    volumes.extend(
        spark_application
            .volumes(s3conn, logdir, None, &requested_secret_lifetime)
            .context(CreateVolumesSnafu)?,
    );

    let containers = vec![cb.build()];

    let mut pod = PodTemplateSpec {
        metadata: Some(
            ObjectMetaBuilder::new()
                .name("spark-submit")
                .with_recommended_labels(spark_application.build_recommended_labels(
                    &spark_image.app_version_label_value,
                    "spark-job-template",
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        ),
        spec: Some(PodSpec {
            containers,
            restart_policy: Some("Never".to_string()),
            service_account_name: serviceaccount.metadata.name.clone(),
            volumes: Some(volumes),
            image_pull_secrets: spark_image.pull_secrets.clone(),
            security_context: Some(security_context()),
            ..PodSpec::default()
        }),
    };

    if let Some(submit_pod_overrides) =
        spark_application.pod_overrides(SparkApplicationRole::Submit)
    {
        pod.merge_from(submit_pod_overrides);
    }

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_application)
            .ownerreference_from_resource(spark_application, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                spark_application
                    .build_recommended_labels(&spark_image.app_version_label_value, "spark-job"),
            )
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(JobSpec {
            template: pod,
            ttl_seconds_after_finished: Some(600),
            backoff_limit: Some(spark_application.retry_on_failure_count()),
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
    spark_app: &v1alpha1::SparkApplication,
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
                .with_recommended_labels(spark_app.build_recommended_labels(
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
                spark_app
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
    _obj: Arc<DeserializeGuard<v1alpha1::SparkApplication>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidSparkApplication { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}
