use crate::Ctx;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    vec,
};

use product_config::writer::to_java_properties_string;
use stackable_operator::time::Duration;
use stackable_spark_k8s_crd::{
    constants::*, s3logdir::S3LogDir, tlscerts, RoleConfig, SparkApplication, SparkApplicationRole,
    SparkContainer, SubmitConfig,
};

use crate::product_logging::{self, resolve_vector_aggregator_address};
use product_config::types::PropertyNameKind;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::k8s_openapi::DeepMerge;
use stackable_operator::{
    builder::{
        configmap::{ConfigMapBuilder, Error as ConfigMapError},
        meta::{Error as MetaError, ObjectMetaBuilder},
        pod::container::{ContainerBuilder, Error as ContainerError},
        pod::resources::ResourceRequirementsBuilder,
        pod::volume::VolumeBuilder,
        pod::PodBuilder,
    },
    client::Error as ClientError,
    commons::{
        authentication::tls::{CaCert, TlsVerification},
        product_image_selection::ResolvedProductImage,
        s3::S3ConnectionSpec,
    },
    k8s_openapi::{
        api::{
            batch::v1::{Job, JobSpec},
            core::v1::{
                ConfigMap, Container, EnvVar, PodSecurityContext, PodSpec, PodTemplateSpec,
                ServiceAccount, Volume,
            },
            rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
        Resource,
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        ResourceExt,
    },
    logging::controller::ReconcilerError,
    product_config_utils::Error as ConfigError,
    product_config_utils::ValidatedRoleConfigByPropertyKind,
    product_logging::{
        framework::{capture_shell_output, create_vector_shutdown_file_command, vector_container},
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig, Logging,
        },
    },
    role_utils::RoleGroupRef,
};

use strum::{EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef { source: MetaError },
    #[snafu(display("failed to apply role ServiceAccount"))]
    ApplyServiceAccount { source: ClientError },
    #[snafu(display("failed to apply global RoleBinding"))]
    ApplyRoleBinding { source: ClientError },
    #[snafu(display("failed to apply Job"))]
    ApplyApplication { source: ClientError },
    #[snafu(display("failed to build stark-submit command"))]
    BuildCommand {
        source: stackable_spark_k8s_crd::Error,
    },
    #[snafu(display("failed to build the pod template config map"))]
    PodTemplateConfigMap { source: ConfigMapError },
    #[snafu(display("pod template serialization"))]
    PodTemplateSerde { source: serde_yaml::Error },
    #[snafu(display("s3 bucket error"))]
    S3Bucket {
        source: stackable_operator::commons::s3::Error,
    },
    #[snafu(display("tls non-verification not supported"))]
    S3TlsNoVerificationNotSupported,
    #[snafu(display("ca-cert verification not supported"))]
    S3TlsCaVerificationNotSupported,
    #[snafu(display("failed to resolve and merge config"))]
    FailedToResolveConfig {
        source: stackable_spark_k8s_crd::Error,
    },
    #[snafu(display("failed to recognise the container name"))]
    UnrecognisedContainerName,
    #[snafu(display("illegal container name"))]
    IllegalContainerName { source: ContainerError },
    #[snafu(display("failed to resolve the s3 log dir configuration"))]
    S3LogDir {
        source: stackable_spark_k8s_crd::s3logdir::Error,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress { source: product_logging::Error },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: product_logging::Error,
        cm_name: String,
    },
    #[snafu(display("failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {}", role))]
    JvmSecurityProperties {
        source: product_config::writer::PropertiesWriterError,
        role: SparkApplicationRole,
    },
    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig { source: ConfigError },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_spark_k8s_crd::Error,
    },
    #[snafu(display("invalid submit config"))]
    SubmitConfig {
        source: stackable_spark_k8s_crd::Error,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild { source: MetaError },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("failed to create Volumes for SparkApplication"))]
    CreateVolumes {
        source: stackable_spark_k8s_crd::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile(spark_application: Arc<SparkApplication>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let client = &ctx.client;

    let opt_s3conn = match spark_application.spec.s3connection.as_ref() {
        Some(s3bd) => s3bd
            .resolve(
                client,
                spark_application.metadata.namespace.as_deref().unwrap(),
            )
            .await
            .context(S3BucketSnafu)
            .ok(),
        _ => None,
    };

    // check early for valid verification options
    if let Some(conn) = opt_s3conn.as_ref() {
        if let Some(tls) = &conn.tls {
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

    let s3logdir = S3LogDir::resolve(
        spark_application.spec.log_file_directory.as_ref(),
        spark_application.metadata.namespace.clone(),
        client,
    )
    .await
    .context(S3LogDirSnafu)?;

    let resolved_product_image = spark_application
        .spec
        .spark_image
        .resolve(SPARK_IMAGE_BASE_NAME, crate::built_info::CARGO_PKG_VERSION);

    let validated_product_config: ValidatedRoleConfigByPropertyKind = spark_application
        .validated_role_config(&resolved_product_image, &ctx.product_config)
        .context(InvalidProductConfigSnafu)?;

    let (serviceaccount, rolebinding) =
        build_spark_role_serviceaccount(&spark_application, &resolved_product_image)?;
    client
        .apply_patch(CONTROLLER_NAME, &serviceaccount, &serviceaccount)
        .await
        .context(ApplyServiceAccountSnafu)?;
    client
        .apply_patch(CONTROLLER_NAME, &rolebinding, &rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let vector_aggregator_address = resolve_vector_aggregator_address(
        client,
        spark_application
            .namespace()
            .as_deref()
            .context(ObjectHasNoNamespaceSnafu)?,
        spark_application
            .spec
            .vector_aggregator_config_map_name
            .as_deref(),
    )
    .await
    .context(ResolveVectorAggregatorAddressSnafu)?;

    let env_vars = spark_application.env(&opt_s3conn, &s3logdir);

    let driver_config = spark_application
        .driver_config()
        .context(FailedToResolveConfigSnafu)?;

    let driver_product_config: Option<&HashMap<PropertyNameKind, BTreeMap<String, String>>> =
        validated_product_config
            .get(&SparkApplicationRole::Driver.to_string())
            .and_then(|r| r.get(&"default".to_string()));

    let driver_pod_template_config_map = pod_template_config_map(
        &spark_application,
        SparkApplicationRole::Driver,
        &driver_config,
        driver_product_config,
        &env_vars,
        &opt_s3conn,
        &s3logdir,
        vector_aggregator_address.as_deref(),
        &resolved_product_image,
    )?;
    client
        .apply_patch(
            CONTROLLER_NAME,
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
        &spark_application,
        SparkApplicationRole::Executor,
        &executor_config,
        executor_product_config,
        &env_vars,
        &opt_s3conn,
        &s3logdir,
        vector_aggregator_address.as_deref(),
        &resolved_product_image,
    )?;
    client
        .apply_patch(
            CONTROLLER_NAME,
            &executor_pod_template_config_map,
            &executor_pod_template_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job_commands = spark_application
        .build_command(
            serviceaccount.metadata.name.as_ref().unwrap(),
            &opt_s3conn,
            &s3logdir,
            &resolved_product_image.image,
        )
        .context(BuildCommandSnafu)?;

    let submit_config = spark_application
        .submit_config()
        .context(SubmitConfigSnafu)?;

    let submit_product_config: Option<&HashMap<PropertyNameKind, BTreeMap<String, String>>> =
        validated_product_config
            .get(&SparkApplicationRole::Submit.to_string())
            .and_then(|r| r.get(&"default".to_string()));

    let submit_job_config_map = submit_job_config_map(
        &spark_application,
        submit_product_config,
        &resolved_product_image,
    )?;
    client
        .apply_patch(
            CONTROLLER_NAME,
            &submit_job_config_map,
            &submit_job_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job = spark_job(
        &spark_application,
        &resolved_product_image,
        &serviceaccount,
        &env_vars,
        &job_commands,
        &opt_s3conn,
        &s3logdir,
        &submit_config,
    )?;
    client
        .apply_patch(CONTROLLER_NAME, &job, &job)
        .await
        .context(ApplyApplicationSnafu)?;

    Ok(Action::await_change())
}

fn init_containers(
    spark_application: &SparkApplication,
    logging: &Logging<SparkContainer>,
    s3conn: &Option<S3ConnectionSpec>,
    s3logdir: &Option<S3LogDir>,
    spark_image: &ResolvedProductImage,
) -> Result<Vec<Container>> {
    let mut jcb = ContainerBuilder::new(&SparkContainer::Job.to_string())
        .context(IllegalContainerNameSnafu)?;
    let job_container = spark_application.spec.image.as_ref().map(|job_image| {
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

        jcb.image(job_image)
            .command(vec!["/bin/bash".to_string(), "-c".to_string()])
            .args(vec![args.join(" && ")])
            .add_volume_mount(VOLUME_MOUNT_NAME_JOB, VOLUME_MOUNT_PATH_JOB)
            .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
            .resources(
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("250m")
                    .with_cpu_limit("500m")
                    .with_memory_request("128Mi")
                    .with_memory_limit("128Mi")
                    .build(),
            )
            .build()
    });

    let mut rcb = ContainerBuilder::new(&SparkContainer::Requirements.to_string())
        .context(IllegalContainerNameSnafu)?;
    let requirements_container = spark_application.requirements().map(|req| {
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
            .command(vec!["/bin/bash".to_string(), "-c".to_string()])
            .args(vec![args.join(" && ")])
            .add_volume_mount(VOLUME_MOUNT_NAME_REQ, VOLUME_MOUNT_PATH_REQ)
            .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
            .image_pull_policy(&spark_image.image_pull_policy);

        rcb.resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("250m")
                .with_cpu_limit("1000m")
                .with_memory_request("1024Mi")
                .with_memory_limit("1024Mi")
                .build(),
        );

        rcb.build()
    });

    // if TLS is enabled, build TrustStore and put secret inside.
    let mut tcb = ContainerBuilder::new(&SparkContainer::Tls.to_string())
        .context(IllegalContainerNameSnafu)?;
    let mut args = Vec::new();

    let tls_container = tlscerts::tls_secret_names(s3conn, s3logdir).map(|cert_secrets| {
        args.extend(tlscerts::convert_system_trust_store_to_pkcs12());
        for cert_secret in cert_secrets {
            args.extend(tlscerts::import_truststore(cert_secret));
            tcb.add_volume_mount(
                cert_secret,
                format!("{STACKABLE_MOUNT_PATH_TLS}/{cert_secret}"),
            );
        }
        tcb.image(&spark_image.image)
            .command(vec!["/bin/bash".to_string(), "-c".to_string()])
            .args(vec![args.join(" && ")])
            .add_volume_mount(STACKABLE_TRUST_STORE_NAME, STACKABLE_TRUST_STORE)
            .resources(
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("250m")
                    .with_cpu_limit("1000m")
                    .with_memory_request("1024Mi")
                    .with_memory_limit("1024Mi")
                    .build(),
            )
            .build()
    });

    Ok(vec![job_container, requirements_container, tls_container]
        .into_iter()
        .flatten()
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn pod_template(
    spark_application: &SparkApplication,
    role: SparkApplicationRole,
    config: &RoleConfig,
    volumes: &[Volume],
    env: &[EnvVar],
    s3conn: &Option<S3ConnectionSpec>,
    s3logdir: &Option<S3LogDir>,
    spark_image: &ResolvedProductImage,
) -> Result<PodTemplateSpec> {
    let container_name = SparkContainer::Spark.to_string();
    let mut cb = ContainerBuilder::new(&container_name).context(IllegalContainerNameSnafu)?;

    cb.add_volume_mounts(config.volume_mounts(spark_application, s3conn, s3logdir))
        .add_env_vars(env.to_vec())
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

    let mut pb = PodBuilder::new();
    pb.metadata(
        ObjectMetaBuilder::new()
            .name(&container_name)
            // this reference is not pointing to a controller but only provides a UID that can used to clean up resources
            // cleanly (specifically driver pods and related config maps) when the spark application is deleted.
            .ownerreference_from_resource(spark_application, None, None)
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                spark_application
                    .build_recommended_labels(&spark_image.app_version_label, &container_name),
            )
            .context(MetadataBuildSnafu)?
            .build(),
    )
    .add_container(cb.build())
    .add_volumes(volumes.to_vec())
    .security_context(security_context())
    .image_pull_secrets_from_product_image(spark_image)
    .affinity(&config.affinity);

    let init_containers = init_containers(
        spark_application,
        &config.logging,
        s3conn,
        s3logdir,
        spark_image,
    )
    .unwrap();

    for init_container in init_containers {
        pb.add_init_container(init_container.clone());
    }

    if config.logging.enable_vector_agent {
        pb.add_container(vector_container(
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
        ));
    }

    let mut pod_template = pb.build_template();
    if let Some(pod_overrides) = spark_application.pod_overrides(role) {
        pod_template.merge_from(pod_overrides);
    }
    Ok(pod_template)
}

#[allow(clippy::too_many_arguments)]
fn pod_template_config_map(
    spark_application: &SparkApplication,
    role: SparkApplicationRole,
    config: &RoleConfig,
    product_config: Option<&HashMap<PropertyNameKind, BTreeMap<String, String>>>,
    env: &[EnvVar],
    s3conn: &Option<S3ConnectionSpec>,
    s3logdir: &Option<S3LogDir>,
    vector_aggregator_address: Option<&str>,
    spark_image: &ResolvedProductImage,
) -> Result<ConfigMap> {
    let cm_name = spark_application.pod_template_config_map_name(role.clone());

    let log_config_map = if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = config.logging.containers.get(&SparkContainer::Spark)
    {
        config_map.into()
    } else {
        cm_name.clone()
    };

    let mut volumes = spark_application
        .volumes(s3conn, s3logdir, Some(&log_config_map))
        .context(CreateVolumesSnafu)?;
    volumes.push(
        VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
            .with_config_map(&cm_name)
            .build(),
    );

    let template = pod_template(
        spark_application,
        role.clone(),
        config,
        volumes.as_ref(),
        env,
        s3conn,
        s3logdir,
        spark_image,
    )?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(spark_application)
                .name(&cm_name)
                .ownerreference_from_resource(spark_application, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(
                    spark_application
                        .build_recommended_labels(&spark_image.app_version_label, "pod-templates"),
                )
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
        vector_aggregator_address,
        &config.logging,
        SparkContainer::Spark,
        SparkContainer::Vector,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu { cm_name })?;

    if let Some(product_config) = product_config {
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
    spark_application: &SparkApplication,
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
                    .build_recommended_labels(&spark_image.app_version_label, "spark-submit"),
            )
            .context(MetadataBuildSnafu)?
            .build(),
    );

    if let Some(product_config) = product_config {
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
    spark_application: &SparkApplication,
    spark_image: &ResolvedProductImage,
    serviceaccount: &ServiceAccount,
    env: &[EnvVar],
    job_commands: &[String],
    s3conn: &Option<S3ConnectionSpec>,
    s3logdir: &Option<S3LogDir>,
    job_config: &SubmitConfig,
) -> Result<Job> {
    let mut cb = ContainerBuilder::new(&SparkContainer::SparkSubmit.to_string())
        .context(IllegalContainerNameSnafu)?;

    let args = [job_commands.join(" ")];

    cb.image_from_product_image(spark_image)
        .command(vec!["/bin/bash".to_string(), "-c".to_string()])
        .args(vec![args.join(" && ")])
        .resources(job_config.resources.clone().into())
        .add_volume_mounts(spark_application.spark_job_volume_mounts(s3conn, s3logdir))
        .add_env_vars(env.to_vec())
        .add_env_var(
            "SPARK_SUBMIT_OPTS",
            format!(
                "-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"
            ),
        )
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
    volumes.extend(
        spark_application
            .volumes(s3conn, s3logdir, None)
            .context(CreateVolumesSnafu)?,
    );

    let containers = vec![cb.build()];

    let mut pod =
        PodTemplateSpec {
            metadata: Some(
                ObjectMetaBuilder::new()
                    .name("spark-submit")
                    .with_recommended_labels(spark_application.build_recommended_labels(
                        &spark_image.app_version_label,
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
                    .build_recommended_labels(&spark_image.app_version_label, "spark-job"),
            )
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(JobSpec {
            template: pod,
            ttl_seconds_after_finished: Some(600),
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
    spark_app: &SparkApplication,
    spark_image: &ResolvedProductImage,
) -> Result<(ServiceAccount, RoleBinding)> {
    let sa_name = spark_app.metadata.name.as_ref().unwrap().to_string();
    let sa = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_app)
            .name(&sa_name)
            .ownerreference_from_resource(spark_app, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                spark_app
                    .build_recommended_labels(&spark_image.app_version_label, "service-account"),
            )
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
                spark_app.build_recommended_labels(&spark_image.app_version_label, "role-binding"),
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
        run_as_user: Some(SPARK_UID),
        run_as_group: Some(0),
        fs_group: Some(1000),
        ..PodSecurityContext::default()
    }
}

pub fn error_policy(_obj: Arc<SparkApplication>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(*Duration::from_secs(5))
}
