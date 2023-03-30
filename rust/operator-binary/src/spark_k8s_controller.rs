use std::{sync::Arc, time::Duration};

use stackable_spark_k8s_crd::{
    constants::*, s3logdir::S3LogDir, SparkApplication, SparkApplicationRole,
};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    commons::{
        affinity::StackableAffinity,
        s3::S3ConnectionSpec,
        tls::{CaCert, TlsVerification},
    },
    k8s_openapi::{
        api::{
            batch::v1::{Job, JobSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, Container, EnvVar, Pod, PodSecurityContext,
                PodSpec, PodTemplateSpec, ServiceAccount, Volume, VolumeMount,
            },
            rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
        Resource,
    },
    kube::runtime::controller::Action,
    logging::controller::ReconcilerError,
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply role ServiceAccount"))]
    ApplyServiceAccount {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply global RoleBinding"))]
    ApplyRoleBinding {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Job"))]
    ApplyApplication {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build stark-submit command"))]
    BuildCommand {
        source: stackable_spark_k8s_crd::Error,
    },
    #[snafu(display("failed to build the pod template config map"))]
    PodTemplateConfigMap {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("no spark base image specified"))]
    ObjectHasNoSparkImage,
    #[snafu(display("driver pod template serialization"))]
    DriverPodTemplateSerde { source: serde_yaml::Error },
    #[snafu(display("executor pod template serialization"))]
    ExecutorPodTemplateSerde { source: serde_yaml::Error },
    #[snafu(display("s3 bucket error"))]
    S3Bucket {
        source: stackable_operator::error::Error,
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
    #[snafu(display("illegal container name: [{container_name}]"))]
    IllegalContainerName {
        source: stackable_operator::error::Error,
        container_name: String,
    },
    #[snafu(display("failed to resolve the s3 log dir configuration"))]
    S3LogDir {
        source: stackable_spark_k8s_crd::s3logdir::Error,
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

    if let Some(conn) = opt_s3conn.as_ref() {
        if let Some(tls) = &conn.tls {
            match &tls.verification {
                TlsVerification::None {} => return S3TlsNoVerificationNotSupportedSnafu.fail(),
                TlsVerification::Server(server_verification) => {
                    match &server_verification.ca_cert {
                        CaCert::WebPki {} => {}
                        CaCert::SecretClass(_) => {
                            return S3TlsCaVerificationNotSupportedSnafu.fail()
                        }
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

    let (serviceaccount, rolebinding) = build_spark_role_serviceaccount(&spark_application)?;
    client
        .apply_patch(CONTROLLER_NAME, &serviceaccount, &serviceaccount)
        .await
        .context(ApplyServiceAccountSnafu)?;
    client
        .apply_patch(CONTROLLER_NAME, &rolebinding, &rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let spark_image = spark_application
        .spec
        .spark_image
        .as_deref()
        .context(ObjectHasNoSparkImageSnafu)?;

    let mut jcb =
        ContainerBuilder::new(CONTAINER_NAME_JOB).with_context(|_| IllegalContainerNameSnafu {
            container_name: APP_NAME.to_string(),
        })?;
    let job_container = spark_application.spec.image.as_ref().map(|job_image| {
        jcb.image(job_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                format!("cp /jobs/* {VOLUME_MOUNT_PATH_JOB}"),
            ])
            .add_volume_mount(VOLUME_MOUNT_NAME_JOB, VOLUME_MOUNT_PATH_JOB)
            .build()
    });

    let mut rcb =
        ContainerBuilder::new(CONTAINER_NAME_REQ).with_context(|_| IllegalContainerNameSnafu {
            container_name: APP_NAME.to_string(),
        })?;
    let requirements_container = spark_application.requirements().map(|req| {
        rcb.image(spark_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                format!("pip install --target={VOLUME_MOUNT_PATH_REQ} {req}"),
            ])
            .add_volume_mount(VOLUME_MOUNT_NAME_REQ, VOLUME_MOUNT_PATH_REQ);
        if let Some(image_pull_policy) = spark_application.spark_image_pull_policy() {
            rcb.image_pull_policy(image_pull_policy.to_string());
        }
        rcb.build()
    });

    let env_vars = spark_application.env();
    let init_containers: Vec<Container> =
        vec![job_container.clone(), requirements_container.clone()]
            .into_iter()
            .flatten()
            .collect();
    let pod_template_config_map = pod_template_config_map(
        &spark_application,
        init_containers.as_ref(),
        &env_vars,
        &opt_s3conn,
        &s3logdir,
    )?;
    client
        .apply_patch(
            CONTROLLER_NAME,
            &pod_template_config_map,
            &pod_template_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job_commands = spark_application
        .build_command(
            serviceaccount.metadata.name.as_ref().unwrap(),
            &opt_s3conn,
            &s3logdir,
        )
        .context(BuildCommandSnafu)?;

    let job = spark_job(
        &spark_application,
        spark_image,
        &serviceaccount,
        &job_container,
        &env_vars,
        &job_commands,
        &opt_s3conn,
        &s3logdir,
    )?;
    client
        .apply_patch(CONTROLLER_NAME, &job, &job)
        .await
        .context(ApplyApplicationSnafu)?;

    Ok(Action::await_change())
}

#[allow(clippy::too_many_arguments)]
fn pod_template(
    spark_application: &SparkApplication,
    container_name: &str,
    init_containers: &[Container],
    volumes: &[Volume],
    volume_mounts: &[VolumeMount],
    env: &[EnvVar],
    affinity: Option<StackableAffinity>,
) -> Result<Pod> {
    // N.B. this may be ignored by spark as preference is given to spark
    // configuration settings.
    let resources = match container_name {
        CONTAINER_NAME_DRIVER => {
            spark_application
                .driver_config()
                .context(FailedToResolveConfigSnafu)?
                .resources
        }
        CONTAINER_NAME_EXECUTOR => {
            spark_application
                .executor_config()
                .context(FailedToResolveConfigSnafu)?
                .resources
        }
        _ => return UnrecognisedContainerNameSnafu.fail(),
    };

    let mut cb =
        ContainerBuilder::new(container_name).with_context(|_| IllegalContainerNameSnafu {
            container_name: APP_NAME.to_string(),
        })?;
    cb.add_volume_mounts(volume_mounts.to_vec())
        .add_env_vars(env.to_vec())
        .resources(resources.into());

    if let Some(image_pull_policy) = spark_application.spark_image_pull_policy() {
        cb.image_pull_policy(image_pull_policy.to_string());
    }

    let mut pb = PodBuilder::new();
    pb.metadata(
        ObjectMetaBuilder::new()
            .name(container_name)
            // this reference is not pointing to a controller but only provides a UID that can used to clean up resources
            // cleanly (specifically driver pods and related config maps) when the spark application is deleted.
            .ownerreference_from_resource(spark_application, None, None)
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(spark_application.build_recommended_labels(container_name))
            .build(),
    )
    .add_container(cb.build())
    .add_volumes(volumes.to_vec())
    .security_context(security_context());

    if let Some(affinity) = affinity {
        pb.affinity(&affinity);
    }

    for init_container in init_containers {
        pb.add_init_container(init_container.clone());
    }

    if let Some(image_pull_secrets) = spark_application.spark_image_pull_secrets() {
        pb.image_pull_secrets(
            image_pull_secrets
                .iter()
                .flat_map(|secret| secret.name.clone()),
        );
    }

    pb.build().context(PodTemplateConfigMapSnafu)
}

fn pod_template_config_map(
    spark_application: &SparkApplication,
    init_containers: &[Container],
    env: &[EnvVar],
    s3conn: &Option<S3ConnectionSpec>,
    s3logdir: &Option<S3LogDir>,
) -> Result<ConfigMap> {
    let volumes = spark_application.volumes(s3conn, s3logdir);

    let driver_template = pod_template(
        spark_application,
        CONTAINER_NAME_DRIVER,
        init_containers,
        volumes.as_ref(),
        spark_application
            .driver_volume_mounts(s3conn, s3logdir)
            .as_ref(),
        env,
        spark_application
            .affinity(SparkApplicationRole::Driver)
            .ok(),
    )?;
    let executor_template = pod_template(
        spark_application,
        CONTAINER_NAME_EXECUTOR,
        init_containers,
        volumes.as_ref(),
        spark_application
            .executor_volume_mounts(s3conn, s3logdir)
            .as_ref(),
        env,
        spark_application
            .affinity(SparkApplicationRole::Executor)
            .ok(),
    )?;

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(spark_application)
                .name(spark_application.pod_template_config_map_name())
                .ownerreference_from_resource(spark_application, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(
                    spark_application.build_recommended_labels("pod-templates"),
                )
                .build(),
        )
        .add_data(
            "driver.yml",
            serde_yaml::to_string(&driver_template).context(DriverPodTemplateSerdeSnafu)?,
        )
        .add_data(
            "executor.yml",
            serde_yaml::to_string(&executor_template).context(ExecutorPodTemplateSerdeSnafu)?,
        )
        .build()
        .context(PodTemplateConfigMapSnafu)
}

#[allow(clippy::too_many_arguments)]
fn spark_job(
    spark_application: &SparkApplication,
    spark_image: &str,
    serviceaccount: &ServiceAccount,
    job_container: &Option<Container>,
    env: &[EnvVar],
    job_commands: &[String],
    s3conn: &Option<S3ConnectionSpec>,
    s3logdir: &Option<S3LogDir>,
) -> Result<Job> {
    let mut volume_mounts = vec![VolumeMount {
        name: VOLUME_MOUNT_NAME_POD_TEMPLATES.into(),
        mount_path: VOLUME_MOUNT_PATH_POD_TEMPLATES.into(),
        ..VolumeMount::default()
    }];
    volume_mounts.extend(spark_application.driver_volume_mounts(s3conn, s3logdir));

    let mut cb =
        ContainerBuilder::new("spark-submit").with_context(|_| IllegalContainerNameSnafu {
            container_name: APP_NAME.to_string(),
        })?;
    let job_config = spark_application
        .job_config()
        .context(FailedToResolveConfigSnafu)?;

    cb.image(spark_image)
        .command(vec!["/bin/sh".to_string()])
        .resources(job_config.resources.into())
        .args(vec!["-c".to_string(), job_commands.join(" ")])
        .add_volume_mounts(volume_mounts)
        .add_env_vars(env.to_vec())
        // TODO: move this to the image
        .add_env_vars(vec![EnvVar {
            name: "SPARK_CONF_DIR".to_string(),
            value: Some("/stackable/spark/conf".to_string()),
            value_from: None,
        }]);

    if let Some(image_pull_policy) = spark_application.spark_image_pull_policy() {
        cb.image_pull_policy(image_pull_policy.to_string());
    }

    let mut volumes = vec![Volume {
        name: String::from(VOLUME_MOUNT_NAME_POD_TEMPLATES),
        config_map: Some(ConfigMapVolumeSource {
            name: Some(spark_application.pod_template_config_map_name()),
            ..ConfigMapVolumeSource::default()
        }),
        ..Volume::default()
    }];
    volumes.extend(spark_application.volumes(s3conn, s3logdir));

    let pod = PodTemplateSpec {
        metadata: Some(
            ObjectMetaBuilder::new()
                .name("spark-submit")
                .with_recommended_labels(
                    spark_application.build_recommended_labels("spark-job-template"),
                )
                .build(),
        ),
        spec: Some(PodSpec {
            containers: vec![cb.build()],
            init_containers: job_container.as_ref().map(|c| vec![c.clone()]),
            restart_policy: Some("Never".to_string()),
            service_account_name: serviceaccount.metadata.name.clone(),
            volumes: Some(volumes),
            image_pull_secrets: spark_application.spark_image_pull_secrets(),
            security_context: Some(security_context()),
            ..PodSpec::default()
        }),
    };

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_application)
            .ownerreference_from_resource(spark_application, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(spark_application.build_recommended_labels("spark-job"))
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
) -> Result<(ServiceAccount, RoleBinding)> {
    let sa_name = spark_app.metadata.name.as_ref().unwrap().to_string();
    let sa = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_app)
            .name(&sa_name)
            .ownerreference_from_resource(spark_app, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(spark_app.build_recommended_labels("service-account"))
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
            .with_recommended_labels(spark_app.build_recommended_labels("role-binding"))
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
        run_as_user: Some(1000),
        run_as_group: Some(1000),
        fs_group: Some(1000),
        ..PodSecurityContext::default()
    }
}

pub fn error_policy(_obj: Arc<SparkApplication>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
