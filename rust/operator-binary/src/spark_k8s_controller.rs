use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder};

use stackable_operator::k8s_openapi::api::batch::v1::{Job, JobSpec};
use stackable_operator::k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvVar, Pod, PodSpec,
    PodTemplateSpec, ServiceAccount, Volume, VolumeMount,
};
use stackable_operator::k8s_openapi::api::rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject};
use stackable_operator::k8s_openapi::Resource;
use stackable_operator::kube::api::ObjectMeta;
use stackable_operator::kube::runtime::controller::{Action, Context};
use stackable_operator::logging::controller::ReconcilerError;
use stackable_operator::product_config::ProductConfigManager;
use stackable_spark_k8s_crd::constants::*;
use stackable_spark_k8s_crd::SparkApplication;
use std::collections::BTreeMap;
use std::{sync::Arc, time::Duration};
use strum::{EnumDiscriminants, IntoStaticStr};

const FIELD_MANAGER_SCOPE: &str = "sparkapplication";
const SPARK_CLUSTER_ROLE: &str = "spark-driver-edit-role";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
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
    #[snafu(display("failed to update status"))]
    ApplyStatus {
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
    #[snafu(display("no job image specified"))]
    ObjectHasNoImage,
    #[snafu(display("no spark base image specified"))]
    ObjectHasNoSparkImage,
    #[snafu(display("invalid pod template"))]
    PodTemplate {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("driver pod template serialization"))]
    DriverPodTemplateSerde { source: serde_yaml::Error },
    #[snafu(display("executor pod template serialization"))]
    ExecutorPodTemplateSerde { source: serde_yaml::Error },
    #[snafu(display("s3 bucket error"))]
    S3Bucket {
        source: stackable_operator::error::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile(
    spark_application: Arc<SparkApplication>,
    ctx: Context<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let client = &ctx.get_ref().client;

    let s3bucket = match spark_application.spec.s3bucket.as_ref() {
        Some(s3bd) => s3bd
            .resolve(client, spark_application.metadata.namespace.as_deref())
            .await
            .context(S3BucketSnafu)
            .ok(),
        _ => None,
    };

    let (serviceaccount, rolebinding) = build_spark_role_serviceaccount(&spark_application)?;
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &serviceaccount, &serviceaccount)
        .await
        .context(ApplyServiceAccountSnafu)?;
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &rolebinding, &rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let spark_image = spark_application
        .spec
        .spark_image
        .as_deref()
        .context(ObjectHasNoSparkImageSnafu)?;

    let job_container = spark_application.spec.image.as_ref().map(|job_image| {
        ContainerBuilder::new(CONTAINER_NAME_JOB)
            .image(job_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                format!("cp /jobs/* {VOLUME_MOUNT_PATH_JOB}"),
            ])
            .add_volume_mount(VOLUME_MOUNT_NAME_JOB, VOLUME_MOUNT_PATH_JOB)
            .build()
    });

    let requirements_container = spark_application.requirements().map(|req| {
        let mut container_builder = ContainerBuilder::new(CONTAINER_NAME_REQ);
        container_builder
            .image(spark_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                format!("pip install --target={VOLUME_MOUNT_PATH_REQ} {req}"),
            ])
            .add_volume_mount(VOLUME_MOUNT_NAME_REQ, VOLUME_MOUNT_PATH_REQ);
        if let Some(image_pull_policy) = spark_application.spark_image_pull_policy() {
            container_builder.image_pull_policy(image_pull_policy.to_string());
        }
        container_builder.build()
    });

    let env_vars = spark_application.env(&s3bucket);
    let init_containers: Vec<Container> =
        vec![job_container.clone(), requirements_container.clone()]
            .into_iter()
            .flatten()
            .collect();
    let pod_template_config_map =
        pod_template_config_map(&spark_application, init_containers.as_ref(), &env_vars)?;
    client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &pod_template_config_map,
            &pod_template_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job_commands = spark_application
        .build_command(serviceaccount.metadata.name.as_ref().unwrap(), &s3bucket)
        .context(BuildCommandSnafu)?;

    let job = spark_job(
        &spark_application,
        spark_image,
        &serviceaccount,
        &job_container,
        &env_vars,
        &job_commands,
    )?;
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &job, &job)
        .await
        .context(ApplyApplicationSnafu)?;

    Ok(Action::await_change())
}

fn pod_template(
    spark_application: &SparkApplication,
    container_name: &str,
    init_containers: &[Container],
    volumes: &[Volume],
    volume_mounts: &[VolumeMount],
    env: &[EnvVar],
    node_selector: Option<BTreeMap<String, String>>,
) -> Result<Pod> {
    let mut container = ContainerBuilder::new(container_name);
    container
        .add_volume_mounts(volume_mounts.to_vec())
        .add_env_vars(env.to_vec());

    if let Some(image_pull_policy) = spark_application.spark_image_pull_policy() {
        container.image_pull_policy(image_pull_policy.to_string());
    }

    let mut pod_spec = PodSpec {
        containers: vec![container.build()],
        volumes: Some(volumes.to_vec()),
        ..PodSpec::default()
    };

    if !init_containers.is_empty() {
        pod_spec.init_containers = Some(init_containers.to_vec());
    }
    if let Some(image_pull_secrets) = spark_application.spark_image_pull_secrets() {
        pod_spec.image_pull_secrets = Some(image_pull_secrets);
    }
    if node_selector.is_some() {
        pod_spec.node_selector = node_selector;
    }
    Ok(Pod {
        metadata: ObjectMeta::default(),
        spec: Some(pod_spec),
        ..Pod::default()
    })
}

fn pod_template_config_map(
    spark_application: &SparkApplication,
    init_containers: &[Container],
    env: &[EnvVar],
) -> Result<ConfigMap> {
    let volumes = spark_application.volumes();

    let driver_template = pod_template(
        spark_application,
        CONTAINER_NAME_DRIVER,
        init_containers,
        volumes.as_ref(),
        spark_application.driver_volume_mounts().as_ref(),
        env,
        spark_application.driver_node_selector(),
    )?;
    let executor_template = pod_template(
        spark_application,
        CONTAINER_NAME_EXECUTOR,
        init_containers,
        volumes.as_ref(),
        spark_application.executor_volume_mounts().as_ref(),
        env,
        spark_application.executor_node_selector(),
    )?;

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(spark_application)
                .name(spark_application.pod_template_config_map_name())
                .ownerreference_from_resource(spark_application, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_labels(spark_application.recommended_labels())
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

fn spark_job(
    spark_application: &SparkApplication,
    spark_image: &str,
    serviceaccount: &ServiceAccount,
    job_container: &Option<Container>,
    env: &[EnvVar],
    job_commands: &[String],
) -> Result<Job> {
    let mut volume_mounts = vec![VolumeMount {
        name: VOLUME_MOUNT_NAME_POD_TEMPLATES.into(),
        mount_path: VOLUME_MOUNT_PATH_POD_TEMPLATES.into(),
        ..VolumeMount::default()
    }];
    volume_mounts.extend(spark_application.driver_volume_mounts());
    if job_container.is_some() {
        volume_mounts.push(VolumeMount {
            name: VOLUME_MOUNT_NAME_JOB.into(),
            mount_path: VOLUME_MOUNT_PATH_JOB.into(),
            ..VolumeMount::default()
        })
    }

    let mut container = ContainerBuilder::new("spark-submit");
    container
        .image(spark_image)
        .command(vec!["/bin/bash".to_string()])
        .args(vec![
            "-c".to_string(),
            "-x".to_string(),
            job_commands.join(" "),
        ])
        .add_volume_mounts(volume_mounts)
        .add_env_vars(env.to_vec())
        // TODO: move this to the image
        .add_env_vars(vec![EnvVar {
            name: "SPARK_CONF_DIR".to_string(),
            value: Some("/stackable/spark/conf".to_string()),
            value_from: None,
        }]);

    if let Some(image_pull_policy) = spark_application.spark_image_pull_policy() {
        container.image_pull_policy(image_pull_policy.to_string());
    }

    let mut volumes = vec![Volume {
        name: String::from(VOLUME_MOUNT_NAME_POD_TEMPLATES),
        config_map: Some(ConfigMapVolumeSource {
            name: Some(spark_application.pod_template_config_map_name()),
            ..ConfigMapVolumeSource::default()
        }),
        ..Volume::default()
    }];
    volumes.extend(spark_application.volumes());

    if job_container.is_some() {
        volumes.push(Volume {
            name: String::from(VOLUME_MOUNT_NAME_JOB),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Volume::default()
        })
    }

    let pod = PodTemplateSpec {
        metadata: Some(
            ObjectMetaBuilder::new()
                .name("spark-submit")
                .with_labels(spark_application.recommended_labels())
                .build(),
        ),
        spec: Some(PodSpec {
            containers: vec![container.build()],
            init_containers: job_container.as_ref().map(|c| vec![c.clone()]),
            restart_policy: Some("Never".to_string()),
            service_account_name: serviceaccount.metadata.name.clone(),
            volumes: Some(volumes),
            image_pull_secrets: spark_application.spark_image_pull_secrets(),
            ..PodSpec::default()
        }),
    };

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_application)
            .ownerreference_from_resource(spark_application, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_labels(spark_application.recommended_labels())
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
            .with_labels(spark_app.recommended_labels())
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
            .with_labels(spark_app.recommended_labels())
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

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
