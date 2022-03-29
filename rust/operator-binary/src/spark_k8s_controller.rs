use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::builder::{
    ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder,
};
use stackable_operator::k8s_openapi::api::batch::v1::{Job, JobSpec};
use stackable_operator::k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvVar, Pod, PodSpec,
    PodTemplateSpec, Volume, VolumeMount,
};
use stackable_operator::logging::controller::ReconcilerError;
use stackable_operator::{
    kube::runtime::controller::{Action, Context},
    product_config::ProductConfigManager,
};
use stackable_spark_k8s_crd::constants::*;
use stackable_spark_k8s_crd::SparkApplication;
use std::{sync::Arc, time::Duration};
use strum::{EnumDiscriminants, IntoStaticStr};

const FIELD_MANAGER_SCOPE: &str = "sparkapplication";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
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
        ContainerBuilder::new(CONTAINER_NAME_REQ)
            .image(spark_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                format!("pip install --target={VOLUME_MOUNT_PATH_REQ} {req}"),
            ])
            .add_volume_mount(VOLUME_MOUNT_NAME_REQ, VOLUME_MOUNT_PATH_REQ)
            .build()
    });

    let pod_template_config_map =
        pod_template_config_map(&spark_application, &job_container, &requirements_container)?;
    client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &pod_template_config_map,
            &pod_template_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job = spark_job(&spark_application, spark_image, &job_container)?;
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &job, &job)
        .await
        .context(ApplyApplicationSnafu)?;

    Ok(Action::await_change())
}

fn pod_template(
    container_name: &str,
    job_container: &Option<Container>,
    requirements_container: &Option<Container>,
    volumes: &[Volume],
    volume_mounts: &[VolumeMount],
    env: &[EnvVar],
) -> Result<Pod> {
    let mut container = ContainerBuilder::new(container_name);
    container
        .add_volume_mounts(volume_mounts.to_vec())
        .add_env_vars(env.to_vec());
    if job_container.is_some() {
        container.add_volume_mount(VOLUME_MOUNT_NAME_JOB, VOLUME_MOUNT_PATH_JOB);
    }

    if requirements_container.is_some() {
        container
            .add_volume_mount(VOLUME_MOUNT_NAME_REQ, VOLUME_MOUNT_PATH_REQ)
            .add_env_var(
                "PYTHONPATH",
                format!("$SPARK_HOME/python:{VOLUME_MOUNT_PATH_REQ}:$PYTHONPATH"),
            );
    }

    let mut template = PodBuilder::new();
    template
        .metadata_default()
        .add_container(container.build())
        .add_volumes(volumes.to_vec());

    if let Some(container) = requirements_container.clone() {
        template.add_init_container(container);
        template.add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_REQ)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
    }
    if let Some(container) = job_container.clone() {
        template.add_init_container(container);
        template.add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_JOB)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
    }
    template.build().context(PodTemplateSnafu)
}

fn pod_template_config_map(
    spark_application: &SparkApplication,
    job_container: &Option<Container>,
    requirements_container: &Option<Container>,
) -> Result<ConfigMap> {
    let volumes = spark_application.volumes();

    let driver_template = pod_template(
        CONTAINER_NAME_DRIVER,
        job_container,
        requirements_container,
        volumes.as_ref(),
        spark_application.driver_volume_mounts().as_ref(),
        spark_application.env().as_ref(),
    )?;
    let executor_template = pod_template(
        CONTAINER_NAME_EXECUTOR,
        job_container,
        requirements_container,
        volumes.as_ref(),
        spark_application.executor_volume_mounts().as_ref(),
        spark_application.env().as_ref(),
    )?;

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(spark_application)
                .name(spark_application.pod_template_config_map_name())
                .ownerreference_from_resource(spark_application, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
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
    job_container: &Option<Container>,
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

    let commands = spark_application
        .build_command()
        .context(BuildCommandSnafu)?;

    let mut container = ContainerBuilder::new("spark-submit");
    container
        .image(spark_image)
        .command(vec!["/bin/bash".to_string()])
        .args(vec!["-c".to_string(), "-x".to_string(), commands.join(" ")])
        .add_volume_mounts(volume_mounts)
        .add_env_vars(spark_application.env())
        // TODO: move this to the image
        .add_env_vars(vec![EnvVar {
            name: "SPARK_CONF_DIR".to_string(),
            value: Some("/stackable/spark/conf".to_string()),
            value_from: None,
        }]);

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
        metadata: Some(ObjectMetaBuilder::new().name("spark-submit").build()),
        spec: Some(PodSpec {
            containers: vec![container.build()],
            init_containers: job_container.as_ref().map(|c| vec![c.clone()]),
            restart_policy: Some("Never".to_string()),
            volumes: Some(volumes),
            ..PodSpec::default()
        }),
    };

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_application)
            .ownerreference_from_resource(spark_application, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
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

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

#[cfg(test)]
mod tests {
    use crate::spark_k8s_controller::pod_template_config_map;
    use crate::spark_k8s_controller::spark_job;
    use crate::SparkApplication;

    #[test]
    fn test_pod_config_map() {
        // N.B. have to include the uid explicitly here in the test (other than letting it be generated)
        let spark_application = serde_yaml::from_str::<SparkApplication>(
        r#"
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: spark-examples-s3
  namespace: default
  uid: "b4952dc3-d670-11e5-8cd0-68f728db1985"
spec:
  version: "1.0"
  sparkImage: docker.stackable.tech/stackable/spark-k8s:3.2.1-hadoop3.2-python39-aws1.11.375-stackable0.3.0
  mode: cluster
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: s3a://stackable-spark-k8s-jars/jobs/spark-examples_2.12-3.2.1.jar
  sparkConf:
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
  executor:
    cores: 1
    instances: 3
    memory: "512m"
  config:
    enableMonitoring: true
        "#).unwrap();

        let pod_template_config_map =
            pod_template_config_map(&spark_application, &None, &None).unwrap();

        assert!(&pod_template_config_map.binary_data.is_none());
        assert_eq!(
            Some(2),
            pod_template_config_map.clone().data.map(|d| d.keys().len())
        );
        assert_eq!(
            Some("b4952dc3-d670-11e5-8cd0-68f728db1985".to_string()),
            pod_template_config_map
                .metadata
                .owner_references
                .map(|r| r[0].uid.to_string())
        );
    }

    #[test]
    fn test_job() {
        // N.B. uid provided explicitly as in previous test
        let spark_application = serde_yaml::from_str::<SparkApplication>(
            r#"
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: spark-examples-s3
  namespace: default
  uid: "b4952dc3-d670-11e5-8cd0-68f728db1985"
spec:
  version: "1.0"
  sparkImage: docker.stackable.tech/stackable/spark-k8s:3.2.1-hadoop3.2-python39-aws1.11.375-stackable0.3.0
  mode: cluster
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: s3a://stackable-spark-k8s-jars/jobs/spark-examples_2.12-3.2.1.jar
  sparkConf:
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
  executor:
    cores: 1
    instances: 3
    memory: "512m"
            "#).unwrap();

        let spark_image = spark_application.spec.spark_image.as_ref().unwrap();
        let job = spark_job(&spark_application, spark_image, &None).unwrap();
        let job_containers = &job
            .clone()
            .spec
            .expect("no job spec found!")
            .template
            .spec
            .expect("no template spec found!")
            .containers;
        assert_eq!(1, job_containers.len());

        let job_args = &job_containers[0].args.clone().expect("no job args found!");
        assert_eq!(3, job_args.len());
        let spark_submit_cmd = &job_args[2];

        assert_eq!(
            Some("s3a://stackable-spark-k8s-jars/jobs/spark-examples_2.12-3.2.1.jar"),
            spark_submit_cmd.split_whitespace().rev().next()
        );

        assert_eq!(
            Some("b4952dc3-d670-11e5-8cd0-68f728db1985".to_string()),
            job.metadata.owner_references.map(|r| r[0].uid.to_string())
        );
    }
}
