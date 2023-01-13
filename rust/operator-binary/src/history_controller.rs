use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder},
    commons::{
        product_image_selection::ResolvedProductImage,
        resources::{NoRuntimeLimits, Resources},
        s3::{InlinedS3BucketSpec, S3AccessStyle, S3ConnectionSpec},
        secret_class::SecretClassVolume,
        tls::{CaCert, TlsVerification},
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, PodSecurityContext, Service, ServiceAccount, ServicePort, ServiceSpec,
                Volume, VolumeMount,
            },
            rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
        Resource,
    },
    kube::runtime::{controller::Action, reflector::ObjectRef},
    labels::{role_group_selector_labels, role_selector_labels, ObjectLabels},
    product_config::ProductConfigManager,
    role_utils::RoleGroupRef,
};
use stackable_spark_k8s_crd::{
    constants::*,
    history::{
        HistoryStorageConfig, LogFileDirectorySpec::S3, S3LogFileDirectorySpec, SparkHistoryServer,
    },
};
use std::time::Duration;
use std::{collections::BTreeMap, sync::Arc};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::logging::controller::ReconcilerError;
use strum::{EnumDiscriminants, IntoStaticStr};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("invalid config map {name}"))]
    InvalidConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("invalid history container name {name}"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update the history server deployment"))]
    ApplyDeployment {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update history server config map"))]
    ApplyConfigMap {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update history server service"))]
    ApplyService {
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
    #[snafu(display("product config validation failed"))]
    ProductConfigValidation {
        source: stackable_spark_k8s_crd::history::Error,
    },
    #[snafu(display("s3 bucket error"))]
    S3Bucket {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("tls non-verification not supported"))]
    S3TlsNoVerificationNotSupported,
    #[snafu(display("ca-cert verification not supported"))]
    S3TlsCaVerificationNotSupported,
    #[snafu(display("missing bucket name for history logs"))]
    BucketNameMissing,
    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig {
        source: stackable_spark_k8s_crd::history::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}
/// Updates the status of the SparkApplication that started the pod.
pub async fn reconcile(shs: Arc<SparkHistoryServer>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile history server");

    let client = &ctx.client;

    let resolved_product_image = shs.spec.image.resolve(HISTORY_IMAGE_BASE_NAME);
    let s3_log_dir = S3LogDir::resolve(&shs, client).await?;

    // TODO: (RBAC) need to use a dedicated service account, role
    let (serviceaccount, rolebinding) =
        build_history_role_serviceaccount(&shs, &resolved_product_image.app_version_label)?;

    client
        .apply_patch(HISTORY_CONTROLLER_NAME, &serviceaccount, &serviceaccount)
        .await
        .context(ApplyServiceAccountSnafu)?;
    client
        .apply_patch(HISTORY_CONTROLLER_NAME, &rolebinding, &rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    // The role_name is always HISTORY_ROLE_NAME
    for (role_name, role_config) in shs
        .validated_role_config(&resolved_product_image, &ctx.product_config)
        .context(ProductConfigValidationSnafu)?
        .iter()
    {
        let service = build_service(
            &shs,
            &resolved_product_image.app_version_label,
            role_name,
            None,
        )?;
        client
            .apply_patch(HISTORY_CONTROLLER_NAME, &service, &service)
            .await
            .context(ApplyServiceSnafu)?;

        for (rolegroup_name, _rolegroup_config) in role_config.iter() {
            let rgr = RoleGroupRef {
                cluster: ObjectRef::from_obj(&*shs),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let config = shs
                .merged_config(&rgr)
                .context(FailedToResolveConfigSnafu)?;

            let service = build_service(
                &shs,
                &resolved_product_image.app_version_label,
                role_name,
                Some(&rgr),
            )?;
            client
                .apply_patch(HISTORY_CONTROLLER_NAME, &service, &service)
                .await
                .context(ApplyServiceSnafu)?;

            let config_map = build_config_map(
                &shs,
                &resolved_product_image.app_version_label,
                &rgr,
                &s3_log_dir,
            )?;
            client
                .apply_patch(HISTORY_CONTROLLER_NAME, &config_map, &config_map)
                .await
                .context(ApplyConfigMapSnafu)?;

            let sts = build_stateful_set(
                &shs,
                &resolved_product_image,
                &rgr,
                &s3_log_dir,
                &config.resources,
            )?;
            client
                .apply_patch(HISTORY_CONTROLLER_NAME, &sts, &sts)
                .await
                .context(ApplyDeploymentSnafu)?;
        }
    }

    Ok(Action::await_change())
}

pub fn error_policy(_obj: Arc<SparkHistoryServer>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

fn build_config_map(
    shs: &SparkHistoryServer,
    app_version_label: &str,
    rolegroupref: &RoleGroupRef<SparkHistoryServer>,
    s3_log_dir: &S3LogDir,
) -> Result<ConfigMap, Error> {
    let spark_config = spark_config(shs, s3_log_dir);

    let result = ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(shs)
                .name(rolegroupref.object_name())
                .ownerreference_from_resource(shs, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(labels(shs, app_version_label, &rolegroupref.role_group))
                .build(),
        )
        .add_data(HISTORY_CONFIG_FILE_NAME, spark_config)
        .build()
        .context(InvalidConfigMapSnafu {
            name: String::from("spark-history-config"),
        })?;

    Ok(result)
}

fn build_stateful_set(
    shs: &SparkHistoryServer,
    resolved_product_image: &ResolvedProductImage,
    rolegroupref: &RoleGroupRef<SparkHistoryServer>,
    s3_log_dir: &S3LogDir,
    resources: &Resources<HistoryStorageConfig, NoRuntimeLimits>,
) -> Result<StatefulSet, Error> {
    let container_name = "spark-history";
    let container = ContainerBuilder::new(container_name)
        .context(InvalidContainerNameSnafu {
            name: String::from(container_name),
        })?
        .image(resolved_product_image.image.clone())
        .resources(resources.clone().into())
        .command(vec!["/bin/bash".to_string()])
        .args(command_args(s3_log_dir))
        .add_container_port("http", 18080)
        // This env var prevents the history server from detaching itself from the
        // start script because this leads to the Pod terminating immediately.
        .add_env_var("SPARK_NO_DAEMONIZE", "true")
        .add_volume_mounts(s3_log_dir.credentials_volume_mount().into_iter())
        .add_volume_mount("config", "/stackable/spark/conf")
        .build();

    let template = PodBuilder::new()
        .add_container(container)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(
            VolumeBuilder::new("config")
                .with_config_map(rolegroupref.object_name())
                .build(),
        )
        .add_volumes(s3_log_dir.credentials_volume().into_iter().collect())
        .metadata_builder(|m| {
            m.with_recommended_labels(labels(
                shs,
                &resolved_product_image.app_version_label,
                &rolegroupref.role_group,
            ))
        })
        .security_context(PodSecurityContext {
            run_as_user: Some(1000),
            run_as_group: Some(1000),
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        })
        .build_template();

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(
                shs,
                &resolved_product_image.app_version_label,
                rolegroupref.role_group.as_ref(),
            ))
            .build(),
        spec: Some(StatefulSetSpec {
            template,
            replicas: shs.replicas(rolegroupref),
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    shs,
                    APP_NAME,
                    &rolegroupref.role,
                    &rolegroupref.role_group,
                )),
                ..LabelSelector::default()
            },
            ..StatefulSetSpec::default()
        }),
        ..StatefulSet::default()
    })
}

fn build_service(
    shs: &SparkHistoryServer,
    app_version_label: &str,
    role: &str,
    group: Option<&RoleGroupRef<SparkHistoryServer>>,
) -> Result<Service, Error> {
    let group_name = match group {
        Some(rgr) => rgr.role_group.clone(),
        None => "global".to_owned(),
    };

    let service_name = match group {
        Some(rgr) => rgr.object_name(),
        None => format!(
            "{}-{}",
            shs.metadata.name.as_ref().unwrap_or(&APP_NAME.to_string()),
            role
        ),
    };

    let selector = match group {
        Some(rgr) => role_group_selector_labels(shs, APP_NAME, &rgr.role, &rgr.role_group),
        None => role_selector_labels(shs, APP_NAME, role),
    };

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .name(service_name)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(shs, app_version_label, &group_name))
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some(String::from("http")),
                port: 18080,
                ..ServicePort::default()
            }]),
            selector: Some(selector),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

fn build_history_role_serviceaccount(
    shs: &SparkHistoryServer,
    app_version_label: &str,
) -> Result<(ServiceAccount, RoleBinding)> {
    let sa_name = shs.metadata.name.as_ref().unwrap().to_string();
    let sa = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .name(&sa_name)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(shs, app_version_label, HISTORY_CONTROLLER_NAME))
            .build(),
        ..ServiceAccount::default()
    };
    let binding_name = &sa_name;
    let binding = RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .name(binding_name)
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(labels(shs, app_version_label, HISTORY_CONTROLLER_NAME))
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

struct S3LogDir {
    bucket: InlinedS3BucketSpec,
    prefix: String,
}

impl S3LogDir {
    async fn resolve(
        shs: &SparkHistoryServer,
        client: &stackable_operator::client::Client,
    ) -> Result<S3LogDir, Error> {
        #[allow(irrefutable_let_patterns)]
        let (s3bucket, prefix) =
            if let S3(S3LogFileDirectorySpec { bucket, prefix }) = &shs.spec.log_file_directory {
                (
                    bucket
                        .resolve(client, shs.metadata.namespace.as_deref().unwrap())
                        .await
                        .context(S3BucketSnafu)
                        .ok(),
                    prefix.clone(),
                )
            } else {
                (None, "".to_string())
            };

        // Check that a bucket name has been defined
        s3bucket
            .as_ref()
            .and_then(|i| i.bucket_name.as_ref())
            .context(BucketNameMissingSnafu)?;

        if let Some(conn) = s3bucket.as_ref().and_then(|i| i.connection.as_ref()) {
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

        if let Some(conn) = s3bucket.as_ref().and_then(|i| i.connection.as_ref()) {
            if conn.tls.as_ref().is_some() {
                tracing::warn!("The resource indicates S3-access should use TLS: TLS-verification has not yet been implemented \
            but an HTTPS-endpoint will be used!");
            }
        }
        Ok(S3LogDir {
            bucket: s3bucket.unwrap(),
            prefix,
        })
    }

    /// Constructs the properties needed for loading event logs from S3.
    /// These properties are later written in the `HISTORY_CONFIG_FILE_NAME_FULL` file.
    ///
    /// The following properties related to credentials are not included:
    /// * spark.hadoop.fs.s3a.aws.credentials.provider
    /// * spark.hadoop.fs.s3a.access.key
    /// * spark.hadoop.fs.s3a.secret.key
    /// instead, the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set
    /// on the container start command.
    fn spark_config(&self) -> BTreeMap<String, String> {
        let mut result = BTreeMap::new();

        result.insert(
            "spark.history.fs.logDirectory".to_string(),
            format!(
                "s3a://{}/{}",
                self.bucket.bucket_name.as_ref().unwrap().clone(), // this is guaranteed to exist at this point
                self.prefix
            ),
        );

        if let Some(endpoint) = self.bucket.endpoint() {
            result.insert("spark.hadoop.fs.s3a.endpoint".to_string(), endpoint);
        }

        if let Some(conn) = self.bucket.connection.as_ref() {
            if let Some(S3AccessStyle::Path) = conn.access_style {
                result.insert(
                    "spark.hadoop.fs.s3a.path.style.access".to_string(),
                    "true".to_string(),
                );
            }
        }
        result
    }

    fn credentials_volume(&self) -> Option<Volume> {
        self.credentials()
            .map(|credentials| credentials.to_volume(VOLUME_NAME_S3_CREDENTIALS))
    }

    fn credentials_volume_mount(&self) -> Option<VolumeMount> {
        self.credentials().map(|_| VolumeMount {
            name: VOLUME_NAME_S3_CREDENTIALS.into(),
            mount_path: S3_SECRET_DIR_NAME.into(),
            ..VolumeMount::default()
        })
    }

    fn credentials(&self) -> Option<SecretClassVolume> {
        if let Some(&S3ConnectionSpec {
            credentials: Some(ref credentials),
            ..
        }) = self.bucket.connection.as_ref()
        {
            Some(credentials.clone())
        } else {
            None
        }
    }
}

fn spark_config(shs: &SparkHistoryServer, s3_log_dir: &S3LogDir) -> String {
    let empty = BTreeMap::new();

    let mut log_dir_settings = s3_log_dir.spark_config();

    tracing::debug!("log_dir_settings: {:?}", log_dir_settings);

    // Add user provided configuration. These can overwrite the "log_file_directory" settings.
    let user_settings = shs.spec.spark_conf.as_ref().unwrap_or(&empty);

    tracing::debug!("user_settings: {:?}", user_settings);

    log_dir_settings.extend(user_settings.clone().into_iter());

    tracing::debug!("merged settings: {:?}", log_dir_settings);

    log_dir_settings
        .iter()
        .map(|(k, v)| format!("{k} {v}"))
        .collect::<Vec<String>>()
        .join("\n")
}

fn command_args(s3logdir: &S3LogDir) -> Vec<String> {
    let mut command = vec![];

    if s3logdir.credentials().is_some() {
        command.extend(vec![
            format!("export AWS_ACCESS_KEY_ID=$(cat {S3_SECRET_DIR_NAME}/{ACCESS_KEY_ID})"),
            "&&".to_string(),
            format!("export AWS_SECRET_ACCESS_KEY=$(cat {S3_SECRET_DIR_NAME}/{SECRET_ACCESS_KEY})"),
            "&&".to_string(),
        ]);
    }
    command.extend(vec![
        "/stackable/spark/sbin/start-history-server.sh".to_string(),
        "--properties-file".to_string(),
        HISTORY_CONFIG_FILE_NAME_FULL.to_string(),
    ]);

    vec![String::from("-c"), command.join(" ")]
}

fn labels<'a, T>(
    shs: &'a T,
    app_version_label: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner: shs,
        app_name: APP_NAME,
        app_version: app_version_label,
        operator_name: OPERATOR_NAME,
        controller_name: HISTORY_CONTROLLER_NAME,
        role: HISTORY_ROLE_NAME,
        role_group,
    }
}
