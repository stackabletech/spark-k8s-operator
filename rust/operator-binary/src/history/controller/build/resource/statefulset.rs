use std::str::FromStr;

use snafu::{OptionExt, ResultExt};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, container::ContainerBuilder, volume::VolumeBuilder},
    },
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{PodSecurityContext, ServiceAccount},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::ResourceExt,
    product_logging::{
        framework::calculate_log_volume_size_limit,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    v2::{
        builder::pod::{
            container::{EnvVarName, EnvVarSet},
            volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
        },
        product_logging::framework::vector_container,
        types::{kubernetes::PersistentVolumeClaimName, operator::RoleGroupName},
    },
};

// PVC name for the listener volume, required by the v2 listener-volume builder. Its value matches
// `LISTENER_VOLUME_NAME` in `crd::constants`.
stackable_operator::constant!(LISTENER_VOLUME_NAME_PVC: PersistentVolumeClaimName = "listener");

use crate::{
    crd::{
        constants::{
            ACCESS_KEY_ID, HISTORY_ROLE_NAME, HISTORY_UI_PORT, LISTENER_VOLUME_DIR,
            LISTENER_VOLUME_NAME, MAX_SPARK_LOG_FILES_SIZE, METRICS_PORT, SECRET_ACCESS_KEY,
            SPARK_DEFAULTS_FILE_NAME, STACKABLE_TRUST_STORE, VECTOR_CONTAINER_NAME,
            VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_NAME_CONFIG_TYPED, VOLUME_MOUNT_NAME_LOG,
            VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_NAME_LOG_TYPED, VOLUME_MOUNT_PATH_CONFIG,
            VOLUME_MOUNT_PATH_LOG, VOLUME_MOUNT_PATH_LOG_CONFIG,
        },
        history::SparkHistoryServerContainer,
        logdir::ResolvedLogDir,
        tlscerts,
    },
    history::{
        config::jvm::construct_history_jvm_args,
        controller::{
            AddVolumeMountSnafu, AddVolumeSnafu, CreateLogDirVolumesSpecSnafu, Error,
            InvalidContainerNameSnafu, MissingSecretLifetimeSnafu,
            build::resource::listener::group_listener_name,
            validate::{self, ValidatedHistoryRoleGroup},
        },
    },
};

#[allow(clippy::result_large_err)]
pub(crate) fn build_stateful_set(
    validated: &validate::ValidatedSparkHistoryServer,
    role_group_name: &RoleGroupName,
    rg: &ValidatedHistoryRoleGroup,
    log_dir: &ResolvedLogDir,
    serviceaccount: &ServiceAccount,
) -> Result<StatefulSet, Error> {
    let resolved_product_image = &validated.resolved_product_image;
    let resource_names = validated.resource_names(role_group_name);

    let log_config_map = if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = rg
        .config
        .config
        .logging
        .containers
        .get(&SparkHistoryServerContainer::SparkHistory)
    {
        config_map.into()
    } else {
        resource_names.role_group_config_map().to_string()
    };

    let recommended_labels = validated.recommended_labels(role_group_name);

    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    let mut pb = PodBuilder::new();

    let requested_secret_lifetime = rg
        .config
        .config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    pb.service_account_name(serviceaccount.name_unchecked())
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG)
                .with_config_map(resource_names.role_group_config_map().to_string())
                .build(),
        )
        .context(AddVolumeSnafu)?
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG_CONFIG)
                .with_config_map(log_config_map)
                .build(),
        )
        .context(AddVolumeSnafu)?
        .add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG)
                .with_empty_dir(
                    None::<String>,
                    Some(calculate_log_volume_size_limit(&[MAX_SPARK_LOG_FILES_SIZE])),
                )
                .build(),
        )
        .context(AddVolumeSnafu)?
        .add_volumes(
            log_dir
                .volumes(&requested_secret_lifetime)
                .context(CreateLogDirVolumesSpecSnafu)?,
        )
        .context(AddVolumeSnafu)?
        .security_context(PodSecurityContext {
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        });

    // Base environment variables, with the already-merged (role + role group) env overrides
    // layered on top (overrides win). The base names are static and known to be valid.
    let known_env_var_name = |name: &str| {
        EnvVarName::from_str(name).expect("the operator-generated env var name is valid")
    };
    let merged_env = EnvVarSet::new()
        .with_values([
            // Needed by the `containerdebug` running in the background of the history container
            // to log it's tracing information to.
            (
                known_env_var_name("CONTAINERDEBUG_LOG_DIRECTORY"),
                format!("{VOLUME_MOUNT_PATH_LOG}/containerdebug"),
            ),
            // This env var prevents the history server from detaching itself from the
            // start script because this leads to the Pod terminating immediately.
            (known_env_var_name("SPARK_NO_DAEMONIZE"), "true".to_owned()),
            (
                known_env_var_name("SPARK_DAEMON_CLASSPATH"),
                "/stackable/spark/extra-jars/*".to_owned(),
            ),
            // JVM arguments for the history server.
            (
                known_env_var_name("SPARK_HISTORY_OPTS"),
                construct_history_jvm_args(&rg.config, log_dir),
            ),
        ])
        .merge(rg.config.env_overrides.clone());

    let container_name = "spark-history";
    let container = ContainerBuilder::new(container_name)
        .context(InvalidContainerNameSnafu)?
        .image_from_product_image(resolved_product_image)
        .resources(rg.config.config.resources.clone().into())
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(command_args(log_dir))
        .add_container_port("http", HISTORY_UI_PORT.into())
        .add_container_port("metrics", METRICS_PORT.into())
        .add_env_vars(merged_env)
        .add_volume_mounts(log_dir.volume_mounts())
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_PATH_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG_CONFIG, VOLUME_MOUNT_PATH_LOG_CONFIG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_PATH_LOG)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?
        .build();

    // Add listener volume
    // Listener endpoints for the Webserver role will use persistent volumes
    // so that load balancers can hard-code the target addresses. This will
    // be the case even when no class is set (and the value defaults to
    // cluster-internal) as the address should still be consistent.
    let volume_claim_templates = Some(vec![listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(
            group_listener_name(validated, HISTORY_ROLE_NAME)
                .parse()
                .expect("the group listener name is a valid ListenerName"),
        ),
        &recommended_labels,
        &LISTENER_VOLUME_NAME_PVC,
    )]);

    pb.add_container(container);

    if let Some(vector_log_config) = &rg.logging.vector_container {
        pb.add_container(vector_container(
            &VECTOR_CONTAINER_NAME,
            resolved_product_image,
            vector_log_config,
            &resource_names,
            &VOLUME_MOUNT_NAME_CONFIG_TYPED,
            &VOLUME_MOUNT_NAME_LOG_TYPED,
            EnvVarSet::new(),
        ));
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(rg.config.pod_overrides.clone());

    let sts_metadata = validated
        .object_meta(
            resource_names.stateful_set_name().to_string(),
            role_group_name,
        )
        .build();

    Ok(StatefulSet {
        metadata: sts_metadata,
        spec: Some(StatefulSetSpec {
            template: pod_template,
            volume_claim_templates,
            replicas: Some(i32::from(rg.config.replicas)),
            selector: LabelSelector {
                match_labels: Some(validated.role_group_selector(role_group_name).into()),
                ..LabelSelector::default()
            },
            ..StatefulSetSpec::default()
        }),
        ..StatefulSet::default()
    })
}

fn command_args(logdir: &ResolvedLogDir) -> Vec<String> {
    let mut command = vec![];

    if let Some(secret_dir) = logdir.credentials_mount_path() {
        command.extend(vec![
            format!("export AWS_ACCESS_KEY_ID=\"$(cat {secret_dir}/{ACCESS_KEY_ID})\""),
            format!("export AWS_SECRET_ACCESS_KEY=\"$(cat {secret_dir}/{SECRET_ACCESS_KEY})\""),
        ]);
    }

    if let Some(secret_name) = logdir.tls_secret_name() {
        command.push(format!("mkdir -p {STACKABLE_TRUST_STORE}"));
        command.push(tlscerts::convert_system_trust_store_to_pkcs12());
        command.push(tlscerts::import_truststore(secret_name));
    }

    command.extend(vec![
        format!("containerdebug --output={VOLUME_MOUNT_PATH_LOG}/containerdebug-state.json --loop &"),
        format!("/stackable/spark/sbin/start-history-server.sh --properties-file {VOLUME_MOUNT_PATH_CONFIG}/{SPARK_DEFAULTS_FILE_NAME}"),
    ]);
    vec![command.join("\n")]
}
