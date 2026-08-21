use std::str::FromStr;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, security::PodSecurityContextBuilder, volume::VolumeBuilder},
    },
    constant,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::apps::v1::{StatefulSet, StatefulSetSpec},
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    product_logging::{
        framework::calculate_log_volume_size_limit,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    v2::{
        builder::pod::{
            container::{EnvVarName, EnvVarSet, new_container_builder},
            volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
        },
        product_logging::framework::vector_container,
        types::{kubernetes::PersistentVolumeClaimName, operator::RoleGroupName},
    },
};

// PVC name for the listener volume, required by the v2 listener-volume builder. Its value matches
// `LISTENER_VOLUME_NAME` in `crd::constants`.
constant!(LISTENER_VOLUME_NAME_PVC: PersistentVolumeClaimName = "listener");

// The classpath for extra JAR files of the history server.
constant!(SPARK_DAEMON_CLASSPATH: EnvVarName = "SPARK_DAEMON_CLASSPATH");
// JVM arguments for the history server.
constant!(SPARK_HISTORY_OPTS: EnvVarName = "SPARK_HISTORY_OPTS");

use crate::{
    crd::{
        constants::{
            ACCESS_KEY_ID, CONTAINERDEBUG_LOG_DIRECTORY, HISTORY_UI_PORT, LISTENER_VOLUME_DIR,
            LISTENER_VOLUME_NAME, MAX_SPARK_LOG_FILES_SIZE, METRICS_PORT, SECRET_ACCESS_KEY,
            SPARK_DEFAULTS_FILE_NAME, SPARK_NO_DAEMONIZE, STACKABLE_TRUST_STORE,
            VOLUME_MOUNT_NAME_CONFIG, VOLUME_MOUNT_NAME_LOG, VOLUME_MOUNT_NAME_LOG_CONFIG,
            VOLUME_MOUNT_PATH_CONFIG, VOLUME_MOUNT_PATH_LOG, VOLUME_MOUNT_PATH_LOG_CONFIG,
        },
        history::SparkHistoryServerContainer,
        logdir::ResolvedLogDir,
        tlscerts,
    },
    history::{
        config::jvm::construct_history_jvm_args,
        controller::{
            build::{
                object_meta, recommended_labels_for_role_group_resources,
                recommended_labels_for_unversioned_role_group_resources,
                resource::listener::group_listener_name, role_group_selector,
            },
            validate::{self, NODE_ROLE_NAME, ValidatedHistoryRoleGroup},
        },
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to create the log dir volumes specification"))]
    CreateLogDirVolumesSpec { source: crate::crd::logdir::Error },

    #[snafu(display("failed to add needed volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) fn build_stateful_set(
    validated: &validate::ValidatedSparkHistoryServer,
    role_group_name: &RoleGroupName,
    rg: &ValidatedHistoryRoleGroup,
    log_dir: &ResolvedLogDir,
) -> Result<StatefulSet> {
    let resolved_product_image = &validated.resolved_product_image;
    let resource_names = validated.role_group_resource_names(role_group_name);

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

    let recommended_labels =
        recommended_labels_for_role_group_resources(validated, role_group_name);

    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    let mut pb = PodBuilder::new();

    let requested_secret_lifetime = rg
        .config
        .config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    pb.service_account_name(
        validated
            .cluster_resource_names()
            .service_account_name()
            .to_string(),
    )
    .metadata(pb_metadata)
    .image_pull_secrets_from_product_image(resolved_product_image)
    .add_volume(
        VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG.as_ref())
            .with_config_map(resource_names.role_group_config_map().to_string())
            .build(),
    )
    .context(AddVolumeSnafu)?
    .add_volume(
        VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG_CONFIG.as_ref())
            .with_config_map(log_config_map)
            .build(),
    )
    .context(AddVolumeSnafu)?
    .add_volume(
        VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG.as_ref())
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
    .security_context(
        PodSecurityContextBuilder::with_stackable_defaults()
            .fs_group(1000)
            .build(),
    );

    // Operator-set environment variables first; the already-merged (role + role group) env
    // overrides are merged in last so that they override any operator-set environment variable.
    let merged_env = EnvVarSet::new()
        .with_value(
            &CONTAINERDEBUG_LOG_DIRECTORY,
            format!("{VOLUME_MOUNT_PATH_LOG}/containerdebug"),
        )
        .with_value(&SPARK_NO_DAEMONIZE, "true")
        .with_value(&SPARK_DAEMON_CLASSPATH, "/stackable/spark/extra-jars/*")
        .with_value(
            &SPARK_HISTORY_OPTS,
            construct_history_jvm_args(&rg.config, log_dir),
        )
        .merge(rg.config.env_overrides.clone());

    let container =
        new_container_builder(&SparkHistoryServerContainer::SparkHistory.to_container_name())
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
            .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG.as_ref(), VOLUME_MOUNT_PATH_CONFIG)
            .context(AddVolumeMountSnafu)?
            .add_volume_mount(
                VOLUME_MOUNT_NAME_LOG_CONFIG.as_ref(),
                VOLUME_MOUNT_PATH_LOG_CONFIG,
            )
            .context(AddVolumeMountSnafu)?
            .add_volume_mount(VOLUME_MOUNT_NAME_LOG.as_ref(), VOLUME_MOUNT_PATH_LOG)
            .context(AddVolumeMountSnafu)?
            .add_volume_mount(LISTENER_VOLUME_NAME.as_ref(), LISTENER_VOLUME_DIR)
            .context(AddVolumeMountSnafu)?
            .build();

    // Add listener volume
    // Listener endpoints for the Webserver role will use persistent volumes
    // so that load balancers can hard-code the target addresses. This will
    // be the case even when no class is set (and the value defaults to
    // cluster-internal) as the address should still be consistent.
    //
    // PVC templates cannot be modified once they are deployed, so the version label is omitted
    // from their labels to keep them stable across version upgrades.
    let volume_claim_templates = Some(vec![listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(group_listener_name(validated, &NODE_ROLE_NAME)),
        &recommended_labels_for_unversioned_role_group_resources(validated, role_group_name),
        &LISTENER_VOLUME_NAME_PVC,
    )]);

    pb.add_container(container);

    if let Some(vector_log_config) = &rg.logging.vector_container {
        pb.add_container(vector_container(
            &SparkHistoryServerContainer::Vector.to_container_name(),
            resolved_product_image,
            vector_log_config,
            &resource_names,
            &VOLUME_MOUNT_NAME_CONFIG,
            &VOLUME_MOUNT_NAME_LOG,
            EnvVarSet::new(),
        ));
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(rg.config.pod_overrides.clone());

    let sts_metadata = object_meta(
        validated,
        resource_names.stateful_set_name().to_string(),
        role_group_name,
    )
    // Opt into the restarter-controller: it rolls the StatefulSet's Pods when a mounted
    // ConfigMap or Secret changes. See stackabletech/issues#816.
    .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
    .build();

    Ok(StatefulSet {
        metadata: sts_metadata,
        spec: Some(StatefulSetSpec {
            template: pod_template,
            volume_claim_templates,
            replicas: rg.config.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(role_group_selector(validated, role_group_name).into()),
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

#[cfg(test)]
mod tests {
    use stackable_operator::k8s_openapi::api::core::v1::EnvVar;

    use super::*;
    use crate::history::controller::build::test_support::minimal_validated_cluster;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *LISTENER_VOLUME_NAME_PVC;
        let _ = *SPARK_DAEMON_CLASSPATH;
        let _ = *SPARK_HISTORY_OPTS;
    }

    /// `envOverrides` must be applied after all operator-set environment variables, so a user
    /// override replaces the operator-set value instead of duplicating it or being ignored.
    #[test]
    fn env_overrides_override_operator_set_env_vars() {
        let mut validated = minimal_validated_cluster();
        let role_group_name: RoleGroupName = "default".parse().expect("valid role group name");

        validated
            .role_groups
            .get_mut(&role_group_name)
            .expect("the default role group exists")
            .config
            .env_overrides = EnvVarSet::new().with_value(
            &EnvVarName::from_str("SPARK_NO_DAEMONIZE").expect("valid env var name"),
            "overridden",
        );

        let rg = validated
            .role_groups
            .get(&role_group_name)
            .expect("the default role group exists");
        let stateful_set = build_stateful_set(
            &validated,
            &role_group_name,
            rg,
            &validated.cluster_config.log_dir,
        )
        .expect("the StatefulSet can be built");

        let env: Vec<EnvVar> = stateful_set
            .spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the StatefulSet has a pod spec")
            .containers
            .iter()
            .find(|container| container.name == "spark-history")
            .expect("the spark-history container exists")
            .env
            .clone()
            .expect("the spark-history container has env vars");

        let matching: Vec<&EnvVar> = env
            .iter()
            .filter(|env_var| env_var.name == "SPARK_NO_DAEMONIZE")
            .collect();

        // The override must replace the operator-set value, not duplicate it.
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].value.as_deref(), Some("overridden"));
    }
}
