// TODO: Look into how to properly resolve `clippy::result_large_err`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]
use std::sync::Arc;

use clap::Parser;
use connect::crd::{CONNECT_FULL_CONTROLLER_NAME, SparkConnectServer};
use futures::{StreamExt, pin_mut, select};
use history::history_controller;
use product_config::ProductConfigManager;
use stackable_operator::{
    YamlSchema,
    cli::{Command, RunArguments},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Pod, Service},
    },
    kube::{
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
    telemetry::Tracing,
};
use tracing::info_span;
use tracing_futures::Instrument;

use crate::{
    connect::crd::SparkConnectServerVersion,
    crd::{
        SparkApplication,
        constants::{
            HISTORY_FULL_CONTROLLER_NAME, OPERATOR_NAME, POD_DRIVER_FULL_CONTROLLER_NAME,
            SPARK_CONTROLLER_NAME, SPARK_FULL_CONTROLLER_NAME,
        },
        history::SparkHistoryServer,
    },
};

mod config;
mod connect;
mod crd;
mod history;
mod pod_driver_controller;
mod product_logging;
mod spark_k8s_controller;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

const PRODUCT_CONFIG_PATHS: [&str; 2] = [
    "deploy/config-spec/properties.yaml",
    "/etc/stackable/spark-k8s-operator/config-spec/properties.yaml",
];
pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => {
            SparkApplication::merged_crd(crd::SparkApplicationVersion::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
            SparkHistoryServer::merged_crd(crd::history::SparkHistoryServerVersion::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
            SparkConnectServer::merged_crd(SparkConnectServerVersion::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
        }
        Command::Run(RunArguments {
            operator_environment: _,
            watch_namespace,
            product_config,
            maintenance: _,
            common,
        }) => {
            // NOTE (@NickLarsenNZ): Before stackable-telemetry was used:
            // - The console log level was set by `SPARK_K8S_OPERATOR_LOG`, and is now `CONSOLE_LOG` (when using Tracing::pre_configured).
            // - The file log level was set by `SPARK_K8S_OPERATOR_LOG`, and is now set via `FILE_LOG` (when using Tracing::pre_configured).
            // - The file log directory was set by `SPARK_K8S_OPERATOR_LOG_DIRECTORY`, and is now set by `ROLLING_LOGS_DIR` (or via `--rolling-logs <DIRECTORY>`).
            let _tracing_guard =
                Tracing::pre_configured(built_info::PKG_NAME, common.telemetry).init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
            );

            let client = stackable_operator::client::initialize_operator(
                Some(OPERATOR_NAME.to_string()),
                &common.cluster_info,
            )
            .await?;

            let ctx = Ctx {
                client: client.clone(),
                product_config: product_config.load(&PRODUCT_CONFIG_PATHS)?,
            };
            let spark_event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: SPARK_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));
            let app_controller = Controller::new(
                watch_namespace
                    .get_api::<DeserializeGuard<crd::v1alpha1::SparkApplication>>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                watcher::Config::default(),
            )
            .shutdown_on_signal()
            .run(
                spark_k8s_controller::reconcile,
                spark_k8s_controller::error_policy,
                Arc::new(ctx),
            )
            .instrument(info_span!("app_controller"))
            // We can let the reporting happen in the background
            .for_each_concurrent(
                16, // concurrency limit
                |result| {
                    // The event_recorder needs to be shared across all invocations, so that
                    // events are correctly aggregated
                    let spark_event_recorder = spark_event_recorder.clone();
                    async move {
                        report_controller_reconciled(
                            &spark_event_recorder,
                            SPARK_FULL_CONTROLLER_NAME,
                            &result,
                        )
                        .await;
                    }
                },
            );

            let pod_driver_event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: POD_DRIVER_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));
            let pod_driver_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<Pod>>(&client),
                watcher::Config::default()
                 .labels(&format!("app.kubernetes.io/managed-by={OPERATOR_NAME}_{SPARK_CONTROLLER_NAME},spark-role=driver")),
            )
            .owns(
                watch_namespace.get_api::<DeserializeGuard<Pod>>(&client),
                watcher::Config::default(),
            )
            .shutdown_on_signal()
            .run(
                pod_driver_controller::reconcile,
                pod_driver_controller::error_policy,
                Arc::new(client.clone()),
            )
            .instrument(info_span!("pod_driver_controller"))
            // We can let the reporting happen in the background
            .for_each_concurrent(
                16, // concurrency limit
                |result| {
                    // The event_recorder needs to be shared across all invocations, so that
                    // events are correctly aggregated
                    let pod_driver_event_recorder = pod_driver_event_recorder.clone();
                    async move {
                        report_controller_reconciled(
                            &pod_driver_event_recorder,
                            POD_DRIVER_FULL_CONTROLLER_NAME,
                            &result,
                        )
                        .await;
                    }
                },
            );

            // Create new object because Ctx cannot be cloned
            let ctx = Ctx {
                client: client.clone(),
                product_config: product_config.load(&PRODUCT_CONFIG_PATHS)?,
            };
            let history_event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: HISTORY_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));
            let history_controller = Controller::new(
                watch_namespace
                    .get_api::<DeserializeGuard<crd::history::v1alpha1::SparkHistoryServer>>(
                        &client,
                    ),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace
                    .get_api::<DeserializeGuard<crd::history::v1alpha1::SparkHistoryServer>>(
                        &client,
                    ),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<DeserializeGuard<StatefulSet>>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<DeserializeGuard<Service>>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                watcher::Config::default(),
            )
            .shutdown_on_signal()
            .run(
                history_controller::reconcile,
                history_controller::error_policy,
                Arc::new(ctx),
            )
            .instrument(info_span!("history_controller"))
            // We can let the reporting happen in the background
            .for_each_concurrent(
                16, // concurrency limit
                |result| {
                    // The event_recorder needs to be shared across all invocations, so that
                    // events are correctly aggregated
                    let history_event_recorder = history_event_recorder.clone();
                    async move {
                        report_controller_reconciled(
                            &history_event_recorder,
                            HISTORY_FULL_CONTROLLER_NAME,
                            &result,
                        )
                        .await;
                    }
                },
            );

            // ==============================
            // Create new object because Ctx cannot be cloned
            let ctx = Ctx {
                client: client.clone(),
                product_config: product_config.load(&PRODUCT_CONFIG_PATHS)?,
            };
            let connect_event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: CONNECT_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));
            let connect_controller = Controller::new(
                watch_namespace
                    .get_api::<DeserializeGuard<connect::crd::v1alpha1::SparkConnectServer>>(
                        &client,
                    ),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace
                    .get_api::<DeserializeGuard<connect::crd::v1alpha1::SparkConnectServer>>(
                        &client,
                    ),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<DeserializeGuard<StatefulSet>>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<DeserializeGuard<Service>>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                watcher::Config::default(),
            )
            .shutdown_on_signal()
            .run(
                connect::controller::reconcile,
                connect::controller::error_policy,
                Arc::new(ctx),
            )
            .instrument(info_span!("connect_controller"))
            // We can let the reporting happen in the background
            .for_each_concurrent(
                16, // concurrency limit
                |result| {
                    // The event_recorder needs to be shared across all invocations, so that
                    // events are correctly aggregated
                    let connect_event_recorder = connect_event_recorder.clone();
                    async move {
                        report_controller_reconciled(
                            &connect_event_recorder,
                            CONNECT_FULL_CONTROLLER_NAME,
                            &result,
                        )
                        .await;
                    }
                },
            );

            //
            pin_mut!(
                app_controller,
                pod_driver_controller,
                history_controller,
                connect_controller
            );
            // kube-runtime's Controller will tokio::spawn each reconciliation, so this only concerns the internal watch machinery
            select! {
                r1 = app_controller => r1,
                r2 = pod_driver_controller => r2,
                r3 = history_controller => r3,
                r4 = connect_controller => r4,
            };
        }
    }
    Ok(())
}
