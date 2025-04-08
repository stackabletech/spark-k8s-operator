use std::{ops::Deref as _, sync::Arc};

use clap::Parser;
use futures::{StreamExt, pin_mut};
use history::history_controller;
use product_config::ProductConfigManager;
use stackable_operator::{
    YamlSchema,
    cli::{Command, ProductOperatorRun, RollingPeriod},
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
};
use stackable_telemetry::{Tracing, tracing::settings::Settings};
use tracing::{info_span, level_filters::LevelFilter};
use tracing_futures::Instrument;

use crate::crd::{
    SparkApplication,
    constants::{
        HISTORY_FULL_CONTROLLER_NAME, OPERATOR_NAME, POD_DRIVER_FULL_CONTROLLER_NAME,
        SPARK_CONTROLLER_NAME, SPARK_FULL_CONTROLLER_NAME,
    },
    history::SparkHistoryServer,
};

mod config;
mod crd;
mod history;
mod pod_driver_controller;
mod product_logging;
mod spark_k8s_controller;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

// TODO (@NickLarsenNZ): Change the variable to `CONSOLE_LOG`
pub const ENV_VAR_CONSOLE_LOG: &str = "SPARK_K8S_OPERATOR_LOG";

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
            SparkApplication::merged_crd(SparkApplication::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
            SparkHistoryServer::merged_crd(SparkHistoryServer::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
        }
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            telemetry_arguments,
            cluster_info_opts,
        }) => {
            let _tracing_guard = Tracing::builder()
                .service_name("spark-k8s-operator")
                .with_console_output((
                    ENV_VAR_CONSOLE_LOG,
                    LevelFilter::INFO,
                    !telemetry_arguments.no_console_output,
                ))
                // NOTE (@NickLarsenNZ): Before stackable-telemetry was used, the log directory was
                // set via an env: `SPARK_K8S_OPERATOR_LOG_DIRECTORY`.
                // See: https://github.com/stackabletech/operator-rs/blob/f035997fca85a54238c8de895389cc50b4d421e2/crates/stackable-operator/src/logging/mod.rs#L40
                // Now it will be `ROLLING_LOGS` (or via `--rolling-logs <DIRECTORY>`).
                .with_file_output(telemetry_arguments.rolling_logs.map(|log_directory| {
                    let rotation_period = telemetry_arguments
                        .rolling_logs_period
                        .unwrap_or(RollingPeriod::Never)
                        .deref()
                        .clone();

                    Settings::builder()
                        .with_environment_variable(ENV_VAR_CONSOLE_LOG)
                        .with_default_level(LevelFilter::INFO)
                        .file_log_settings_builder(log_directory, "tracing-rs.log")
                        .with_rotation_period(rotation_period)
                        .build()
                }))
                .with_otlp_log_exporter((
                    "OTLP_LOG",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_logs,
                ))
                .with_otlp_trace_exporter((
                    "OTLP_TRACE",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_traces,
                ))
                .build()
                .init()?;

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
                &cluster_info_opts,
            )
            .await?;

            let ctx = Ctx {
                client: client.clone(),
                product_config: product_config.load(&PRODUCT_CONFIG_PATHS)?,
            };
            let spark_event_recorder = Arc::new(Recorder::new(client.as_kube_client(), Reporter {
                controller: SPARK_FULL_CONTROLLER_NAME.to_string(),
                instance: None,
            }));
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

            let pod_driver_event_recorder =
                Arc::new(Recorder::new(client.as_kube_client(), Reporter {
                    controller: POD_DRIVER_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                }));
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
            let history_event_recorder =
                Arc::new(Recorder::new(client.as_kube_client(), Reporter {
                    controller: HISTORY_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                }));
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

            pin_mut!(app_controller, pod_driver_controller, history_controller);
            // kube-runtime's Controller will tokio::spawn each reconciliation, so this only concerns the internal watch machinery
            futures::future::select(
                futures::future::select(app_controller, pod_driver_controller),
                history_controller,
            )
            .await;
        }
    }
    Ok(())
}
