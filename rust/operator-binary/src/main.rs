use std::sync::Arc;

use clap::{Parser, crate_description, crate_version};
use futures::{StreamExt, pin_mut};
use history::history_controller;
use product_config::ProductConfigManager;
use stackable_operator::{
    YamlSchema,
    cli::{Command, ProductOperatorRun},
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
use tracing::info_span;
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
            tracing_target,
            cluster_info_opts,
        }) => {
            stackable_operator::logging::initialize_logging(
                "SPARK_K8S_OPERATOR_LOG",
                "spark-k8s",
                tracing_target,
            );
            stackable_operator::utils::print_startup_string(
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
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
