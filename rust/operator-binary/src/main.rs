mod history;
mod pod_driver_controller;
mod product_logging;
mod spark_k8s_controller;

use std::sync::Arc;

use clap::{crate_description, crate_version, Parser};
use futures::StreamExt;
use history::history_controller;
use product_config::ProductConfigManager;
use stackable_operator::cli::{Command, ProductOperatorRun};
use stackable_operator::k8s_openapi::api::apps::v1::StatefulSet;
use stackable_operator::k8s_openapi::api::core::v1::Pod;
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Service};
use stackable_operator::kube::runtime::{controller::Controller, watcher};
use stackable_operator::logging::controller::report_controller_reconciled;
use stackable_operator::CustomResourceExt;
use stackable_spark_k8s_crd::constants::{
    CONTROLLER_NAME, HISTORY_CONTROLLER_NAME, OPERATOR_NAME, POD_DRIVER_CONTROLLER_NAME,
};
use stackable_spark_k8s_crd::history::SparkHistoryServer;
use stackable_spark_k8s_crd::SparkApplication;
use tracing::info_span;
use tracing_futures::Instrument;

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
            SparkApplication::print_yaml_schema(built_info::PKG_VERSION)?;
            SparkHistoryServer::print_yaml_schema(built_info::PKG_VERSION)?;
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
            let app_controller = Controller::new(
                watch_namespace.get_api::<SparkApplication>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<ConfigMap>(&client),
                watcher::Config::default(),
            )
            .shutdown_on_signal()
            .run(
                spark_k8s_controller::reconcile,
                spark_k8s_controller::error_policy,
                Arc::new(ctx),
            )
            .map(|res| {
                report_controller_reconciled(
                    &client,
                    &format!("{CONTROLLER_NAME}.{OPERATOR_NAME}"),
                    &res,
                )
            })
            .instrument(info_span!("app_controller"));

            let pod_driver_controller = Controller::new(
                watch_namespace.get_api::<Pod>(&client),
                watcher::Config::default()
                    .labels(&format!("app.kubernetes.io/managed-by={OPERATOR_NAME}_{CONTROLLER_NAME},spark-role=driver")),
            )
            .owns(
                watch_namespace.get_api::<Pod>(&client),
                watcher::Config::default(),
            )
            .shutdown_on_signal()
            .run(
                pod_driver_controller::reconcile,
                pod_driver_controller::error_policy,
                Arc::new(client.clone()),
            )
            .map(|res| report_controller_reconciled(&client, &format!("{OPERATOR_NAME}.{POD_DRIVER_CONTROLLER_NAME}"), &res))
            .instrument(info_span!("pod_driver_controller"));

            // Create new object because Ctx cannot be cloned
            let ctx = Ctx {
                client: client.clone(),
                product_config: product_config.load(&PRODUCT_CONFIG_PATHS)?,
            };
            let history_controller = Controller::new(
                watch_namespace.get_api::<SparkHistoryServer>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<SparkHistoryServer>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<StatefulSet>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<Service>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<ConfigMap>(&client),
                watcher::Config::default(),
            )
            .shutdown_on_signal()
            .run(
                history_controller::reconcile,
                history_controller::error_policy,
                Arc::new(ctx),
            )
            .map(|res| {
                report_controller_reconciled(
                    &client,
                    &format!("{OPERATOR_NAME}.{HISTORY_CONTROLLER_NAME}"),
                    &res,
                )
            })
            .instrument(info_span!("history_controller"));

            futures::stream::select(
                futures::stream::select(app_controller, pod_driver_controller),
                history_controller,
            )
            .collect::<()>()
            .await;
        }
    }
    Ok(())
}
