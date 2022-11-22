mod pod_driver_controller;
mod spark_k8s_controller;

use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use stackable_operator::cli::{Command, ProductOperatorRun};
use stackable_operator::k8s_openapi::api::core::v1::ConfigMap;
use stackable_operator::k8s_openapi::api::core::v1::Pod;
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::runtime::controller::Controller;
use stackable_operator::logging::controller::report_controller_reconciled;
use stackable_operator::CustomResourceExt;
use stackable_spark_k8s_crd::CONTROLLER_NAME;
use stackable_spark_k8s_crd::{SparkApplication, OPERATOR_NAME};
use tracing::info_span;
use tracing_futures::Instrument;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

use crate::pod_driver_controller::POD_DRIVER_CONTROLLER_NAME;

#[derive(Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => {
            SparkApplication::print_yaml_schema()?;
        }
        Command::Run(ProductOperatorRun {
            product_config: _,
            watch_namespace,
            tracing_target,
        }) => {
            stackable_operator::logging::initialize_logging(
                "SPARK_K8S_OPERATOR_LOG",
                "spark-k8s",
                tracing_target,
            );
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );

            let client =
                stackable_operator::client::create_client(Some(OPERATOR_NAME.to_string())).await?;

            let app_controller = Controller::new(
                watch_namespace.get_api::<SparkApplication>(&client),
                ListParams::default(),
            )
            .owns(
                watch_namespace.get_api::<ConfigMap>(&client),
                ListParams::default(),
            )
            .shutdown_on_signal()
            .run(
                spark_k8s_controller::reconcile,
                spark_k8s_controller::error_policy,
                Arc::new(spark_k8s_controller::Ctx {
                    client: client.clone(),
                }),
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
                ListParams::default()
                    .labels(&format!("app.kubernetes.io/managed-by={OPERATOR_NAME}_{CONTROLLER_NAME},spark-role=driver")),
            )
            .owns(
                watch_namespace.get_api::<Pod>(&client),
                ListParams::default(),
            )
            .shutdown_on_signal()
            .run(
                pod_driver_controller::reconcile,
                pod_driver_controller::error_policy,
                Arc::new(pod_driver_controller::Ctx {
                    client: client.clone(),
                }),
            )
            .map(|res| report_controller_reconciled(&client, POD_DRIVER_CONTROLLER_NAME, &res))
            .instrument(info_span!("pod_driver_controller"));

            futures::stream::select(app_controller, pod_driver_controller)
                .collect::<()>()
                .await;
        }
    }
    Ok(())
}
