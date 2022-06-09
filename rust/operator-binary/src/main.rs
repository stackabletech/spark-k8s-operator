mod pod_driver_controller;
mod spark_k8s_controller;

use clap::Parser;
use futures::StreamExt;
use stackable_operator::cli::{Command, ProductOperatorRun};
use stackable_operator::k8s_openapi::api::core::v1::ConfigMap;
use stackable_operator::k8s_openapi::api::core::v1::Pod;
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::runtime::controller::{Context, Controller};
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::logging::controller::report_controller_reconciled;
use stackable_spark_k8s_crd::SparkApplication;
use tracing::info_span;
use tracing_futures::Instrument;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

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
        Command::Crd => println!("{}", serde_yaml::to_string(&SparkApplication::crd())?,),
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
                stackable_operator::client::create_client(Some("spark.stackable.tech".to_string()))
                    .await?;

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
                Context::new(spark_k8s_controller::Ctx {
                    client: client.clone(),
                }),
            )
            .map(|res| {
                report_controller_reconciled(
                    &client,
                    "sparkapplications.spark.stackable.tech",
                    &res,
                )
            })
            .instrument(info_span!("app_controller"));

            let pod_driver_controller = Controller::new(
                watch_namespace.get_api::<Pod>(&client),
                ListParams::default()
                    .labels("app.kubernetes.io/managed-by=spark-k8s-operator,spark-role=driver"),
            )
            .owns(
                watch_namespace.get_api::<Pod>(&client),
                ListParams::default(),
            )
            .shutdown_on_signal()
            .run(
                pod_driver_controller::reconcile,
                pod_driver_controller::error_policy,
                Context::new(pod_driver_controller::Ctx {
                    client: client.clone(),
                }),
            )
            .map(|res| {
                report_controller_reconciled(
                    &client,
                    "pod-driver.sparkapplications.stackable.tech",
                    &res,
                )
            })
            .instrument(info_span!("pod_driver_controller"));

            futures::stream::select(app_controller, pod_driver_controller)
                .collect::<()>()
                .await;
        }
    }
    Ok(())
}
