mod spark_k8s_controller;

use clap::Parser;
use futures::stream::StreamExt;
use stackable_operator::cli::{Command, ProductOperatorRun};
use stackable_operator::k8s_openapi::api::core::v1::ConfigMap;
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::runtime::controller::{Context, Controller};
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::logging::controller::report_controller_reconciled;
use stackable_spark_k8s_crd::SparkApplication;

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
    stackable_operator::logging::initialize_logging("SPARK_K8S_OPERATOR_LOG");
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => println!("{}", serde_yaml::to_string(&SparkApplication::crd())?,),
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
        }) => {
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );

            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/spark-k8s-operator/config-spec/properties.yaml",
            ])?;

            let client =
                stackable_operator::client::create_client(Some("spark.stackable.tech".to_string()))
                    .await?;

            Controller::new(
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
                    product_config,
                }),
            )
            .map(|res| {
                report_controller_reconciled(
                    &client,
                    "sparkapplications.spark.stackable.tech",
                    &res,
                )
            })
            .collect::<()>()
            .await;
        }
    }

    Ok(())
}
