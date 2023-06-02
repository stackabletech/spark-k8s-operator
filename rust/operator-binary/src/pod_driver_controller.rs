use stackable_operator::{k8s_openapi::api::core::v1::Pod, kube::runtime::controller::Action};
use stackable_spark_k8s_crd::{
    constants::POD_DRIVER_CONTROLLER_NAME, SparkApplication, SparkApplicationStatus, 
    constants::SYSTEM_TRUST_STORE, constants::SYSTEM_TRUST_STORE_PASSWORD, constants::STACKABLE_TLS_STORE_PASSWORD, 
};
use std::sync::Arc;
use std::time::Duration;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::logging::controller::ReconcilerError;
use strum::{EnumDiscriminants, IntoStaticStr};

const LABEL_NAME_INSTANCE: &str = "app.kubernetes.io/instance";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Label [{LABEL_NAME_INSTANCE}] not found for pod name [{pod_name}]"))]
    LabelInstanceNotFound { pod_name: String },
    #[snafu(display("Failed to update status for application [{name}]"))]
    ApplySparkApplicationStatus {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("Pod name not found"))]
    PodNameNotFound,
    #[snafu(display("Namespace not found"))]
    NamespaceNotFound,
    #[snafu(display("Status phase not found for pod [{pod_name}]"))]
    PodStatusPhaseNotFound { pod_name: String },
    #[snafu(display("Spark application [{name}] not found"))]
    SparkApplicationNotFound {
        source: stackable_operator::error::Error,
        name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}
/// Updates the status of the SparkApplication that started the pod.
pub async fn reconcile(pod: Arc<Pod>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile driver pod");

    let pod_name = pod.metadata.name.as_ref().context(PodNameNotFoundSnafu)?;
    let app_name = pod
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(&String::from(LABEL_NAME_INSTANCE)))
        .context(LabelInstanceNotFoundSnafu {
            pod_name: pod_name.clone(),
        })?;
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref()).context(
        PodStatusPhaseNotFoundSnafu {
            pod_name: pod_name.clone(),
        },
    )?;

    let app = ctx
        .client
        .get::<SparkApplication>(
            app_name.as_ref(),
            pod.metadata
                .namespace
                .as_ref()
                .context(NamespaceNotFoundSnafu)?,
        )
        .await
        .context(SparkApplicationNotFoundSnafu {
            name: app_name.clone(),
        })?;

    tracing::info!("Update spark application [{app_name}] status to [{phase}]");

    ctx.client
        .apply_patch_status(
            POD_DRIVER_CONTROLLER_NAME,
            &app,
            &SparkApplicationStatus {
                phase: phase.clone(),
            },
        )
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: app_name.clone(),
        })?;

    Ok(Action::await_change())
}

pub fn error_policy(_obj: Arc<Pod>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

/// Generates the shell script to create key and truststores from the certificates provided
/// by the secret operator.
pub fn create_key_and_trust_store(
    cert_directory: &str,
    stackable_cert_directory: &str,
    alias_name: &str,
    secret_class: &str
) -> Vec<String> {
    vec![
        format!("echo [{stackable_cert_directory}] Cleaning up truststore - just in case"),
        format!("rm -f {stackable_cert_directory}/truststore.p12"),
        format!("echo [{stackable_cert_directory}] Creating truststore"),
        format!("keytool -importcert -file {cert_directory}/{secret_class}-tls-certificate/ca.cert -keystore {stackable_cert_directory}/truststore.p12 -storetype pkcs12 -noprompt -alias {alias_name} -storepass {STACKABLE_TLS_STORE_PASSWORD}"),
        format!("echo [{stackable_cert_directory}] Creating certificate chain"),
        format!("cat {cert_directory}/{secret_class}-tls-certificate/ca.crt {cert_directory}/{secret_class}-tls-certificate/tls.crt > {stackable_cert_directory}/{secret_class}/chain.crt"),
        format!("echo [{stackable_cert_directory}] Creating keystore"),
        format!("openssl pkcs12 -export -in {stackable_cert_directory}/{secret_class}/chain.crt -inkey {cert_directory}/{secret_class}/tls.key -out {stackable_cert_directory}/keystore.p12 --passout pass:{STACKABLE_TLS_STORE_PASSWORD}")
    ]
}

pub fn add_cert_to_stackable_truststore(
    cert_file: &str,
    truststore_directory: &str,
    alias_name: &str,
) -> Vec<String> {
    vec![
        format!("echo [{truststore_directory}] Adding cert from {cert_file} to truststore {truststore_directory}/truststore.p12"),
        format!("keytool -importcert -file {cert_file} -keystore {truststore_directory}/truststore.p12 -storetype pkcs12 -noprompt -alias {alias_name} -storepass {STACKABLE_TLS_STORE_PASSWORD}"),
    ]
}
