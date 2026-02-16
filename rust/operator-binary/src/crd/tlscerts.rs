use std::collections::BTreeSet;

use stackable_operator::{
    commons::tls_verification::{
        CaCert, Tls, TlsClientDetails, TlsServerVerification, TlsVerification,
    },
    crd::s3,
};

use crate::crd::{
    constants::{STACKABLE_MOUNT_PATH_TLS, STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE},
    logdir::ResolvedLogDir,
};

pub fn tls_secret_name(s3conn: &s3::v1alpha1::ConnectionSpec) -> Option<&str> {
    if let s3::v1alpha1::ConnectionSpec {
        tls:
            TlsClientDetails {
                tls:
                    Some(Tls {
                        verification:
                            TlsVerification::Server(TlsServerVerification {
                                ca_cert: CaCert::SecretClass(ref secret_name),
                            }),
                    }),
            },
        ..
    } = s3conn
    {
        return Some(secret_name);
    }

    None
}

pub fn tls_secret_names<'a>(
    s3conn: &'a Option<s3::v1alpha1::ConnectionSpec>,
    logdir: &'a Option<ResolvedLogDir>,
) -> Option<Vec<&'a str>> {
    // Ensure the secret names are duplicated.
    let mut names = BTreeSet::new();

    if let Some(secret_name) = s3conn.as_ref().and_then(|s3conn| tls_secret_name(s3conn)) {
        names.insert(secret_name);
    }

    if let Some(logdir) = logdir {
        if let Some(secret_name) = logdir.tls_secret_name() {
            names.insert(secret_name);
        }
    }
    if names.is_empty() {
        None
    } else {
        Some(names.into_iter().collect())
    }
}

pub fn convert_system_trust_store_to_pkcs12() -> String {
    format!(
        "cert-tools generate-pkcs12-truststore --pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem --out {STACKABLE_TRUST_STORE}/truststore.p12 --out-password {STACKABLE_TLS_STORE_PASSWORD}"
    )
}

pub fn import_truststore(secret_name: &str) -> String {
    let mount_trust_store_path = format!("{STACKABLE_MOUNT_PATH_TLS}/{secret_name}/truststore.p12");
    let trust_store_path = format!("{STACKABLE_TRUST_STORE}/truststore.p12");

    format!(
        "cert-tools generate-pkcs12-truststore --pkcs12 {trust_store_path}:{STACKABLE_TLS_STORE_PASSWORD} --pkcs12 {mount_trust_store_path} --out {trust_store_path} --out-password {STACKABLE_TLS_STORE_PASSWORD}"
    )
}
