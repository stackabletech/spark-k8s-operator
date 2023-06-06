use stackable_operator::commons::{
    s3::S3ConnectionSpec,
    tls::{CaCert, TlsVerification},
};

use crate::constants::{
    STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE, SYSTEM_TRUST_STORE,
    SYSTEM_TRUST_STORE_PASSWORD,
};

pub fn tls_secret_name(s3conn: &Option<S3ConnectionSpec>) -> Option<&str> {
    if let Some(conn) = s3conn.as_ref() {
        if let Some(tls) = &conn.tls {
            if let TlsVerification::Server(verification) = &tls.verification {
                if let CaCert::SecretClass(secret_name) = &verification.ca_cert {
                    return Some(secret_name);
                }
            }
        }
    }
    None
}

pub fn create_key_and_trust_store() -> Vec<String> {
    vec![
        format!("keytool -importkeystore -srckeystore {SYSTEM_TRUST_STORE} -srcstoretype jks -srcstorepass {SYSTEM_TRUST_STORE_PASSWORD} -destkeystore {STACKABLE_TRUST_STORE}/truststore.p12 -deststoretype pkcs12 -deststorepass {STACKABLE_TLS_STORE_PASSWORD} -noprompt"),
    ]
}

pub fn add_cert_to_stackable_truststore(
    stackable_tls_store_dir: &str,
    secret_name: &str,
    stackable_trust_store: &str,
    stackable_trust_store_pwd: &str,
) -> Vec<String> {
    vec![
        format!("echo [{stackable_tls_store_dir}/{secret_name}/ca.crt] Adding cert..."),
        format!("keytool -importcert -file {stackable_tls_store_dir}/{secret_name}/ca.crt -alias stackable-{secret_name} -keystore {stackable_trust_store}/truststore.p12 -storepass {stackable_trust_store_pwd} -noprompt"),
        format!("echo [{stackable_tls_store_dir}/{secret_name}/ca.crt] Checking for cert..."),
        format!("keytool -list -keystore {stackable_trust_store}/truststore.p12 -storepass {stackable_trust_store_pwd} -noprompt | grep stackable"),
    ]
}
