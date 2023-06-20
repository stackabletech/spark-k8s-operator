use stackable_operator::commons::{
    s3::S3ConnectionSpec,
    tls::{CaCert, TlsVerification},
};

use crate::{
    constants::{
        STACKABLE_MOUNT_PATH_TLS, STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE,
        SYSTEM_TRUST_STORE, SYSTEM_TRUST_STORE_PASSWORD,
    },
    s3logdir::S3LogDir,
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

pub fn tls_secret_names<'a>(
    s3conn: &'a Option<S3ConnectionSpec>,
    s3logdir: &'a Option<S3LogDir>,
) -> Option<Vec<&'a str>> {
    let mut names = Vec::new();

    if let Some(secret_name) = tls_secret_name(s3conn) {
        names.push(secret_name);
    }

    if let Some(logdir) = s3logdir {
        if let Some(secret_name) = tls_secret_name(&logdir.bucket.connection) {
            names.push(secret_name);
        }
    }
    if names.is_empty() {
        None
    } else {
        Some(names)
    }
}

pub fn create_key_and_trust_store() -> Vec<String> {
    vec![
        format!("keytool -importkeystore -srckeystore {SYSTEM_TRUST_STORE} -srcstoretype jks -srcstorepass {SYSTEM_TRUST_STORE_PASSWORD} -destkeystore {STACKABLE_TRUST_STORE}/truststore.p12 -deststoretype pkcs12 -deststorepass {STACKABLE_TLS_STORE_PASSWORD} -noprompt"),
    ]
}

pub fn add_cert_to_stackable_truststore(secret_name: &str) -> Vec<String> {
    vec![
        format!("echo [{STACKABLE_MOUNT_PATH_TLS}/{secret_name}/ca.crt] Adding cert..."),
        format!("keytool -importcert -file {STACKABLE_MOUNT_PATH_TLS}/{secret_name}/ca.crt -alias stackable-{secret_name} -keystore {STACKABLE_TRUST_STORE}/truststore.p12 -storepass {STACKABLE_TLS_STORE_PASSWORD} -noprompt"),
        format!("echo [{STACKABLE_MOUNT_PATH_TLS}/{secret_name}/ca.crt] Checking for cert..."),
        format!("keytool -list -keystore {STACKABLE_TRUST_STORE}/truststore.p12 -storepass {STACKABLE_TLS_STORE_PASSWORD} -noprompt | grep stackable"),
    ]
}
