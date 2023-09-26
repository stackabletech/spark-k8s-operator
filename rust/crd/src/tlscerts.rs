use stackable_operator::commons::authentication::tls::{Tls, TlsServerVerification};
use stackable_operator::commons::{
    authentication::tls::{CaCert, TlsVerification},
    s3::S3ConnectionSpec,
};

use crate::{
    constants::{
        STACKABLE_MOUNT_PATH_TLS, STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE,
        SYSTEM_TRUST_STORE, SYSTEM_TRUST_STORE_PASSWORD,
    },
    s3logdir::S3LogDir,
};

pub fn tls_secret_name(s3conn: &Option<S3ConnectionSpec>) -> Option<&str> {
    if let Some(S3ConnectionSpec {
        tls:
            Some(Tls {
                verification:
                    TlsVerification::Server(TlsServerVerification {
                        ca_cert: CaCert::SecretClass(ref secret_name),
                    }),
            }),
        ..
    }) = s3conn
    {
        return Some(secret_name);
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

pub fn convert_system_trust_store_to_pkcs12() -> Vec<String> {
    vec![
        format!("keytool -importkeystore -srckeystore {SYSTEM_TRUST_STORE} -srcstoretype jks -srcstorepass {SYSTEM_TRUST_STORE_PASSWORD} -destkeystore {STACKABLE_TRUST_STORE}/truststore.p12 -deststoretype pkcs12 -deststorepass {STACKABLE_TLS_STORE_PASSWORD} -noprompt"),
    ]
}

pub fn import_truststore(secret_name: &str) -> Vec<String> {
    let mount_trust_store_path = format!("{STACKABLE_MOUNT_PATH_TLS}/{secret_name}/truststore.p12");
    let trust_store_path = format!("{STACKABLE_TRUST_STORE}/truststore.p12");

    vec![
        format!("echo Importing [{mount_trust_store_path}] to [{trust_store_path}] ..."),
        format!("keytool -importkeystore -srckeystore {mount_trust_store_path} -srcalias 1 -srcstorepass \"\" -destkeystore {trust_store_path} -destalias stackable-{secret_name} -storepass {STACKABLE_TLS_STORE_PASSWORD} -noprompt"),
    ]
}
