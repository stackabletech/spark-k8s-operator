use stackable_operator::kvp::ObjectLabels;
use strum::Display;

use crate::{
    connect::crd::{CONNECT_CONTROLLER_NAME, CONNECT_SERVER_ROLE_NAME},
    crd::constants::{APP_NAME, OPERATOR_NAME},
};

const DUMMY_SPARK_CONNECT_GROUP_NAME: &str = "default";

pub fn labels<'a, T>(scs: &'a T, app_version_label: &'a str, role: &'a str) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner: scs,
        app_name: APP_NAME,
        app_version: app_version_label,
        operator_name: OPERATOR_NAME,
        controller_name: CONNECT_CONTROLLER_NAME,
        role,
        role_group: DUMMY_SPARK_CONNECT_GROUP_NAME,
    }
}

// The dead code annotation is to shut up complains about missing Executor instantiations
// These will come in the future.
#[allow(dead_code)]
#[derive(Clone, Debug, Display)]
#[strum(serialize_all = "lowercase")]
pub enum SparkConnectRole {
    Server,
    Executor,
}

pub fn object_name(stacklet_name: &str, role: SparkConnectRole) -> String {
    match role {
        SparkConnectRole::Server => format!("{}-{}", stacklet_name, CONNECT_SERVER_ROLE_NAME),
        SparkConnectRole::Executor => todo!(),
    }
}
