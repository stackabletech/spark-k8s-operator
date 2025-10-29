use stackable_operator::kvp::ObjectLabels;

use crate::crd::constants::{
    HISTORY_APP_NAME, HISTORY_CONTROLLER_NAME, HISTORY_ROLE_NAME, OPERATOR_NAME,
};

pub mod config;
pub mod history_controller;
pub mod operations;
pub mod service;

pub(crate) fn recommended_labels<'a, T>(
    shs: &'a T,
    app_version_label_value: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner: shs,
        app_name: HISTORY_APP_NAME,
        app_version: app_version_label_value,
        operator_name: OPERATOR_NAME,
        controller_name: HISTORY_CONTROLLER_NAME,
        role: HISTORY_ROLE_NAME,
        role_group,
    }
}
