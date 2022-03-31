pub const APP_NAME: &str = "spark-k8s";

pub const VOLUME_MOUNT_NAME_POD_TEMPLATES: &str = "pod-template";
pub const VOLUME_MOUNT_PATH_POD_TEMPLATES: &str = "/stackable/spark/pod-templates";

pub const CONTAINER_NAME_JOB: &str = "job";
pub const VOLUME_MOUNT_NAME_JOB: &str = "job-files";
pub const VOLUME_MOUNT_PATH_JOB: &str = "/stackable/spark/jobs";

pub const CONTAINER_NAME_REQ: &str = "requirements";
pub const VOLUME_MOUNT_NAME_REQ: &str = "req-files";
pub const VOLUME_MOUNT_PATH_REQ: &str = "/stackable/spark/requirements";

pub const CONTAINER_IMAGE_NAME_DRIVER: &str = "dummy-overwritten-by-command-line";
pub const CONTAINER_NAME_DRIVER: &str = "spark-driver";

pub const CONTAINER_IMAGE_NAME_EXECUTOR: &str = "dummy-overwritten-by-command-line";
pub const CONTAINER_NAME_EXECUTOR: &str = "spark-executor";
