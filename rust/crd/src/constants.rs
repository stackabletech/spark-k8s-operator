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

pub const ACCESS_KEY_ID: &str = "accessKeyId";
pub const SECRET_ACCESS_KEY: &str = "secretAccessKey";
pub const S3_SECRET_DIR_NAME: &str = "/stackable/secrets";

pub const MIN_MEMORY_OVERHEAD: u32 = 384;
pub const JVM_OVERHEAD_FACTOR: f32 = 0.1;
pub const NON_JVM_OVERHEAD_FACTOR: f32 = 0.4;

pub const OPERATOR_NAME: &str = "spark.stackable.tech";
pub const CONTROLLER_NAME: &str = "sparkapplication";
pub const POD_DRIVER_CONTROLLER_NAME: &str = "pod-driver";
pub const HISTORY_CONTROLLER_NAME: &str = "history";

pub const HISTORY_ROLE_NAME: &str = "node";

pub const HISTORY_IMAGE_BASE_NAME: &str = "spark-k8s";

pub const HISTORY_CONFIG_FILE_NAME: &str = "spark-defaults.conf";
pub const HISTORY_CONFIG_FILE_NAME_FULL: &str = "/stackable/spark/conf/spark-defaults.conf";

pub const LABEL_NAME_INSTANCE: &str = "app.kubernetes.io/instance";
