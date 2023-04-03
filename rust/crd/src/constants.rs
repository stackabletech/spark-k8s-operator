pub const APP_NAME: &str = "spark-k8s";

pub const VOLUME_MOUNT_NAME_DRIVER_POD_TEMPLATES: &str = "driver-pod-template";
pub const VOLUME_MOUNT_PATH_DRIVER_POD_TEMPLATES: &str = "/stackable/spark/driver-pod-templates";

pub const VOLUME_MOUNT_NAME_EXECUTOR_POD_TEMPLATES: &str = "executor-pod-template";
pub const VOLUME_MOUNT_PATH_EXECUTOR_POD_TEMPLATES: &str =
    "/stackable/spark/executor-pod-templates";

pub const POD_TEMPLATE_FILE: &str = "template.yaml";

pub const VOLUME_MOUNT_NAME_CONFIG: &str = "config";

pub const VOLUME_MOUNT_NAME_JOB: &str = "job-files";
pub const VOLUME_MOUNT_PATH_JOB: &str = "/stackable/spark/jobs";

pub const VOLUME_MOUNT_NAME_REQ: &str = "req-files";
pub const VOLUME_MOUNT_PATH_REQ: &str = "/stackable/spark/requirements";

pub const VOLUME_MOUNT_NAME_LOG_CONFIG: &str = "log-config";
pub const VOLUME_MOUNT_PATH_LOG_CONFIG: &str = "/stackable/log_config";

pub const VOLUME_MOUNT_NAME_LOG: &str = "log";
pub const VOLUME_MOUNT_PATH_LOG: &str = "/stackable/log";

pub const LOG4J2_CONFIG_FILE: &str = "log4j2.properties";

pub const ACCESS_KEY_ID: &str = "accessKey";
pub const SECRET_ACCESS_KEY: &str = "secretKey";
pub const S3_SECRET_DIR_NAME: &str = "/stackable/secrets";

pub const MIN_MEMORY_OVERHEAD: u32 = 384;
pub const JVM_OVERHEAD_FACTOR: f32 = 0.1;
pub const NON_JVM_OVERHEAD_FACTOR: f32 = 0.4;

pub const MAX_SPARK_LOG_FILES_SIZE_IN_MIB: u32 = 10;
pub const MAX_INIT_CONTAINER_LOG_FILES_SIZE_IN_MIB: u32 = 1;
pub const LOG_VOLUME_SIZE_IN_MIB: u32 =
    MAX_SPARK_LOG_FILES_SIZE_IN_MIB + MAX_INIT_CONTAINER_LOG_FILES_SIZE_IN_MIB;

pub const OPERATOR_NAME: &str = "spark.stackable.tech";
pub const CONTROLLER_NAME: &str = "sparkapplication";
pub const POD_DRIVER_CONTROLLER_NAME: &str = "pod-driver";
pub const HISTORY_CONTROLLER_NAME: &str = "history";

pub const HISTORY_ROLE_NAME: &str = "node";

pub const HISTORY_IMAGE_BASE_NAME: &str = "spark-k8s";

pub const HISTORY_CONFIG_FILE_NAME: &str = "spark-defaults.conf";
pub const HISTORY_CONFIG_FILE_NAME_FULL: &str = "/stackable/spark/conf/spark-defaults.conf";

pub const SPARK_CLUSTER_ROLE: &str = "spark-k8s-clusterrole";
