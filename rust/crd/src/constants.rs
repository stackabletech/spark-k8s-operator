//! This module provides constants that are shared via multiple crates.

pub const PORT_NAME_WEB: &str = "http";
pub const PORT_NAME_SPARK: &str = "spark";

pub const MASTER_HTTP_PORT: u16 = 8080;
pub const MASTER_RPC_PORT: u16 = 7077;
pub const WORKER_HTTP_PORT: u16 = 8081;
pub const HISTORY_SERVER_HTTP_PORT: u16 = 18080;

pub const HTTP_PORT_NAME: &str = "http";
pub const RPC_PORT_NAME: &str = "rpc";

pub const CONF_DIR: &str = "/stackable/config";
pub const LOG_DIR: &str = "/stackable/log";

/// Value for the APP_NAME_LABEL label key
pub const APP_NAME: &str = "spark";
pub const SPARK_DEFAULTS_CONF: &str = "spark-defaults.conf";
/// Name of the environment variables file where spark nodes look for configuration data
pub const SPARK_ENV_SH: &str = "spark-env.sh";
/// Name of the metrics properties file to enable e.g. JMX metrics.
pub const SPARK_METRICS_PROPERTIES: &str = "metrics.properties";
/// Basic start up parameter: We need to point the spark nodes to "our" configuration
/// folder. Must be set at all times (before starting the process).
pub const SPARK_CONF_DIR: &str = "SPARK_CONF_DIR";
/// Common parameter: Must be set to true on all nodes (Master, Worker, HistoryServer) to
/// enable node logging to be read and analysed by the HistoryServer.
pub const SPARK_DEFAULTS_EVENT_LOG_ENABLED: &str = "spark.eventLog.enabled";
/// Common parameter: Must be set on nodes (Master, Worker) to point where to write
/// the logs. Should be a common storage path like HDFS, S3 in order for the HistoryServer to read.
pub const SPARK_DEFAULTS_EVENT_LOG_DIR: &str = "spark.eventLog.dir";
/// Common parameter: Must be set to true on all nodes (Master, Worker, HistoryServer) to
/// enable authentication.
pub const SPARK_DEFAULTS_AUTHENTICATE: &str = "spark.authenticate";
/// Common parameter: Must be set on all nodes (Master, Worker, HistoryServer) to activate
/// a secret password which needs to be supplied via spark-submit.
pub const SPARK_DEFAULTS_AUTHENTICATE_SECRET: &str = "spark.authenticate.secret";
/// Common parameter: Must be set to '0' on all nodes (Master, Worker, HistoryServer) to disable
/// automatic port search. Otherwise the nodes will increase their given port if it's already in use.
pub const SPARK_DEFAULTS_PORT_MAX_RETRIES: &str = "spark.port.maxRetries";
/// Master specific parameter: Set the master port in environment variables.
pub const SPARK_DEFAULTS_MASTER_PORT: &str = "spark.master.port";
/// Master specific parameter: Set the master port in environment variables.
pub const SPARK_ENV_MASTER_PORT: &str = "SPARK_MASTER_PORT";
/// Master specific parameter: Set the master web ui port in environment variables.
pub const SPARK_ENV_MASTER_WEBUI_PORT: &str = "SPARK_MASTER_WEBUI_PORT";
/// Worker specific parameter: Set the worker cores in environment variables.
pub const SPARK_ENV_WORKER_CORES: &str = "SPARK_WORKER_CORES";
/// Worker specific parameter: Set the worker memory (500m, 2g) in environment variables.
pub const SPARK_ENV_WORKER_MEMORY: &str = "SPARK_WORKER_MEMORY";
/// Worker specific parameter: Set the worker port in environment variables.
pub const SPARK_ENV_WORKER_PORT: &str = "SPARK_WORKER_PORT";
/// Worker specific parameter: Set the worker web ui port in environment variables.
pub const SPARK_ENV_WORKER_WEBUI_PORT: &str = "SPARK_WORKER_WEBUI_PORT";
/// HistoryServer specific parameter: Set directory where to search for logs. Normally should
/// match the 'SPARK_EVENT_LOG_DIR' set on master and worker nodes
pub const SPARK_DEFAULTS_HISTORY_FS_LOG_DIRECTORY: &str = "spark.history.fs.logDirectory";
/// HistoryServer specific parameter: Set directory to cache application history data. If not set,
/// the data will be kept in memory and is lost after restarts.
pub const SPARK_DEFAULTS_HISTORY_STORE_PATH: &str = "spark.history.store.path";
/// HistoryServer specific parameter: Set HistoryServer web ui port to access the common logs.
pub const SPARK_DEFAULTS_HISTORY_WEBUI_PORT: &str = "spark.history.ui.port";
