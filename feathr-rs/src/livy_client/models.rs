use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn is_default<T>(v: &T) -> bool
where
    T: Default + Eq,
{
    v == &T::default()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterSize {
    pub driver_cores: u64,
    pub driver_memory: String,
    pub executor_cores: u64,
    pub executor_memory: String,
    pub num_executors: u64,
}

/**
 * Some pre-defined cluster sizes
 */
#[allow(non_snake_case)]
impl ClusterSize {
    pub fn SMALL() -> Self {
        todo!()
    }
    
    pub fn MEDIUM() -> Self {
        ClusterSize {
            driver_memory: "4g".to_string(),
            driver_cores: 2,
            executor_memory: "4g".to_string(),
            executor_cores: 2,
            num_executors: 2,
        }
    }

    pub fn LARGE() -> Self {
        todo!()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivyStates {
    Busy,
    Dead,
    Error,
    Idle,
    Killed,
    NotStarted,
    Recovering,
    Running,
    ShuttingDown,
    Starting,
    Success,
}

impl Default for LivyStates {
    fn default() -> Self {
        Self::NotStarted
    }
}

impl std::fmt::Display for LivyStates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PluginCurrentState {
    Cleanup,
    Ended,
    Monitoring,
    Preparation,
    Queued,
    ResourceAcquisition,
    Submission,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SchedulerCurrentState {
    Ended,
    Queued,
    Scheduled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SparkErrorSource {
    None,
    Dependency,
    System,
    Unknown,
    User,
}

impl Default for SparkErrorSource {
    fn default() -> Self {
        SparkErrorSource::None
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SparkJobType {
    SparkBatch,
    SparkSession,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkRequest {
    #[serde(skip_serializing_if = "is_default")]
    pub archives: Vec<String>,
    #[serde(skip_serializing_if = "is_default")]
    pub args: Vec<String>,
    #[serde(skip_serializing_if = "is_default")]
    pub artifact_id: String,
    #[serde(skip_serializing_if = "is_default")]
    pub class_name: String,
    #[serde(skip_serializing_if = "is_default")]
    pub conf: HashMap<String, String>,
    #[serde(flatten)]
    pub cluster_size: ClusterSize,
    #[serde(skip_serializing_if = "is_default")]
    pub file: String,
    #[serde(skip_serializing_if = "is_default")]
    pub files: Vec<String>,
    #[serde(skip_serializing_if = "is_default")]
    pub jars: Vec<String>,
    pub name: String,
    #[serde(skip_serializing_if = "is_default")]
    pub py_files: Vec<String>,
    #[serde(skip_serializing_if = "is_default")]
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkScheduler {
    pub cancellation_requested_at: Option<DateTime<Utc>>,
    pub current_state: SchedulerCurrentState,
    pub ended_at: Option<DateTime<Utc>>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub submitted_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkServiceError {
    pub error_code: String,
    pub message: String,
    pub source: SparkErrorSource,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkServicePlugin {
    pub cleanup_started_at: Option<DateTime<Utc>>,
    pub current_state: PluginCurrentState,
    pub monitoring_started_at: Option<DateTime<Utc>>,
    pub preparation_started_at: Option<DateTime<Utc>>,
    pub resource_acquisition_started_at: Option<DateTime<Utc>>,
    pub submission_started_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SparkJobState {
    pub busy_at: Option<DateTime<Utc>>,
    pub current_state: LivyStates,
    pub dead_at: Option<DateTime<Utc>>,
    pub error_at: Option<DateTime<Utc>>,
    pub idle_at: Option<DateTime<Utc>>,
    // pub job_creation_request: SparkRequest,
    pub killed_at: Option<DateTime<Utc>>,
    pub not_started_at: Option<DateTime<Utc>>,
    pub recovering_at: Option<DateTime<Utc>>,
    pub shutting_down_at: Option<DateTime<Utc>>,
    pub starting_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SparkJobResult {
    Cancelled,
    Failed,
    Succeeded,
    Uncertain,
}

impl Default for SparkJobResult {
    fn default() -> Self {
        Self::Uncertain
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct SparkJob {
    pub app_id: Option<String>,
    pub app_info: Option<HashMap<String, Option<String>>>,
    pub artifact_id: Option<String>,
    pub error_info: Vec<SparkServiceError>,
    pub id: u64,
    pub job_type: Option<SparkJobType>,
    pub livy_info: Option<SparkJobState>,
    pub log: Option<Vec<String>>,
    pub name: Option<String>,
    pub plugin_info: Option<SparkServicePlugin>,
    pub result: SparkJobResult,
    pub scheduler_info: Option<SparkScheduler>,
    pub spark_pool_name: String,
    pub state: LivyStates,
    pub submitter_id: String,
    pub submitter_name: String,
    pub tags: Option<HashMap<String, String>>,
    pub workspace_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LivyStatementStates {
    Available,
    Cancelled,
    Cancelling,
    Error,
    Running,
    Waiting,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SparkStatement {
    pub code: String,
    pub id: u64,
    pub state: LivyStatementStates,
    pub output: LivyStatementOutput,
    pub progress: f64,
    pub started: u64,
    pub completed: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct LivyStatementOutput {
    pub data: HashMap<String, String>,
    pub ename: String,
    pub evalue: String,
    pub execution_count: u64,
    pub status: String,
    pub traceback: Vec<String>,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SparkStatementLanguageType {
    Dotnetspark,
    Pyspark,
    Spark,
    Sql,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SparkStatementOptions {
    pub code: String,
    pub kind: SparkStatementLanguageType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BigDataPoolReference {
    pub reference_name: String,
    #[serde(rename = "type")]
    pub reference_type: BigDataPoolReferenceType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BigDataPoolReferenceType {
    BigDataPoolReference,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloudError {
    pub error: CloudErrorBody,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloudErrorBody {
    pub code: String,
    // details: Vec<CloudError>,
    pub message: String,
    pub target: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Folder {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkJobDefinition {
    pub description: String,
    pub folder: Folder,
    pub job_properties: SparkRequest,
    pub language: String,
    pub required_spark_version: String,
    pub target_big_data_pool: BigDataPoolReference,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SparkJobDefinitionResource {
    pub etag: String,
    pub id: String,
    pub name: String,
    pub properties: SparkJobDefinition,
    #[serde(rename = "type")]
    pub resource_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkJobCollection {
    pub from: usize,
    pub total: usize,
    pub sessions: Vec<SparkJob>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SparkStatementCollection {
    pub statements: Vec<SparkStatement>,
    pub total_statements: usize,
}