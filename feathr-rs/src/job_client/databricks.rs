use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use dbfs_client::DbfsClient;
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

use crate::{Error, JobClient, JobId, JobStatus, SubmitJobRequest, VarSource};

#[async_trait]
trait LoggedResponse {
    async fn detailed_error_for_status(self) -> Result<Self, Error>
    where
        Self: Sized;
}

#[async_trait]
impl LoggedResponse for reqwest::Response {
    async fn detailed_error_for_status(self) -> Result<Self, Error> {
        if self.status().is_client_error() || self.status().is_server_error() {
            let url = self.url().to_string();
            let status = self.status().to_string();
            let text = self.text().await?;
            Err(
                match serde_json::from_str::<DatabricksErrorResponse>(&text) {
                    Ok(resp) => Error::DatabricksApiError(resp.error_code, resp.message),
                    Err(_) => Error::DatabricksHttpError(url, status, text),
                },
            )
        } else {
            Ok(self)
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct DatabricksErrorResponse {
    pub error_code: String,
    pub message: String,
}

#[derive(Debug)]
pub struct DatabricksClient {
    url_base: String,
    dbfs: DbfsClient,
    client: reqwest::Client,
    workspace_dir: String,
    cluster: Cluster,
    maven_artifact: String,
}

impl DatabricksClient {
    pub fn new(
        url_base: &str,
        token: &str,
        workspace_dir: &str,
        cluster: Option<Cluster>,
        maven_artifact: &str,
    ) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        if !token.is_empty() {
            headers.insert(
                "Authorization",
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
            );
        }
        Self {
            url_base: format!("{}/api/2.0", url_base.trim_end_matches("/")),
            dbfs: DbfsClient::new(url_base, token),
            client: reqwest::ClientBuilder::new()
                .default_headers(headers)
                .build()
                .unwrap(),
            workspace_dir: workspace_dir.to_string(),
            cluster: cluster.unwrap_or(Cluster::NewCluster(NewCluster {
                num_workers: 2,
                spark_version: "9.1.x-scala2.12".to_string(),
                node_type_id: "Standard_D4_v2".to_string(),
                spark_conf: Default::default(),
                custom_tags: Default::default(),
            })),
            maven_artifact: maven_artifact.to_string(),
        }
    }

    async fn get_run_status(
        &self,
        id: u64,
    ) -> Result<(JobStatus, String, Option<HashMap<String, String>>), Error> {
        let url = format!("{}/jobs/runs/get-output?run_id={}", self.url_base, id);
        let resp: GetRunOutputResponse = self
            .client
            .get(url)
            .send()
            .await?
            .detailed_error_for_status()
            .await?
            .json()
            .await?;
        debug!("Status response: {:#?}", resp);
        let status = match resp.metadata.state.life_cycle_state {
            RunLifeCycleState::Pending => JobStatus::Starting,
            RunLifeCycleState::Running | RunLifeCycleState::Terminating => JobStatus::Running,
            RunLifeCycleState::Terminated => match resp.metadata.state.result_state {
                Some(RunResultState::Success) => JobStatus::Success,
                _ => JobStatus::Failed,
            },
            RunLifeCycleState::Skipped | RunLifeCycleState::InternalError => JobStatus::Failed,
        };

        Ok((
            status,
            vec![
                resp.error.map(|s| format!("{}\n", s)).unwrap_or_default(),
                resp.logs.map(|s| format!("{}\n", s)).unwrap_or_default(),
                resp.error_trace
                    .map(|s| format!("{}\n", s))
                    .unwrap_or_default(),
            ]
            .join(""),
            match resp.metadata.cluster_spec.cluster {
                Cluster::ExistingClusterId(_) => {
                    warn!("Cannot get output directory from existing cluster");
                    Default::default()
                }
                Cluster::NewCluster(nc) => nc.custom_tags,
            }
            .to_owned(),
        ))
    }

    pub(crate) async fn from_var_source(
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<Self, crate::Error> {
        let workspace_dir = var_source
            .get_environment_variable(&["spark_config", "databricks", "work_dir"])
            .await?
            .as_str()
            .trim_start_matches("dbfs:/")
            .to_string();

        let url_base = var_source
            .get_environment_variable(&["spark_config", "databricks", "workspace_instance_url"])
            .await?
            .as_str()
            .trim_end_matches("/")
            .to_string();

        let token = var_source
            .get_environment_variable(&["DATABRICKS_WORKSPACE_TOKEN_VALUE"])
            .await?;

        #[derive(Debug, Deserialize)]
        struct ConfigTemplate {
            #[serde(flatten)]
            cluster: Cluster,
        }

        let value: serde_yaml::Value = serde_yaml::from_str(
            &var_source
                .get_environment_variable(&["spark_config", "databricks", "config_template"])
                .await?,
        )?;

        debug!("{:#?}", value);

        let config_template = serde_yaml::from_value::<ConfigTemplate>(value.to_owned())?;
        let nc = config_template.cluster;

        let maven_artifact = var_source
            .get_environment_variable(&["spark_config", "maven_artifact"])
            .await
            .ok()
            .map(|s| if s.is_empty() {
                super::FEATHR_MAVEN_ARTIFACT.to_string()
            } else {
                s
            })
            .unwrap_or(super::FEATHR_MAVEN_ARTIFACT.to_string());
        debug!("Maven artifact: {}", maven_artifact);

        Ok(Self::new(
            &url_base,
            &token,
            &workspace_dir,
            Some(nc),
            &maven_artifact,
        ))
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum RunLifeCycleState {
    Pending,
    Running,
    Terminating,
    Terminated,
    Skipped,
    InternalError,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum RunResultState {
    Success,
    Failed,
    Timedout,
    Canceled,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct RunState {
    life_cycle_state: RunLifeCycleState,
    result_state: Option<RunResultState>,
    // Other fields omitted
}

#[derive(Clone, Debug, Deserialize)]
struct RunMetadata {
    state: RunState,
    cluster_spec: ClusterSpec,
    // Other fields omitted
}

#[derive(Clone, Debug, Deserialize)]
struct ClusterSpec {
    #[serde(flatten)]
    cluster: Cluster,
}

#[derive(Clone, Debug, Deserialize)]
struct GetRunOutputResponse {
    metadata: RunMetadata,
    error: Option<String>,
    logs: Option<String>,
    error_trace: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SubmitRunRequest {
    tasks: Vec<SubmitRunSettings>,
    run_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SubmitRunSettings {
    task_key: String,
    #[serde(flatten)]
    cluster: Cluster,
    #[serde(flatten)]
    task: SparkTask,
    libraries: Vec<Library>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NewCluster {
    #[serde(default)]
    pub num_workers: u32,
    #[serde(default)]
    pub spark_version: String,
    #[serde(default)]
    pub node_type_id: String,
    #[serde(default)]
    pub spark_conf: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_tags: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cluster {
    ExistingClusterId(String),
    NewCluster(NewCluster),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SparkTask {
    SparkJarTask {
        main_class_name: String,
        parameters: Vec<String>,
    },
    SparkPythonTask {
        python_file: String,
        parameters: Vec<String>,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Library {
    Jar(String),
    Egg(String),
    Whl(String),
    Pypi {
        package: String,
        repo: String,
    },
    Maven {
        coordinates: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        repo: Option<String>,
        exclusions: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize)]
struct SubmitRunResponse {
    run_id: u64,
}

#[async_trait]
impl JobClient for DatabricksClient {
    async fn write_remote_file(&self, path: &str, content: &[u8]) -> Result<String, Error> {
        self.dbfs.write_file(path, content).await?;
        Ok(path.to_string())
    }

    async fn read_remote_file(&self, path: &str) -> Result<Bytes, Error> {
        Ok(self.dbfs.read_file(path).await?.into())
    }

    async fn submit_job(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
        request: SubmitJobRequest,
    ) -> Result<JobId, Error> {
        let args = self.get_arguments(var_source.clone(), &request).await?;

        let main_jar_path = if request.main_jar_path.is_none() {
            var_source
                .get_environment_variable(&[
                    "spark_config",
                    "databricks",
                    "feathr_runtime_location",
                ])
                .await
                .ok()
        } else {
            request.main_jar_path
        };

        let mut orig_files: Vec<String> = vec![];
        let mut orig_jars: Vec<String> = match main_jar_path.clone() {
            Some(p) => vec![p],
            None => vec![],
        };

        for f in request.reference_files.into_iter() {
            if f.ends_with(".jar") {
                orig_jars.push(f)
            } else {
                orig_files.push(f)
            }
        }

        debug!("Uploading JARs: {:#?}", orig_jars);
        let jars = self.multi_upload_or_get_url(&orig_jars).await?;
        debug!("JARs uploaded, URLs: {:#?}", jars);

        debug!("Uploading files: {:#?}", orig_files);
        let files = self.multi_upload_or_get_url(&orig_files).await?;
        debug!("Files uploaded, URLs: {:#?}", files);

        debug!("Uploading Python files: {:#?}", request.python_files);
        let py_files = self.multi_upload_or_get_url(&request.python_files).await?;
        debug!("Python files uploaded, URLs: {:#?}", py_files);

        let task = if let Some(code) = request.main_python_script {
            let py_url = self
                .write_remote_file(
                    &self.get_remote_url(&format!(
                        "feathr_pyspark_driver_{}_{}.py",
                        request.name,
                        request.job_key.as_simple()
                    )),
                    code.as_bytes(),
                )
                .await?;
            debug!("Main executable file: {}", py_url);
            SparkTask::SparkPythonTask {
                python_file: py_url,
                parameters: args,
            }
        } else {
            debug!("Main class name: {}", request.main_class_name);
            SparkTask::SparkJarTask {
                main_class_name: request.main_class_name,
                parameters: args,
            }
        };

        let mut libraries: Vec<Library> = jars.into_iter().map(|jar| Library::Jar(jar)).collect();

        if main_jar_path.is_none() {
            // Add maven artifact as the dependency
            libraries.push(Library::Maven {
                coordinates: self.maven_artifact.clone(),
                repo: None,
                exclusions: vec![],
            });
        }

        let cluster = match self.cluster.clone() {
            Cluster::NewCluster(mut cluster) => {
                cluster.custom_tags = if request.output.is_empty() {
                    None
                } else {
                    let tags: HashMap<String, String> = [("output".to_string(), request.output)]
                        .into_iter()
                        .collect();
                    Some(tags)
                };
                Cluster::NewCluster(cluster)
            }
            Cluster::ExistingClusterId(cluster_id) => Cluster::ExistingClusterId(cluster_id),
        };

        let job = SubmitRunRequest {
            tasks: vec![SubmitRunSettings {
                task_key: request.job_key.as_simple().to_string(),
                cluster,
                task,
                libraries,
            }],
            run_name: request.name,
        };
        debug!(
            "Job request: {}",
            serde_json::to_string_pretty(&job).unwrap()
        );

        let url = format!("{}/jobs/runs/submit", self.url_base);
        debug!("URL: {}", url);
        let text = self
            .client
            .post(url)
            .json(&job)
            .send()
            .await?
            .detailed_error_for_status()
            .await?
            .text()
            .await?;
        debug!("Response: {}", text);
        let resp: SubmitRunResponse = serde_json::from_str(&text)?;
        debug!("Job submitted, id is {}", resp.run_id);
        Ok(JobId(resp.run_id))
    }

    async fn get_job_status(&self, job_id: JobId) -> Result<JobStatus, Error> {
        Ok(self.get_run_status(job_id.0).await?.0)
    }

    async fn get_job_log(&self, job_id: JobId) -> Result<String, Error> {
        Ok(self.get_run_status(job_id.0).await?.1)
    }

    async fn get_job_output_url(&self, job_id: JobId) -> Result<Option<String>, Error> {
        Ok(self
            .get_run_status(job_id.0)
            .await?
            .2
            .map(|tags| tags.get("output").map(|v| v.to_owned()))
            .flatten())
    }

    async fn upload_or_get_url(&self, path: &str) -> Result<String, Error> {
        let bytes = if path.starts_with("http:") || path.starts_with("https:") {
            // It's a Internet file
            reqwest::Client::new()
                .get(path)
                .send()
                .await?
                .bytes()
                .await?
        } else if path.starts_with("dbfs:/") {
            // It's a file on the storage
            return Ok(path.to_string());
        } else {
            // Local file
            let mut v: Vec<u8> = vec![];
            tokio::fs::File::open(path)
                .await?
                .read_to_end(&mut v)
                .await?;
            Bytes::from(v)
        };
        let url = self.get_remote_url(&self.get_file_name(path)?);
        self.write_remote_file(&url, &bytes).await
    }

    fn get_remote_url(&self, filename: &str) -> String {
        format!(
            "dbfs:/{}",
            [self.workspace_dir.as_str().trim_end_matches("/"), filename]
                .join("/")
                .trim_start_matches("/")
                .to_string()
        )
    }

    fn is_url_on_storage(&self, url: &str) -> bool {
        url.starts_with("dbfs:")
    }
}

#[cfg(test)]

mod tests {
    use super::*;

    #[test]
    fn ser_spark_run() {
        let lib = vec![
            Library::Jar("abc.jar".to_string()),
            Library::Jar("def.jar".to_string()),
        ];
        println!("{}", serde_json::to_string_pretty(&lib).unwrap());

        let x = SubmitRunSettings {
            task_key: uuid::Uuid::new_v4().to_string(),
            cluster: Cluster::NewCluster(NewCluster {
                num_workers: 2,
                spark_version: "9.1.x-scala2.12".to_string(),
                node_type_id: "Standard_D3_v2".to_string(),
                spark_conf: Default::default(),
                custom_tags: None,
            }),
            task: SparkTask::SparkJarTask {
                main_class_name: "mainClassName".to_string(),
                parameters: vec!["arg1".to_string(), "arg2".to_string(), "arg3".to_string()],
            },
            libraries: lib,
        };
        println!("{}", serde_json::to_string_pretty(&x).unwrap());
    }

    #[test]
    fn cluster_conf() {
        #[derive(Debug, Deserialize)]
        struct ConfigTemplate {
            #[serde(flatten)]
            cluster: Cluster,
        }

        let s = r#"{'run_name':'','new_cluster':{'spark_version':'9.1.x-scala2.12','node_type_id':'Standard_D3_v2','num_workers':2,'spark_conf':{}},'libraries':[{'jar':''}],'spark_jar_task':{'main_class_name':'','parameters':['']}}"#;

        let ct: ConfigTemplate = serde_yaml::from_str(s).unwrap();
        match ct.cluster {
            Cluster::NewCluster(c) => {
                assert_eq!(c.num_workers, 2)
            }
            _ => assert!(false),
        }

        let s = r#"{'run_name':'','existing_cluster_id':'spark31','libraries':[{'jar':''}],'spark_jar_task':{'main_class_name':'','parameters':['']}}"#;

        let ct: ConfigTemplate = serde_yaml::from_str(s).unwrap();
        match ct.cluster {
            Cluster::ExistingClusterId(s) => {
                assert_eq!(s, "spark31")
            }
            _ => assert!(false),
        }
    }
}
