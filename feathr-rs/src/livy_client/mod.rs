mod error;
mod models;

mod azure_synapse;

use async_trait::async_trait;
use log::{debug, trace};
use reqwest::{RequestBuilder, Response};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

pub use error::{LivyClientError, Result};
pub use models::*;

pub use azure_synapse::*;

/// Log if `Result` is an error
trait Logged {
    fn log(self) -> Self;
}

impl<T, E> Logged for std::result::Result<T, E>
where
    E: std::fmt::Debug,
{
    fn log(self) -> Self {
        if let Err(e) = &self {
            trace!("---TraceError--- {:#?}", e)
        }
        self
    }
}

/**
 * Reqwest::error_for_status doesn't log response body, which makes debug much harder.
 */
async fn get_response(url: &str, resp: Response) -> Result<String> {
    let status = resp.status();
    let text = resp.text().await.log()?;
    debug!("Status: {}", status);
    trace!("Response: {}", text);
    if status.is_client_error() || status.is_server_error() {
        Err(LivyClientError::HttpError(url.to_string(), status, text))
    } else {
        Ok(text)
    }
}

macro_rules! http_req {
    ($method:ident) => {
        async fn $method<REQ, RESP>(&self, url: &str, req: Option<REQ>) -> Result<RESP>
        where
            REQ: Serialize,
            RESP: DeserializeOwned,
        {
            debug!("URL: {}", url);
            debug!("Method: {}", stringify!($method));
            debug!("Request: {}", serde_json::to_string_pretty(&req).unwrap());
            let builder = self
                .authenticator
                .authenticate(self.client.$method(url))
                .await?;
            let resp = match req {
                Some(r) => builder.json(&r),
                None => builder,
            }
            .send()
            .await
            .log()?;
            Ok(serde_json::from_str(&get_response(url, resp).await?)?)
        }
    };
}

/**
 * The authenticator trait is used by LivyClient to handle the authentication process,
 * Mostly used by Azure Synapse, which is an extended Livy API service with AAD Auth.
 */
#[async_trait]
pub trait Authenticator {
    async fn authenticate(
        &self,
        builder: RequestBuilder,
    ) -> std::result::Result<RequestBuilder, error::LivyClientError>;
}

/**
 * No auth
 */
pub struct DummyAuthenticator;

#[async_trait]
impl Authenticator for DummyAuthenticator {
    async fn authenticate(
        &self,
        builder: RequestBuilder,
    ) -> std::result::Result<RequestBuilder, LivyClientError> {
        Ok(builder)
    }
}

/**
 * HTTP Basic Auth with username and password
 */
pub struct BasicAuthenticator {
    username: String,
    password: Option<String>,
}

#[async_trait]
impl Authenticator for BasicAuthenticator {
    async fn authenticate(
        &self,
        builder: RequestBuilder,
    ) -> std::result::Result<RequestBuilder, LivyClientError> {
        Ok(builder.basic_auth(self.username.clone(), self.password.clone()))
    }
}

/**
 * Livy API client
 */
#[derive(Debug)]
pub struct LivyClient<T: Authenticator> {
    client: reqwest::Client,
    url_base: String,
    log_base: String,
    authenticator: T,
}

impl<T: Authenticator> LivyClient<T> {
    /**
     * Create a new Livy API Client
     * User should provide a customized reqwest::Client, or just use default one.
     * Without `log_base` set correctly, the client cannot fetch driver logs from the server,
     * but other functions are not affected.
     */
    pub fn new(
        client: reqwest::Client,
        url_base: &str,
        log_base: &str,
    ) -> LivyClient<DummyAuthenticator> {
        LivyClient {
            client,
            url_base: Self::remove_trailing_slash(url_base),
            log_base: Self::remove_trailing_slash(log_base),
            authenticator: DummyAuthenticator,
        }
    }

    /**
     * Create Livy API client with customized authenticator
     */
    pub fn with_authenticator<A: Authenticator>(
        client: reqwest::Client,
        url_base: &str,
        log_base: &str,
        authenticator: A,
    ) -> LivyClient<A> {
        LivyClient {
            client,
            url_base: Self::remove_trailing_slash(url_base),
            log_base: Self::remove_trailing_slash(log_base),
            authenticator,
        }
    }

    pub async fn get_sessions(&self) -> Result<Vec<SparkJob>> {
        let mut ret: Vec<SparkJob> = vec![];
        let mut from = 0usize;
        loop {
            let resp = self
                .get::<(), SparkJobCollection>(
                    &format!("{}/sessions?from={from}&detailed=true", self.url_base),
                    None,
                )
                .await?;
            if resp.sessions.is_empty() {
                break;
            }
            ret.extend(resp.sessions.into_iter());
            if ret.len() >= resp.total {
                break;
            }
            from = ret.len();
        }
        Ok(ret)
    }

    pub async fn create_session(&self, session: SparkRequest) -> Result<SparkJob> {
        self.post(
            &format!("{}/sessions?detailed=true", self.url_base),
            Some(session),
        )
        .await
    }

    pub async fn get_session(&self, id: u64) -> Result<SparkJob> {
        self.get::<(), _>(
            &format!("{}/sessions/{}?detailed=true", self.url_base, id),
            None,
        )
        .await
    }

    pub async fn cancel_session(&self, id: u64) -> Result<()> {
        self.delete(&format!("{}/sessions/{}?detailed=true", self.url_base, id))
            .await
    }

    pub async fn get_session_statements(&self, session_id: u64) -> Result<Vec<SparkStatement>> {
        Ok(self
            .get::<(), SparkStatementCollection>(
                &format!(
                    "{}/sessions/{}/statements?detailed=true",
                    self.url_base, session_id
                ),
                None,
            )
            .await?
            .statements)
    }

    pub async fn create_session_statement(
        &self,
        session_id: u64,
        code: &str,
        kind: SparkStatementLanguageType,
    ) -> Result<SparkStatement> {
        #[derive(Debug, Serialize)]
        struct Request {
            code: String,
            kind: SparkStatementLanguageType,
        }
        self.post::<Request, SparkStatement>(
            &format!(
                "{}/sessions/{}/statements?detailed=true",
                self.url_base, session_id
            ),
            Some(Request {
                code: code.to_string(),
                kind,
            }),
        )
        .await
    }

    pub async fn get_session_statement(
        &self,
        session_id: u64,
        statement_id: u64,
    ) -> Result<SparkStatement> {
        self.get::<(), _>(
            &format!(
                "{}/sessions/{}/statements/{}?detailed=true",
                self.url_base, session_id, statement_id
            ),
            None,
        )
        .await
    }

    pub async fn cancel_session_statement(&self, session_id: u64, statement_id: u64) -> Result<()> {
        self.delete(&format!(
            "{}/sessions/{}/statements/{}?detailed=true",
            self.url_base, session_id, statement_id
        ))
        .await
    }

    /**
     * Get driver log, currently only works with Azure Synapse
     */
    pub async fn get_session_driver_stdout_log(&self, id: u64) -> Result<String> {
        let app_id = self
            .get_session(id)
            .await?
            .app_id
            .ok_or_else(|| LivyClientError::InvalidJobState(id))?;
        self.get_driver_log(id, &app_id, "stdout").await
    }

    pub async fn get_session_driver_stderr_log(&self, id: u64) -> Result<String> {
        let app_id = self
            .get_session(id)
            .await?
            .app_id
            .ok_or_else(|| LivyClientError::InvalidJobState(id))?;
        self.get_driver_log(id, &app_id, "stderr").await
    }

    pub async fn get_batch_jobs(&self) -> Result<Vec<SparkJob>> {
        let mut ret: Vec<SparkJob> = vec![];
        let mut from = 0usize;
        loop {
            let resp = self
                .get::<(), SparkJobCollection>(
                    &format!("{}/batches?from={from}&detailed=true", self.url_base),
                    None,
                )
                .await?;
            if resp.sessions.is_empty() {
                break;
            }
            ret.extend(resp.sessions.into_iter());
            if ret.len() >= resp.total {
                break;
            }
            from = ret.len();
        }
        Ok(ret)
    }

    pub async fn create_batch_job(&self, job: SparkRequest) -> Result<SparkJob> {
        self.post(
            &format!("{}/batches?detailed=true", self.url_base),
            Some(job),
        )
        .await
    }

    pub async fn get_batch_job(&self, id: u64) -> Result<SparkJob> {
        self.get::<(), _>(
            &format!("{}/batches/{}?detailed=true", self.url_base, id),
            None,
        )
        .await
    }

    pub async fn cancel_batch_job(&self, id: u64) -> Result<()> {
        self.delete(&format!("{}/batches/{}?detailed=true", self.url_base, id))
            .await
    }

    /**
     * NOTE: Livy outputs PySpark program error to StdOut instead of StdErr,
     * and the StdErr is flooded by lots of Spark logs, basically useless.
     */
    pub async fn get_batch_job_driver_stdout_log(&self, id: u64) -> Result<String> {
        // @see: https://docs.microsoft.com/en-us/answers/questions/253744/synapse-spark-logs.html
        let app_id = self
            .get_batch_job(id)
            .await?
            .app_id
            .ok_or_else(|| LivyClientError::InvalidJobState(id))?;
        self.get_driver_log(id, &app_id, "stdout").await
    }

    pub async fn get_batch_job_driver_stderr_log(&self, id: u64) -> Result<String> {
        let app_id = self
            .get_batch_job(id)
            .await?
            .app_id
            .ok_or_else(|| LivyClientError::InvalidJobState(id))?;
        self.get_driver_log(id, &app_id, "stderr").await
    }

    fn remove_trailing_slash(s: &str) -> String {
        if s.ends_with('/') {
            &s[0..s.len() - 1]
        } else {
            s
        }
        .to_string()
    }

    async fn get_driver_log(&self, id: u64, app_id: &str, stream: &str) -> Result<String> {
        self.get_raw(&format!(
            "{}/livyid/{}/applications/{}/driverlog/{}/?isDownload=true",
            self.log_base, id, app_id, stream,
        ))
        .await
    }

    async fn get_raw(&self, url: &str) -> Result<String> {
        debug!("URL: {}", url);
        debug!("Method: GET");
        let resp = self
            .authenticator
            .authenticate(self.client.get(url))
            .await?
            .send()
            .await
            .log()?;
        get_response(url, resp).await
    }

    async fn delete(&self, url: &str) -> Result<()> {
        let builder = self
            .authenticator
            .authenticate(self.client.delete(format!("{}{}", self.url_base, url)))
            .await?;
        get_response(url, builder.send().await?).await?;
        Ok(())
    }

    http_req!(get);
    // http_method!(put);
    http_req!(post);
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    fn init() -> LivyClient<AadAuthenticator> {
        crate::tests::init_logger();
        AzureSynapseClientBuilder::default()
            .url(env::var("SYNAPSE_DEV_URL").unwrap())
            .pool(env::var("SYNAPSE_POOL_NAME").unwrap())
            .build()
            .unwrap()
    }

    #[ignore]
    #[tokio::test]
    async fn get_sessions() {
        let client = init();
        let sessions = client.get_sessions().await.unwrap();
        println!("{:#?}", sessions);
    }

    #[ignore]
    #[tokio::test]
    async fn create_session() {
        let client = init();
        let req = SparkRequest {
            artifact_id: "SomeArtifact".to_string(),
            name: "SomeName".to_string(),
            py_files: vec![
                "abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/pyspark-test.py"
                    .to_string(),
            ],
            cluster_size: ClusterSize::MEDIUM(),
            ..Default::default()
        };
        println!("{}", serde_json::to_string_pretty(&req).unwrap());
        let result = client.create_session(req).await.unwrap();
        println!("{:#?}", result);
    }

    #[ignore]
    #[tokio::test]
    async fn get_jobs() {
        let client = init();
        let jobs = client.get_batch_jobs().await.unwrap();
        println!("{:#?}", jobs);
    }

    #[ignore]
    #[tokio::test]
    async fn get_driver_log() {
        let client = init();
        let id = client.get_batch_jobs().await.unwrap()[0].id;

        let stdout = client.get_batch_job_driver_stdout_log(id).await.unwrap();

        println!("StdOut:\n{}", stdout);
    }

    #[ignore]
    #[tokio::test]
    async fn create_job() {
        let client = init();
        let req = SparkRequest {
            artifact_id: "SomeArtifact".to_string(),
            name: "SomeJobName".to_string(),
            file: "abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/pyspark-test.py"
                .to_string(),
            cluster_size: ClusterSize::MEDIUM(),
            ..Default::default()
        };
        println!("{}", serde_json::to_string_pretty(&req).unwrap());
        let result = client.create_batch_job(req).await.unwrap();
        println!("{:#?}", result);
    }
}
