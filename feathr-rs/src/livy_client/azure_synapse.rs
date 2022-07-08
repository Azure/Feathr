use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use azure_core::auth::{TokenResponse, TokenCredential};
use azure_identity::DefaultAzureCredential;
use chrono::{DateTime, Duration};
use log::trace;
use oauth2::AccessToken;
use reqwest::RequestBuilder;
use thiserror::Error;

use super::{Authenticator, LivyClient, LivyClientError, Result};

#[derive(Debug, Error)]
pub enum AzureSynapseError {
    #[error("Missing Url")]
    MissingSynapseUrl,

    #[error("Missing Pool Name")]
    MissingSynapsePool,

    #[error(transparent)]
    DefaultCredentialError(#[from] azure_core::error::Error),
}

pub struct AadAuthenticator {
    credential: DefaultAzureCredential,
    token: Arc<RwLock<TokenResponse>>,
}

impl std::fmt::Debug for AadAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AadAuthenticator").finish()
    }
}

impl AadAuthenticator {
    async fn fetch_token(&self) -> Result<()> {
        // @see: https://docs.microsoft.com/en-us/azure/synapse-analytics/spark/connect-monitor-azure-synapse-spark-application-level-metrics
        let resp = self
            .credential
            .get_token("https://dev.azuresynapse.net")
            .await
            .map_err(|e| AzureSynapseError::from(e))?;
        // CAUTION: For development only, to be removed
        trace!("Token: {}", &resp.token.secret());
        *(self.token.write()?) = resp;
        Ok(())
    }

    async fn get_token(&self) -> Result<String> {
        // 30 seconds ahead of the real expiration, make sure most in-progress operations can be completed w/o trouble.
        if self.token.read()?.expires_on - Duration::seconds(30) < chrono::Utc::now() {
            self.fetch_token().await?;
        }
        Ok(self.token.read()?.token.secret().to_owned())
    }
}

#[async_trait]
impl Authenticator for AadAuthenticator {
    async fn authenticate(
        &self,
        builder: RequestBuilder,
    ) -> std::result::Result<RequestBuilder, LivyClientError> {
        Ok(builder.bearer_auth(self.get_token().await?))
    }
}

pub struct AzureSynapseClientBuilder {
    credential: DefaultAzureCredential,
    api_version: String,
    url: Option<String>,
    pool: Option<String>,
}

impl AzureSynapseClientBuilder {
    pub fn with_credential(credential: DefaultAzureCredential) -> Result<Self> {
        Ok(Self {
            credential,
            api_version: "2022-02-22-preview".to_string(),
            url: None,
            pool: None,
        })
    }

    pub fn api_version<T>(&mut self, version: T) -> &mut Self
    where
        T: AsRef<str>,
    {
        self.api_version = version.as_ref().to_string();
        self
    }

    pub fn url<T>(mut self, url: T) -> Self
    where
        T: AsRef<str>,
    {
        self.url = Some(url.as_ref().to_string());
        self
    }

    pub fn pool<T>(mut self, pool: T) -> Self
    where
        T: AsRef<str>,
    {
        self.pool = Some(pool.as_ref().to_string());
        self
    }

    pub fn build(self) -> Result<LivyClient<AadAuthenticator>> {
        let url = self
            .url
            .as_ref()
            .ok_or_else(|| AzureSynapseError::MissingSynapseUrl)?;
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| AzureSynapseError::MissingSynapsePool)?;
        // It expired long long ago
        let t = chrono::NaiveDateTime::from_timestamp(0, 0);
        let auth = AadAuthenticator {
            credential: self.credential,
            token: Arc::new(RwLock::new(TokenResponse::new(
                AccessToken::new(Default::default()),
                DateTime::from_utc(t, chrono::Utc),
            ))),
        };

        Ok(LivyClient {
            client: reqwest::Client::new(),
            url_base: format!(
                "{}/livyApi/versions/{}/sparkpools/{}",
                url, self.api_version, pool
            ),
            log_base: format!("{}/sparkhistory/api/v1/sparkpools/{}", url, pool),
            authenticator: auth,
        })
    }
}

impl Default for AzureSynapseClientBuilder {
    fn default() -> Self {
        Self {
            credential: Default::default(),
            api_version: "2022-02-22-preview".to_string(),
            url: None,
            pool: None,
        }
    }
}
