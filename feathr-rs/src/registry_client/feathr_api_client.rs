use std::sync::Arc;

use async_trait::async_trait;
use azure_core::auth::TokenCredential;
use azure_identity::{DefaultAzureCredential, DefaultAzureCredentialBuilder};
use log::debug;
use reqwest::RequestBuilder;
use uuid::Uuid;

use crate::{Error, FeatureRegistry, VarSource};

use super::api_models::{self, CreationResponse};

#[derive(Clone)]
pub struct FeathrApiClient {
    registry_endpoint: String,
    client: reqwest::Client,
    version: usize,
    credential: Option<Arc<DefaultAzureCredential>>,
}

impl std::fmt::Debug for FeathrApiClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeathrApiClient")
            .field("registry_endpoint", &self.registry_endpoint)
            .field("client", &self.client)
            .field("version", &self.version)
            .finish()
    }
}

impl FeathrApiClient {
    pub fn new(registry_url: &str, version: usize, auth: bool) -> Self {
        Self {
            registry_endpoint: registry_url.to_string(),
            client: Default::default(),
            version,
            credential: if auth {
                Some(Arc::new(
                    DefaultAzureCredentialBuilder::new()
                        .exclude_managed_identity_credential()
                        .build(),
                ))
            } else {
                None
            },
        }
    }
    /**
     * Create Api Client from a VarSource
     */
    pub async fn from_var_source(
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<Self, crate::Error> {
        let auth: bool = var_source
            .get_environment_variable(&["feature_registry", "auth"])
            .await
            .unwrap_or("true".to_string())
            .parse()
            .map_err(|e| crate::Error::InvalidConfig(format!("Invalid api_version, {}", e)))?;
        Ok(Self {
            registry_endpoint: var_source
                .get_environment_variable(&["feature_registry", "api_endpoint"])
                .await?,
            client: Default::default(),
            version: var_source
                .get_environment_variable(&["feature_registry", "api_version"])
                .await
                .unwrap_or("1".to_string())
                .parse()
                .map_err(|e| crate::Error::InvalidConfig(format!("Invalid api_version, {}", e)))?,
            credential: if auth {
                Some(Arc::new(
                    DefaultAzureCredentialBuilder::new()
                        .exclude_managed_identity_credential()
                        .build(),
                ))
            } else {
                None
            },
        })
    }

    async fn auth(&self, builder: RequestBuilder) -> Result<RequestBuilder, Error> {
        Ok(if let Some(cred) = self.credential.clone() {
            debug!("Acquiring token");
            match cred.get_token("https://management.azure.com/").await {
                Ok(res) => {
                    debug!("Token acquired");
                    builder.bearer_auth(res.token.secret())
                }
                Err(e) => {
                    debug!("Failed to acquire token, error is {}", e);
                    builder
                }
            }
        } else {
            builder
        })
    }
}

#[allow(unused_variables)]
#[async_trait]
impl FeatureRegistry for FeathrApiClient {
    async fn load_project(&self, name: &str) -> Result<api_models::EntityLineage, Error> {
        let url = match self.version {
            1 => format!("{}/projects/{}", self.registry_endpoint, name),
            2 => format!("{}/projects/{}/lineage", self.registry_endpoint, name),
            _ => Err(crate::Error::InvalidConfig(format!(
                "Unsupported api_version {}",
                self.version
            )))?,
        };
        debug!("URL: {}", url);
        Ok(self
            .auth(self.client.get(url))
            .await?
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn new_project(&self, definition: api_models::ProjectDef) -> Result<(Uuid, u64), Error> {
        let url = format!("{}/projects", self.registry_endpoint);
        debug!(
            "ProjectDef: {}",
            serde_json::to_string(&definition).unwrap()
        );
        let r: CreationResponse = self
            .auth(self.client.post(url))
            .await?
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok((r.guid, r.version))
    }

    async fn new_source(
        &self,
        project_id: Uuid,
        definition: api_models::SourceDef,
    ) -> Result<(Uuid, u64), Error> {
        let url = format!(
            "{}/projects/{}/datasources",
            self.registry_endpoint, project_id
        );
        debug!("SourceDef: {}", serde_json::to_string(&definition).unwrap());
        let r: CreationResponse = self
            .auth(self.client.post(url))
            .await?
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok((r.guid, r.version))
    }

    async fn new_anchor(
        &self,
        project_id: Uuid,
        definition: api_models::AnchorDef,
    ) -> Result<(Uuid, u64), Error> {
        let url = format!("{}/projects/{}/anchors", self.registry_endpoint, project_id);
        debug!("AnchorDef: {}", serde_json::to_string(&definition).unwrap());
        let r: CreationResponse = self
            .auth(self.client.post(url))
            .await?
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok((r.guid, r.version))
    }

    async fn new_anchor_feature(
        &self,
        project_id: Uuid,
        anchor_id: Uuid,
        definition: api_models::AnchorFeatureDef,
    ) -> Result<(Uuid, u64), Error> {
        let url = format!(
            "{}/projects/{}/anchors/{}/features",
            self.registry_endpoint, project_id, anchor_id
        );
        debug!(
            "AnchorFeatureDef: {}",
            serde_json::to_string(&definition).unwrap()
        );
        let r: CreationResponse = self
            .auth(self.client.post(url))
            .await?
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok((r.guid, r.version))
    }

    async fn new_derived_feature(
        &self,
        project_id: Uuid,
        definition: api_models::DerivedFeatureDef,
    ) -> Result<(Uuid, u64), Error> {
        let url = format!(
            "{}/projects/{}/derivedfeatures",
            self.registry_endpoint, project_id
        );
        debug!(
            "DerivedFeatureDef: {}",
            serde_json::to_string(&definition).unwrap()
        );
        let r: CreationResponse = self
            .auth(self.client.post(url))
            .await?
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok((r.guid, r.version))
    }
}
