use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use uuid::Uuid;

use crate::{Error, FeatureRegistry, VarSource};

use super::api_models::{self, CreationResponse};

#[derive(Clone, Debug)]
pub struct FeathrApiClient {
    registry_endpoint: String,
    client: reqwest::Client,
}

impl FeathrApiClient {
    pub fn new(registry_url: &str) -> Self {
        Self {
            registry_endpoint: registry_url.to_string(),
            client: Default::default(),
        }
    }
    /**
     * Create Api Client from a VarSource
     */
    pub async fn from_var_source(
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<Self, crate::Error> {
        Ok(Self {
            registry_endpoint: var_source
                .get_environment_variable(&["feature_registry", "endpoint"])
                .await?,
            client: Default::default(),
        })
    }
}

#[allow(unused_variables)]
#[async_trait]
impl FeatureRegistry for FeathrApiClient {
    async fn load_project(&self, name: &str) -> Result<api_models::EntityLineage, Error> {
        let url = format!("{}/projects/{}/lineage", self.registry_endpoint, name);
        debug!("URL: {}", url);
        Ok(self.client.get(url).send().await?.json().await?)
    }
    async fn new_project(&self, definition: api_models::ProjectDef) -> Result<Uuid, Error> {
        let url = format!("{}/projects", self.registry_endpoint);
        debug!("ProjectDef: {}", serde_json::to_string(&definition).unwrap());
        let r: CreationResponse = self
            .client
            .post(url)
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok(r.guid)
    }

    async fn new_source(
        &self,
        project_id: Uuid,
        definition: api_models::SourceDef,
    ) -> Result<Uuid, Error> {
        let url = format!("{}/projects/{}/datasources", self.registry_endpoint, project_id);
        debug!("SourceDef: {}", serde_json::to_string(&definition).unwrap());
        let r: CreationResponse = self
            .client
            .post(url)
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok(r.guid)
    }

    async fn new_anchor(
        &self,
        project_id: Uuid,
        definition: api_models::AnchorDef,
    ) -> Result<Uuid, Error> {
        let url = format!("{}/projects/{}/anchors", self.registry_endpoint, project_id);
        debug!("AnchorDef: {}", serde_json::to_string(&definition).unwrap());
        let r: CreationResponse = self
            .client
            .post(url)
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok(r.guid)
    }

    async fn new_anchor_feature(
        &self,
        project_id: Uuid,
        anchor_id: Uuid,
        definition: api_models::AnchorFeatureDef,
    ) -> Result<Uuid, Error> {
        let url = format!("{}/projects/{}/anchors/{}/features", self.registry_endpoint, project_id, anchor_id);
        debug!("AnchorFeatureDef: {}", serde_json::to_string(&definition).unwrap());
        let r: CreationResponse = self
            .client
            .post(url)
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok(r.guid)
    }

    async fn new_derived_feature(
        &self,
        project_id: Uuid,
        definition: api_models::DerivedFeatureDef,
    ) -> Result<Uuid, Error> {
        let url = format!("{}/projects/{}/derivedfeatures", self.registry_endpoint, project_id);
        debug!("DerivedFeatureDef: {}", serde_json::to_string(&definition).unwrap());
        let r: CreationResponse = self
            .client
            .post(url)
            .json(&definition)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Entity created, id: {}", r.guid);
        Ok(r.guid)
    }
}
