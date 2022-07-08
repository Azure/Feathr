use async_trait::async_trait;
use uuid::Uuid;

use crate::Error;

mod feathr_api_client;
pub mod api_models;

pub use feathr_api_client::FeathrApiClient;

// TODO:
#[async_trait]
pub trait FeatureRegistry: Send + Sync {
    async fn load_project(&self, name: &str) -> Result<api_models::EntityLineage, Error>;
    async fn new_project(&self, definition: api_models::ProjectDef) -> Result<Uuid, Error>;
    async fn new_source(&self, project_id: Uuid, definition: api_models::SourceDef) -> Result<Uuid, Error>;
    async fn new_anchor(&self, project_id: Uuid, definition: api_models::AnchorDef) -> Result<Uuid, Error>;
    async fn new_anchor_feature(&self, project_id: Uuid, anchor_id: Uuid, definition: api_models::AnchorFeatureDef) -> Result<Uuid, Error>;
    async fn new_derived_feature(&self, project_id: Uuid, definition: api_models::DerivedFeatureDef) -> Result<Uuid, Error>;
}