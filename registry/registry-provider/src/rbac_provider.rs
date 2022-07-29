use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::RegistryError;

use std::str::FromStr;

use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Credential {
    RbacDisabled,
    User(String),
    App(Uuid),
}

impl ToString for Credential {
    fn to_string(&self) -> String {
        match self {
            Credential::RbacDisabled => "*".to_string(),
            Credential::User(user) => user.clone(),
            Credential::App(app) => app.to_string(),
        }
    }
}

impl FromStr for Credential {
    type Err = RegistryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(uuid) = Uuid::from_str(s) {
            Ok(Credential::App(uuid))
        } else {
            Ok(Credential::User(s.to_string()))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

impl ToString for Permission {
    fn to_string(&self) -> String {
        match self {
            Permission::Read => "consumer",
            Permission::Write => "producer",
            Permission::Admin => "admin",
        }
        .to_string()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Resource {
    Global,
    // So far only project is used
    NamedEntity(String),
    Entity(Uuid),
}

impl ToString for Resource {
    fn to_string(&self) -> String {
        match self {
            Resource::Global => "global".to_string(),
            Resource::NamedEntity(name) => name.clone(),
            Resource::Entity(uuid) => uuid.to_string(),
        }
    }
}

impl FromStr for Resource {
    type Err = RegistryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.to_lowercase() == "global" {
            Ok(Resource::Global)
        } else if let Ok(uuid) = Uuid::from_str(s) {
            Ok(Resource::Entity(uuid))
        } else {
            Ok(Resource::NamedEntity(s.to_string()))
        }
    }
}

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum RbacError {
    #[error("Credential {0} not found")]
    CredentialNotFound(String),

    #[error("Resource {0} not found")]
    ResourceNotFound(String),

    #[error("Credential {0} doesn't have {2:?} permission to resource {1:?}")]
    PermissionDenied(String, Resource, Permission),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RbacRecord {
    pub credential: Credential,
    pub resource: Resource,
    pub permission: Permission,
    pub requestor: Credential,
    pub reason: String,
    pub time: DateTime<Utc>,
}

#[async_trait]
pub trait RbacProvider: Send + Sync {
    fn check_permission(
        &self,
        credential: &Credential,
        resource: &Resource,
        permission: Permission,
    ) -> Result<bool, RegistryError>;

    fn load_permissions<RI>(&mut self, permissions: RI) -> Result<(), RegistryError>
    where
        RI: Iterator<Item = RbacRecord>;

    fn get_permissions(&self) -> Result<Vec<RbacRecord>, RegistryError>;

    async fn grant_permission(&mut self, grant: &RbacRecord) -> Result<(), RegistryError>;

    async fn revoke_permission(&mut self, revoke: &RbacRecord) -> Result<(), RegistryError>;
}
