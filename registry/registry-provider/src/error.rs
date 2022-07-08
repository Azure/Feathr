use std::fmt::Debug;

use serde::{Serialize, Deserialize};
use thiserror::Error;
use uuid::Uuid;

use crate::EntityType;

#[derive(Clone, Debug, Error, Serialize, Deserialize)]
pub enum RegistryError {
    #[error("Entity[{0}] has incorrect type {1:?}")]
    WrongEntityType(Uuid, EntityType),

    #[error("Entity[{0}] not found")]
    EntityNotFound(String),

    #[error("Entity with name {0} already exists")]
    EntityNameExists(String),

    #[error("Entity[{0}] already exists")]
    EntityIdExists(Uuid),

    #[error("Entity[{0}] doesn't exist")]
    InvalidEntity(Uuid),

    #[error("Invalid edge from [{0:?}] to [{1:?}]")]
    InvalidEdge(EntityType, EntityType),

    #[error("Cannot delete [{0}] when it still has dependents")]
    DeleteInUsed(Uuid),

    #[error("{0}")]
    FtsError(String),

    #[error("{0}")]
    ExternalStorageError(String),
}

