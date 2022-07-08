use common_utils::Logged;
use poem::{error::ResponseError, http::StatusCode};
use registry_provider::RegistryError;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Clone, Debug, Error, Serialize, Deserialize)]
pub enum ApiError {
    #[error("Entity('{0}') is not found")]
    NotFoundError(String),

    #[error("{0}")]
    Conflict(String),

    #[error("{0}")]
    BadRequest(String),

    #[error("{0}")]
    Forbidden(String),

    #[error("{0}")]
    InternalError(String),
}

impl ResponseError for ApiError {
    fn status(&self) -> poem::http::StatusCode {
        match &self {
            ApiError::NotFoundError(_) => StatusCode::NOT_FOUND,
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<RegistryError> for ApiError {
    fn from(e: RegistryError) -> Self {
        match e {
            RegistryError::WrongEntityType(id, _) => ApiError::NotFoundError(id.to_string()),
            RegistryError::EntityNotFound(e) => ApiError::NotFoundError(e),
            RegistryError::InvalidEntity(id) => ApiError::NotFoundError(id.to_string()),
            RegistryError::InvalidEdge(_, _) => ApiError::InternalError(format!("{:?}", e)),
            RegistryError::EntityNameExists(_) => ApiError::Conflict(format!("{:?}", e)),
            RegistryError::EntityIdExists(_) => ApiError::Conflict(format!("{:?}", e)),
            RegistryError::DeleteInUsed(_) => ApiError::BadRequest(format!("{:?}", e)),
            RegistryError::FtsError(_) => ApiError::InternalError(format!("{:?}", e)),
            RegistryError::ExternalStorageError(_) => ApiError::InternalError(format!("{:?}", e)),
        }
    }
}

pub trait IntoApiResult<T> {
    fn map_api_error(self) -> Result<T, ApiError>;
}

impl<T> IntoApiResult<T> for Result<T, RegistryError> {
    fn map_api_error(self) -> Result<T, ApiError> {
        self.log().map_err(|e| e.into())
    }
}
