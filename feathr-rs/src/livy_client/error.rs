use std::sync::PoisonError;

use reqwest::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LivyClientError {
    #[error("{0}")]
    SyncError(String),

    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    #[error("Url={0}, Status={1}, Response={2}")]
    HttpError(String, StatusCode, String),

    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),

    #[error(transparent)]
    AzureSynapseError(#[from] super::azure_synapse::AzureSynapseError),

    #[error("Job {0} is not in valid state")]
    InvalidJobState(u64)
}

impl<Guard> From<PoisonError<Guard>> for LivyClientError {
    fn from(e: PoisonError<Guard>) -> Self {
        LivyClientError::SyncError(e.to_string())
    }
}
pub type Result<T> = std::result::Result<T, LivyClientError>;