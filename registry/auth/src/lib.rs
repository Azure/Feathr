use thiserror::Error;

mod token;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("{0}")]
    ReqwestError(String),

    #[error("{0}")]
    JwtError(String),

    #[error(transparent)]
    CertError(#[from] openssl::error::ErrorStack),

    #[error("Token format is invalid.")]
    InvalidToken,

    #[error("Token timestamp is invalid.")]
    InvalidTimestamp,

    #[error("Key('{0}') is not found.")]
    KeyNotFound(String),

    #[error("Failed to initialize auth lib")]
    InitializationError,
}

pub use token::decode_token;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
