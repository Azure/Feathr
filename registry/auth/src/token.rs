use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use common_utils::Logged;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use openssl::x509::X509;
use serde::{de::DeserializeOwned, Deserialize};
use serde_with::TimestampSeconds;
use tokio::sync::{OnceCell, RwLock};

use crate::AuthError;

impl From<reqwest::Error> for AuthError {
    fn from(e: reqwest::Error) -> Self {
        Self::ReqwestError(e.to_string())
    }
}

impl From<jsonwebtoken::errors::Error> for AuthError {
    fn from(e: jsonwebtoken::errors::Error) -> Self {
        Self::JwtError(e.to_string())
    }
}

pub struct TokenDecoder {
    // TODO: Refresh periodically, daily maybe?
    keys: HashMap<String, DecodingKey>,
}

impl TokenDecoder {
    pub async fn new(base_url: &str) -> Result<Self, AuthError> {
        let resp: OpenIdConfiguration = reqwest::get(format!(
            "{}/v2.0/.well-known/openid-configuration",
            base_url
        ))
        .await?
        .json()
        .await?;
        let cfg: AadKeyConfiguration = reqwest::get(resp.jwks_uri).await?.json().await?;
        Ok(Self {
            keys: cfg
                .keys
                .into_iter()
                .filter_map(|k| k.into_decoding_key().log().ok())
                .collect::<HashMap<_, _>>(),
        })
    }

    pub fn decode_token<T>(&self, token: &str, check_expiration: bool) -> Result<T, AuthError>
    where
        T: DeserializeOwned,
    {
        let now = chrono::Utc::now();

        #[serde_with::serde_as]
        #[derive(Clone, Debug, Deserialize)]
        struct Claims<U> {
            #[serde_as(as = "TimestampSeconds<i64>")]
            nbf: DateTime<Utc>,
            #[serde_as(as = "TimestampSeconds<i64>")]
            exp: DateTime<Utc>,
            #[serde(flatten)]
            user_claims: U,
        }
        let claims: Claims<T> = self.decode_token_claims_no_validation(token.trim())?;
        if check_expiration && ((claims.nbf > now) || (claims.exp < now)) {
            return Err(AuthError::InvalidTimestamp);
        }
        Ok(claims.user_claims)
    }

    fn get_decoding_key(&self, kid: &str) -> Result<&DecodingKey, AuthError> {
        let key = self
            .keys
            .get(kid)
            .ok_or_else(|| AuthError::KeyNotFound(kid.to_owned()))?;
        Ok(key)
    }

    fn decode_token_claims_no_validation<T>(&self, token: &str) -> Result<T, AuthError>
    where
        T: DeserializeOwned,
    {
        let header = decode_header(token)?;
        let kid = &header.kid.or(header.x5t).ok_or(AuthError::InvalidToken)?;
        let key = self.get_decoding_key(kid)?;
        // TODO: Use 'alg' header
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = false;
        let decoded = decode(token, key, &validation)?;
        Ok(decoded.claims)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct OpenIdConfiguration {
    jwks_uri: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
struct AadKey {
    kty: String,
    #[serde(rename = "use")]
    use_: String,
    kid: String,
    x5t: String,
    n: String,
    e: String,
    x5c: Vec<String>,
    issuer: String,
}

impl AadKey {
    fn into_decoding_key(self) -> Result<(String, DecodingKey), AuthError> {
        let x509 = X509::from_pem(
            format!(
                "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----",
                self.x5c
                    .get(0)
                    .ok_or_else(|| AuthError::KeyNotFound(self.kid.to_owned()))?
            )
            .as_bytes(),
        )?;
        let pk = x509.public_key()?.public_key_to_pem()?;
        let key = DecodingKey::from_rsa_pem(pk.as_slice())?;
        Ok((self.kid, key))
    }
}

#[derive(Clone, Debug, Deserialize)]
struct AadKeyConfiguration {
    keys: Vec<AadKey>,
}

static DECODER: OnceCell<Option<Arc<RwLock<TokenDecoder>>>> = OnceCell::const_new();

pub async fn decode_token<C>(token: &str) -> Result<C, AuthError>
where
    C: DeserializeOwned,
{
    DECODER
        .get_or_init(|| async {
            let base_url = std::env::var("OPENID_BASE_URL")
                .unwrap_or("https://login.microsoftonline.com/common".to_string());
            TokenDecoder::new(&base_url)
                .await
                .ok()
                .map(|d| Arc::new(RwLock::new(d)))
        })
        .await
        .as_ref()
        .ok_or_else(|| AuthError::InitializationError)?
        .read()
        .await
        .decode_token(token, true)
        .map_err(|e| e.into())
}
