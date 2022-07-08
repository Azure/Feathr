mod sequencer;
mod api_v2;
mod api_v1;
mod management;
mod raft;
mod raft_network_impl;

pub use sequencer::RaftSequencer;
pub use api_v1::FeathrApiV1;
pub use api_v2::FeathrApiV2;
pub use management::management_routes;
use poem::{
    http::HeaderValue,
    web::headers::{Error, Header},
};
pub use raft::raft_routes;
pub use raft_network_impl::RegistryNetwork;
use reqwest::header::HeaderName;

/// The `Host` header.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct ManagementCode(String);

pub const MANAGEMENT_CODE_HEADER_NAME: &str = "x-registry-management-code";
pub const OPT_SEQ_HEADER_NAME: &str = "x-registry-opt-seq";

static MANAGEMENT_CODE_HEADER: HeaderName = HeaderName::from_static(MANAGEMENT_CODE_HEADER_NAME);

impl ManagementCode {
    pub fn code(&self) -> &str {
        &self.0
    }
}

impl Header for ManagementCode {
    fn name() -> &'static HeaderName {
        &MANAGEMENT_CODE_HEADER
    }

    fn decode<'i, I: Iterator<Item = &'i HeaderValue>>(values: &mut I) -> Result<Self, Error> {
        let v = values
            .next()
            .map(|val| String::from_utf8_lossy(val.as_bytes()).to_string())
            .ok_or_else(Error::invalid)?;
        Ok(Self(v))
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let bytes = self.0.as_str().as_bytes();
        let val = HeaderValue::from_bytes(bytes).expect("Invalid HeaderValue");
        values.extend(::std::iter::once(val));
    }
}
