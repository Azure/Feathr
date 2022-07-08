use std::{sync::Arc, fmt::Display};

use openraft::Raft;
use registry_api::{FeathrApiRequest, FeathrApiResponse};
use serde::{Deserialize, Serialize};

mod store;
mod network;
mod app;
mod client;

pub type RegistryNodeId = u64;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct RegistryTypeConfig {}

impl Display for RegistryTypeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RegistryTypeConfig")
    }
}

impl openraft::RaftTypeConfig for RegistryTypeConfig {
    type D = FeathrApiRequest;
    type R = FeathrApiResponse;
    type NodeId = RegistryNodeId;
}

pub type RegistryRaft = Raft<RegistryTypeConfig, RegistryNetwork, Arc<RegistryStore>>;

pub use store::*;
pub use network::*;
pub use app::*;
pub use client::RegistryClient;
