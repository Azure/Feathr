use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_utils::Appliable;
use log::trace;
use openraft::error::AppendEntriesError;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RemoteError;
use openraft::error::VoteError;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::Node;
use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::RegistryNodeId;
use crate::RegistryTypeConfig;
use crate::MANAGEMENT_CODE_HEADER_NAME;

pub struct RegistryNetwork {
    pub clients: Arc<HashMap<String, reqwest::Client>>,
    config: Arc<crate::NodeConfig>,
}

impl RegistryNetwork {
    pub fn new(config: crate::NodeConfig) -> Self {
        Self {
            clients: Arc::new(HashMap::new()),
            config: Arc::new(config),
        }
    }

    pub async fn send_rpc<Req, Resp, Err>(
        &mut self,
        target: RegistryNodeId,
        target_node: Option<&Node>,
        uri: &str,
        req: Req,
    ) -> Result<Resp, RPCError<RegistryNodeId, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let addr = target_node.map(|x| &x.addr).unwrap();

        let url = format!("http://{}/{}", addr, uri);

        let clients = Arc::get_mut(&mut self.clients).unwrap();

        let client = clients
            .entry(url.clone())
            .or_insert_with(reqwest::Client::new);

        trace!("send_rpc: url is `{}`", url);
        let resp = client
            .post(url)
            .apply(|r| match &self.config.management_code {
                Some(c) => r.header(MANAGEMENT_CODE_HEADER_NAME, c),
                None => r,
            })
            .json(&req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

// NOTE: This could be implemented also on `Arc<RegistryNetwork>`, but since it's empty, implemented directly.
#[async_trait]
impl RaftNetworkFactory<RegistryTypeConfig> for RegistryNetwork {
    type Network = RegistryNetworkConnection;

    async fn connect(&mut self, target: RegistryNodeId, node: Option<&Node>) -> Self::Network {
        RegistryNetworkConnection {
            owner: RegistryNetwork::new(self.config.as_ref().to_owned()),
            target,
            target_node: node.cloned(),
        }
    }
}

pub struct RegistryNetworkConnection {
    owner: RegistryNetwork,
    target: RegistryNodeId,
    target_node: Option<Node>,
}

#[async_trait]
impl RaftNetwork<RegistryTypeConfig> for RegistryNetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<RegistryTypeConfig>,
    ) -> Result<
        AppendEntriesResponse<RegistryNodeId>,
        RPCError<RegistryNodeId, AppendEntriesError<RegistryNodeId>>,
    > {
        self.owner
            .send_rpc(self.target, self.target_node.as_ref(), "raft-append", req)
            .await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<RegistryTypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<RegistryNodeId>,
        RPCError<RegistryNodeId, InstallSnapshotError<RegistryNodeId>>,
    > {
        self.owner
            .send_rpc(self.target, self.target_node.as_ref(), "raft-snapshot", req)
            .await
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<RegistryNodeId>,
    ) -> Result<VoteResponse<RegistryNodeId>, RPCError<RegistryNodeId, VoteError<RegistryNodeId>>>
    {
        self.owner
            .send_rpc(self.target, self.target_node.as_ref(), "raft-vote", req)
            .await
    }
}
