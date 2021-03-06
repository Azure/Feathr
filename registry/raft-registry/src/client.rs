use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use common_utils::Appliable;
use common_utils::Logged;
use log::debug;
use openraft::error::AddLearnerError;
use openraft::error::CheckIsLeaderError;
use openraft::error::ClientWriteError;
use openraft::error::ForwardToLeader;
use openraft::error::Infallible;
use openraft::error::InitializeError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RemoteError;
use openraft::raft::AddLearnerResponse;
use openraft::raft::ClientWriteResponse;
use openraft::RaftMetrics;
use registry_api::FeathrApiResponse;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::FeathrApiRequest;
use crate::RegistryNodeId;
use crate::RegistryTypeConfig;
use crate::MANAGEMENT_CODE_HEADER_NAME;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Empty {}

#[derive(Clone)]
pub struct RegistryClient {
    /// The leader node to send request to.
    ///
    /// All traffic should be sent to the leader in a cluster.
    pub leader: Arc<Mutex<(RegistryNodeId, String)>>,

    pub inner: Client,

    code: Option<String>,
}

impl RegistryClient {
    /// Create a client with a leader node id and a node manager to get node address by node id.
    pub fn new(leader_id: RegistryNodeId, leader_addr: String, code: Option<String>) -> Self {
        Self {
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
            inner: reqwest::Client::new(),
            code,
        }
    }

    // --- Application API

    /// Read value by key, in an inconsistent mode.
    ///
    /// This method may return stale value because it does not force to read on a legal leader.
    pub async fn request(
        &self,
        req: &FeathrApiRequest,
    ) -> Result<FeathrApiResponse, RPCError<RegistryNodeId, Infallible>> {
        self.do_send_rpc_to_leader("handle-request", Some(req))
            .await
    }

    /// Consistent Read value by key, in an inconsistent mode.
    ///
    /// This method MUST return consistent value or CheckIsLeaderError.
    pub async fn consistent_request(
        &self,
        req: &FeathrApiRequest,
    ) -> Result<FeathrApiResponse, RPCError<RegistryNodeId, CheckIsLeaderError<RegistryNodeId>>>
    {
        self.send_rpc_to_leader("handle-leader-request", Some(req))
            .await
    }

    // --- Cluster management API

    /// Initialize a cluster of only the node that receives this request.
    ///
    /// This is the first step to initialize a cluster.
    /// With a initialized cluster, new node can be added with [`write`].
    /// Then setup replication with [`add_learner`].
    /// Then make the new node a member with [`change_membership`].
    pub async fn init(
        &self,
    ) -> Result<(), RPCError<RegistryNodeId, InitializeError<RegistryNodeId>>> {
        self.do_send_rpc_to_leader("init", Some(&Empty {})).await
    }

    /// Add a node as learner.
    ///
    /// The node to add has to exist, i.e., being added with `write(RegistryRequest::AddNode{})`
    pub async fn add_learner(
        &self,
        req: (RegistryNodeId, String),
    ) -> Result<
        AddLearnerResponse<RegistryNodeId>,
        RPCError<RegistryNodeId, AddLearnerError<RegistryNodeId>>,
    > {
        self.send_rpc_to_leader("add-learner", Some(&req)).await
    }

    /// Change membership to the specified set of nodes.
    ///
    /// All nodes in `req` have to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    pub async fn change_membership(
        &self,
        req: &BTreeSet<RegistryNodeId>,
    ) -> Result<
        ClientWriteResponse<RegistryTypeConfig>,
        RPCError<RegistryNodeId, ClientWriteError<RegistryNodeId>>,
    > {
        self.send_rpc_to_leader("change-membership", Some(req))
            .await
    }

    /// Get the metrics about the cluster.
    ///
    /// Metrics contains various information about the cluster, such as current leader,
    /// membership config, replication status etc.
    /// See [`RaftMetrics`].
    pub async fn metrics(
        &self,
    ) -> Result<RaftMetrics<RegistryTypeConfig>, RPCError<RegistryNodeId, Infallible>> {
        self.do_send_rpc_to_leader("metrics", None::<&()>).await
    }

    // --- Internal methods

    /// Send RPC to specified node.
    ///
    /// It sends out a POST request if `req` is Some. Otherwise a GET request.
    /// The remote endpoint must respond a reply in form of `Result<T, E>`.
    /// An `Err` happened on remote will be wrapped in an [`RPCError::RemoteError`].
    async fn do_send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<RegistryNodeId, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let (leader_id, url) = {
            let t = self.leader.lock().unwrap();
            let target_addr = &t.1;
            (t.0, format!("http://{}/{}", target_addr, uri))
        };

        let resp = if let Some(r) = req {
            debug!(
                ">>> client send request to {}: {}",
                url,
                serde_json::to_string_pretty(&r).unwrap()
            );
            self.inner.post(url.clone()).json(r)
        } else {
            debug!(">>> client send request to {}", url,);
            self.inner.get(url.clone())
        }
        .apply(|r| match &self.code {
            Some(c) => r.header(MANAGEMENT_CODE_HEADER_NAME, c),
            None => r,
        })
        .send()
        .await
        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        debug!(
            "<<< client recv reply from {}: {}",
            url,
            serde_json::to_string_pretty(&res).unwrap()
        );

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(leader_id, e)))
    }

    /// Try the best to send a request to the leader.
    ///
    /// If the target node is not a leader, a `ForwardToLeader` error will be
    /// returned and this client will retry at most 3 times to contact the updated leader.
    async fn send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<RegistryNodeId, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error
            + Serialize
            + DeserializeOwned
            + TryInto<ForwardToLeader<RegistryNodeId>>
            + Clone,
    {
        // Retry at most 3 times to find a valid leader.
        let mut n_retry = 3;

        loop {
            let res: Result<Resp, RPCError<RegistryNodeId, Err>> =
                self.do_send_rpc_to_leader(uri, req).await.log();

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res = <Err as TryInto<ForwardToLeader<RegistryNodeId>>>::try_into(
                    remote_err.source.clone(),
                );

                if let Ok(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                    ..
                }) = forward_err_res
                {
                    // Update target to the new leader.
                    {
                        let mut t = self.leader.lock().unwrap();
                        *t = (leader_id, leader_node.addr);
                    }

                    n_retry -= 1;
                    if n_retry > 0 {
                        debug!("Retrying, {} times left", n_retry);
                        continue;
                    }
                }
            }

            return Err(rpc_err);
        }
    }
}
