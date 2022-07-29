use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use log::{debug, trace};
use openraft::{
    error::{CheckIsLeaderError, InitializeError},
    raft::ClientWriteRequest,
    Config, EntryPayload, Node, Raft,
};
use poem::error::Forbidden;
use registry_api::{
    ApiError, FeathrApiProvider, FeathrApiRequest, FeathrApiResponse, IntoApiResult,
};
use registry_provider::{Credential, Permission, RbacError, RbacProvider};
use sql_provider::load_content;
use tokio::net::ToSocketAddrs;

use crate::{
    ManagementCode, RegistryClient, RegistryNetwork, RegistryNodeId, RegistryRaft, RegistryStore,
    Restore,
};

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
#[derive(Clone)]
pub struct RaftRegistryApp {
    pub id: RegistryNodeId,
    pub addr: String,
    pub raft: RegistryRaft,
    pub store: Arc<RegistryStore>,
    pub config: Arc<Config>,
    pub forwarder: RegistryClient,
}

impl RaftRegistryApp {
    pub async fn new(node_id: RegistryNodeId, addr: String, cfg: crate::NodeConfig) -> Self {
        // Create a configuration for the raft instance.
        let config = Arc::new(cfg.raft_config.clone());

        // Create a instance of where the Raft data will be stored.
        let es = RegistryStore::open_create(node_id, cfg.clone());

        // es.load_latest_snapshot().await.unwrap();

        let mut store = Arc::new(es);

        store.restore().await;

        // Create the network layer that will connect and communicate the raft instances and
        // will be used in conjunction with the store created above.
        let network = RegistryNetwork::new(cfg);

        // Create a local raft instance.
        let raft = Raft::new(node_id, config.clone(), network, store.clone());

        let forwarder = RegistryClient::new(node_id, addr.clone(), store.get_management_code());

        // Create an application that will store all the instances created above, this will
        // be later used on the web services.
        RaftRegistryApp {
            id: node_id,
            addr,
            raft,
            store,
            config,
            forwarder,
        }
    }

    pub async fn check_permission(
        &self,
        credential: &Credential,
        resource: Option<&str>,
        permission: Permission,
    ) -> poem::Result<()> {
        let resource = match resource {
            Some(s) => s.parse().map_api_error()?,
            None => {
                // Read/write project list works as long as there is an identity
                return Ok(());
            }
        };
        if !self
            .store
            .state_machine
            .read()
            .await
            .registry
            .check_permission(credential, &resource, permission)
            .map_api_error()?
        {
            return Err(Forbidden(RbacError::PermissionDenied(
                credential.to_string(),
                resource,
                permission,
            )));
        }
        Ok(())
    }

    pub async fn check_code(&self, code: Option<ManagementCode>) -> poem::Result<()> {
        trace!("Checking code {:?}", code);
        match self.store.get_management_code() {
            Some(c) => match code.map(|c| c.code().to_string()) {
                Some(code) => {
                    if c == code {
                        Ok(())
                    } else {
                        Err(ApiError::Forbidden("forbidden".to_string()))?
                    }
                }
                None => Err(ApiError::Forbidden("forbidden".to_string()))?,
            },
            None => Ok(()),
        }
    }

    pub async fn init(&self) -> Result<(), InitializeError<RegistryNodeId>> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.id,
            Node {
                addr: self.addr.clone(),
                data: Default::default(),
            },
        );
        self.raft.initialize(nodes).await
    }

    pub async fn load_data(&self) -> anyhow::Result<()> {
        let (entities, edges, permission_map) = load_content().await?;
        match self
            .request(
                None,
                FeathrApiRequest::BatchLoad {
                    entities,
                    edges,
                    permissions: permission_map,
                },
            )
            .await
        {
            FeathrApiResponse::Error(e) => Err(e)?,
            _ => Ok(()),
        }
    }

    pub async fn request(&self, opt_seq: Option<u64>, req: FeathrApiRequest) -> FeathrApiResponse {
        let mut is_leader = true;
        let should_forward = match self.raft.is_leader().await {
            Ok(_) => {
                // This instance is the leader, do not forward
                trace!("This node is the leader");
                false
            }
            Err(CheckIsLeaderError::ForwardToLeader(node_id)) => {
                debug!("Should forward the request to node {}", node_id);
                is_leader = false;
                match opt_seq {
                    Some(seq) => match self.store.state_machine.read().await.last_applied_log {
                        Some(l) => {
                            // Check is local log index is newer than required seq, forward if local is out dated
                            trace!("Local log index is {}, required seq is {}", l.index, seq);
                            l.index < seq
                        }
                        None => {
                            // There is no local log index, so we have to forward
                            trace!("No last applied log");
                            true
                        }
                    },
                    // opt_seq is not set, forward to the leader for consistent read
                    None => true,
                }
            }
            Err(e) => {
                trace!("Check leader failed, error is {:?}", e);
                false
                // return FeathrApiResponse::Error(ApiError::InternalError("Raft cluster error".to_string()));
            }
        };
        if should_forward {
            debug!("The request is being forwarded to the leader");
            match self.forwarder.consistent_request(&req).await {
                Ok(v) => v,
                Err(e) => FeathrApiResponse::Error(ApiError::InternalError(format!("{:?}", e))),
            }
        } else {
            debug!("The request is being handled locally");
            // Only writing requests need to go to raft state machine
            if req.is_writing_request() {
                if is_leader {
                    let request = ClientWriteRequest::new(EntryPayload::Normal(req));
                    self.raft
                        .client_write(request)
                        .await
                        .map(|r| r.data)
                        .unwrap_or_else(|e| {
                            FeathrApiResponse::Error(ApiError::InternalError(format!("{:?}", e)))
                        })
                } else {
                    FeathrApiResponse::Error(ApiError::BadRequest(
                        "Updating requests must be submitted to the Raft leader".to_string(),
                    ))
                }
            } else {
                self.store
                    .state_machine
                    .write()
                    .await
                    .registry
                    .request(req)
                    .await
            }
        }
    }

    pub async fn join_cluster(&self, seeds: &[String], promote: bool) -> anyhow::Result<()> {
        // `self.forwarder` is unusable at the moment as this node is not member of any cluster
        for seed in expand_seeds(seeds).await? {
            debug!("Collecting cluster info from {}", seed);
            let client = RegistryClient::new(1, seed.to_owned(), self.store.get_management_code());
            if let Ok(metrics) = client.metrics().await {
                if let Some(leader_id) = metrics.current_leader {
                    if let Some(leader_node) = metrics.membership_config.get_node(&leader_id) {
                        debug!("Found leader node {} at {}", leader_id, leader_node.addr);
                        debug!(
                            "Trying to join the cluster via leader node {} at '{}'",
                            leader_id, leader_node.addr
                        );
                        // Create a new client that points to the leader instead of seed
                        let client = RegistryClient::new(
                            leader_id,
                            leader_node.addr.to_owned(),
                            self.store.get_management_code(),
                        );
                        // Remove stale old instance of this node
                        if let Ok(m) = client.metrics().await {
                            let mut nodes: BTreeSet<RegistryNodeId> =
                                m.membership_config.get_nodes().keys().copied().collect();
                            debug!("Found nodes: {:?}", nodes);
                            if nodes.contains(&self.id) {
                                debug!("Node with id {} exists in the cluster, clean up stale instance", self.id);
                                nodes.remove(&self.id);
                                client.change_membership(&nodes).await?;
                                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                            }
                        };
                        debug!("Adding this node into the cluster as learner");
                        if let Ok(resp) = client.add_learner((self.id, self.addr.clone())).await {
                            trace!("Got response {:?}", resp);
                            debug!("This node has joined the cluster as learner");
                            if promote {
                                debug!("Promoting this node into voter");
                                // Fetch metrics from the leader node
                                if let Ok(metrics) = client.metrics().await {
                                    debug!("Collecting node info from the leader");
                                    let mut nodes: BTreeSet<RegistryNodeId> = metrics
                                        .membership_config
                                        .get_nodes()
                                        .keys()
                                        .copied()
                                        .collect();
                                    debug!("Found nodes: {:?}", nodes);
                                    nodes.insert(self.id);
                                    debug!("Updating cluster membership");
                                    if let Ok(resp) = client.change_membership(&nodes).await {
                                        trace!("Got response {:?}", resp);
                                        debug!("Node {} promoted into voter", self.id);
                                        return Ok(());
                                    }
                                }
                            } else {
                                // Join as learner
                                return Ok(());
                            }
                        }
                    }
                }
            }
            debug!("Failed to join the cluster via seed {}", seed);
        }
        Err(anyhow::Error::msg("Failed to join the cluster"))
    }

    pub async fn join_or_init(&self, seeds: &[String], init: bool) -> anyhow::Result<()> {
        match self.join_cluster(seeds, true).await {
            Err(_) if init => {
                self.init().await?;
            }
            _ => (),
        }
        Ok(())
    }
}

/**
 * Discover seeds via DNS, it should work with K8S internal DNS service
 * TODO: Support more discover method, e.g. K8S API, broadcasting.
 */
async fn expand_seeds<T>(seeds: &[T]) -> anyhow::Result<Vec<String>>
where
    T: ToSocketAddrs,
{
    let mut ret = vec![];
    for seed in seeds {
        ret.extend(tokio::net::lookup_host(seed).await?.map(|sa| {
            if sa.is_ipv4() {
                format!("{}:{}", sa.ip(), sa.port())
            } else {
                format!("[{}]:{}", sa.ip(), sa.port())
            }
        }))
    }
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::expand_seeds;

    #[tokio::test]
    async fn test_expand() {
        let seeds = [
            "www.qq.com:80",
            "www.un.org:443",
            "www.baidu.com:443",
            "127.0.0.1:12345",
            "[::1]:54321",
        ];
        let r = expand_seeds(&seeds).await.unwrap();
        println!("{:?}", r);
        assert!(r.len() > 3);
        assert!(r.contains(&"127.0.0.1:12345".to_string()));
        assert!(r.contains(&"[::1]:54321".to_string()));
    }
}
