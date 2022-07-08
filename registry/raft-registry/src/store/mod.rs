mod config;
mod store;

use std::{
    fmt::Debug,
    io::Cursor,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use async_trait::async_trait;
use log::debug;
use openraft::{
    storage::{LogState, Snapshot},
    AnyError, EffectiveMembership, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId,
    RaftLogReader, RaftSnapshotBuilder, RaftStorage, SnapshotMeta, StateMachineChanges,
    StorageError, StorageIOError, Vote,
};
use registry_api::{FeathrApiProvider, FeathrApiResponse};
use registry_provider::EntityProperty;
use serde::{Deserialize, Serialize};
use sled::{Db, IVec};
use sql_provider::Registry;
use tokio::sync::{Mutex, RwLock};

use crate::{RegistryNodeId, RegistryTypeConfig};

pub use config::NodeConfig;

#[derive(Debug)]
pub struct RegistrySnapshot {
    pub meta: SnapshotMeta<RegistryNodeId>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RegistryStateMachine {
    pub last_applied_log: Option<LogId<RegistryNodeId>>,

    pub last_membership: EffectiveMembership<RegistryNodeId>,

    pub registry: Registry<EntityProperty>,
}

#[derive(Debug)]
pub struct RegistryStore {
    last_purged_log_id: RwLock<Option<LogId<RegistryNodeId>>>,

    /// The Raft log.
    pub log: sled::Tree, //RwLock<BTreeMap<u64, Entry<StorageRaftTypeConfig>>>,

    /// The Raft state machine.
    pub state_machine: RwLock<RegistryStateMachine>,

    /// The current granted vote.
    vote: sled::Tree,

    snapshot_idx: Arc<Mutex<u64>>,

    current_snapshot: RwLock<Option<RegistrySnapshot>>,

    config: NodeConfig,

    pub node_id: RegistryNodeId,
}

fn get_sled_db(config: NodeConfig, node_id: RegistryNodeId) -> Db {
    let db_path = format!(
        "{}/{}-{}.binlog",
        config.journal_path, config.instance_prefix, node_id
    );
    let db = sled::open(db_path.clone()).unwrap();
    tracing::debug!("get_sled_db: created log at: {:?}", db_path);
    db
}

impl RegistryStore {
    pub fn open_create(node_id: RegistryNodeId, config: NodeConfig) -> RegistryStore {
        tracing::info!("open_create, node_id: {}", node_id);

        let db = get_sled_db(config.clone(), node_id);

        let log = db
            .open_tree(format!("journal_entities_{}", node_id))
            .unwrap();

        let vote = db.open_tree(format!("votes_{}", node_id)).unwrap();

        let current_snapshot = RwLock::new(None);

        RegistryStore {
            last_purged_log_id: Default::default(),
            config,
            node_id,
            log,
            state_machine: Default::default(),
            vote,
            snapshot_idx: Arc::new(Mutex::new(0)),
            current_snapshot,
        }
    }

    pub fn get_management_code(&self) -> Option<String> {
        self.config.management_code.clone()
    }
}

//Store trait for restore things from snapshot and log
#[async_trait]
pub trait Restore {
    async fn restore(&mut self);
}

#[async_trait]
impl Restore for Arc<RegistryStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn restore(&mut self) {
        tracing::debug!("restore");
        let log = &self.log;

        let first = log
            .iter()
            .rev()
            .next()
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<RegistryTypeConfig>>(&*val)
                    .unwrap()
                    .log_id
            });

        match first {
            Some(x) => {
                tracing::debug!("restore: first log id = {:?}", x);
                let mut ld = self.last_purged_log_id.write().await;
                *ld = Some(x);
            }
            None => {}
        }

        let snapshot = self.get_current_snapshot().await.unwrap();

        match snapshot {
            Some(ss) => {
                self.install_snapshot(&ss.meta, ss.snapshot).await.unwrap();
            }
            None => {}
        }
    }
}

#[async_trait]
impl RaftLogReader<RegistryTypeConfig> for Arc<RegistryStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<RegistryTypeConfig>, StorageError<RegistryNodeId>> {
        let log = &self.log;
        let last = log
            .iter()
            .rev()
            .next()
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<RegistryTypeConfig>>(&*val)
                    .unwrap()
                    .log_id
            });

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };
        tracing::trace!(
            "get_log_state: last_purged = {:?}, last = {:?}",
            last_purged,
            last
        );
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RegistryTypeConfig>>, StorageError<RegistryNodeId>> {
        let log = &self.log;
        let response = log
            .range(transform_range_bound(range))
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<RegistryTypeConfig>>(&*val)
                    .map_err(|e| {
                        let v = val.clone().to_vec();
                        let s = String::from_utf8_lossy(&v);
                        debug!("val: '{}'", s);
                        e
                    })
                    .unwrap()
            })
            .collect();

        Ok(response)
    }
}

fn transform_range_bound<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
    range: RB,
) -> (Bound<IVec>, Bound<IVec>) {
    (
        serialize_bound(&range.start_bound()),
        serialize_bound(&range.end_bound()),
    )
}

fn serialize_bound(v: &Bound<&u64>) -> Bound<IVec> {
    match v {
        Bound::Included(v) => Bound::Included(IVec::from(&v.to_be_bytes())),
        Bound::Excluded(v) => Bound::Excluded(IVec::from(&v.to_be_bytes())),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[async_trait]
impl RaftSnapshotBuilder<RegistryTypeConfig, Cursor<Vec<u8>>> for Arc<RegistryStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<RegistryNodeId, Cursor<Vec<u8>>>, StorageError<RegistryNodeId>> {
        let (data, last_applied_log);

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
        }

        let last_applied_log = match last_applied_log {
            None => {
                panic!("can not compact empty state machine");
            }
            Some(x) => x,
        };

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().await;
            *l += 1;
            *l
        };

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied_log.leader_id, last_applied_log.index, snapshot_idx
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership: self.state_machine.read().await.last_membership.clone(),
            snapshot_id,
        };

        let snapshot = RegistrySnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        self.write_snapshot().await.unwrap();

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<RegistryTypeConfig> for Arc<RegistryStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(
        &mut self,
        vote: &Vote<RegistryNodeId>,
    ) -> Result<(), StorageError<RegistryNodeId>> {
        self.vote
            .insert(b"vote", IVec::from(serde_json::to_vec(vote).unwrap()))
            .unwrap();
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<RegistryNodeId>>, StorageError<RegistryNodeId>> {
        let value = self.vote.get(b"vote").unwrap();
        match value {
            None => Ok(None),
            Some(val) => Ok(Some(
                serde_json::from_slice::<Vote<RegistryNodeId>>(&*val).unwrap(),
            )),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry<RegistryTypeConfig>],
    ) -> Result<(), StorageError<RegistryNodeId>> {
        let log = &self.log;
        for entry in entries {
            log.insert(
                entry.log_id.index.to_be_bytes(),
                IVec::from(serde_json::to_vec(&*entry).unwrap()),
            )
            .unwrap();
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<RegistryNodeId>,
    ) -> Result<(), StorageError<RegistryNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let log = &self.log;
        let keys = log
            .range(transform_range_bound(log_id.index..))
            .map(|res| res.unwrap())
            .map(|(k, _v)| k); //TODO Why originally used collect instead of the iter.
        for key in keys {
            log.remove(&key).unwrap();
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<RegistryNodeId>,
    ) -> Result<(), StorageError<RegistryNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let log = &self.log;

            let keys = log
                .range(transform_range_bound(..=log_id.index))
                .map(|res| res.unwrap())
                .map(|(k, _)| k);
            for key in keys {
                log.remove(&key).unwrap();
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RegistryNodeId>>,
            EffectiveMembership<RegistryNodeId>,
        ),
        StorageError<RegistryNodeId>,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<RegistryTypeConfig>],
    ) -> Result<Vec<FeathrApiResponse>, StorageError<RegistryNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(FeathrApiResponse::Unit),
                EntryPayload::Normal(ref req) => {
                    res.push(sm.registry.request(req.to_owned()).await)
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = EffectiveMembership::new(Some(entry.log_id), mem.clone());
                    res.push(FeathrApiResponse::Unit)
                }
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<RegistryNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RegistryNodeId>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<RegistryTypeConfig>, StorageError<RegistryNodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = RegistrySnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: RegistryStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.clone()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RegistryNodeId, Self::SnapshotData>>, StorageError<RegistryNodeId>>
    {
        tracing::debug!("get_current_snapshot: start");

        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => {
                let data = self.read_snapshot_file().await;
                //tracing::debug!("get_current_snapshot: data = {:?}",data);

                let data = match data {
                    Ok(c) => c,
                    Err(_e) => return Ok(None),
                };

                let content: RegistryStateMachine = serde_json::from_slice(&data).unwrap();

                let last_applied_log = content.last_applied_log.unwrap();
                tracing::debug!(
                    "get_current_snapshot: last_applied_log = {:?}",
                    last_applied_log
                );

                let snapshot_idx = {
                    let mut l = self.snapshot_idx.lock().await;
                    *l += 1;
                    *l
                };

                let snapshot_id = format!(
                    "{}-{}-{}",
                    last_applied_log.leader_id, last_applied_log.index, snapshot_idx
                );

                let meta = SnapshotMeta {
                    last_log_id: last_applied_log,
                    last_membership: self.state_machine.read().await.last_membership.clone(),
                    snapshot_id,
                };

                tracing::debug!("get_current_snapshot: meta {:?}", meta);

                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
        }
    }
}
