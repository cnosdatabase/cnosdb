use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{Config, RaftMetrics};

use crate::apply_store::ApplyStorageRef;
use crate::errors::{ReplicationError, ReplicationResult};
use crate::node_store::NodeStorage;
use crate::raft_network::Network;
use crate::{OpenRaftNode, RaftNodeId, RaftNodeInfo};

#[derive(Clone)]
pub struct RaftNode {
    id: RaftNodeId,
    info: RaftNodeInfo,
    storage: Arc<NodeStorage>,
    engine: ApplyStorageRef,

    raft: OpenRaftNode,
    config: Arc<Config>,
}

impl RaftNode {
    pub async fn new(
        id: RaftNodeId,
        info: RaftNodeInfo,
        storage: Arc<NodeStorage>,
        engine: ApplyStorageRef,
    ) -> ReplicationResult<Self> {
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };
        let config = Arc::new(config.validate().unwrap());

        let (log_store, state_machine) = Adaptor::new(storage.clone());

        let network = Network {};
        let raft = openraft::Raft::new(id, config.clone(), network, log_store, state_machine)
            .await
            .map_err(|err| ReplicationError::RaftInternalErr {
                msg: format!("New raft execute failed: {}", err),
            })?;

        Ok(Self {
            id,
            info,
            storage,
            raft,
            config,
            engine,
        })
    }

    pub fn raw_raft(&self) -> OpenRaftNode {
        self.raft.clone()
    }

    /// Initialize a single-node cluster.
    pub async fn raft_init(&self) -> ReplicationResult<()> {
        let mut nodes = BTreeMap::new();
        nodes.insert(self.id, self.info.clone());

        self.raft
            .initialize(nodes)
            .await
            .map_err(|err| ReplicationError::RaftInternalErr {
                msg: format!("Initialize raft execute failed: {}", err),
            })?;

        Ok(())
    }

    /// Add a node as **Learner**.
    pub async fn raft_add_learner(
        &self,
        id: RaftNodeId,
        info: RaftNodeInfo,
    ) -> ReplicationResult<()> {
        self.raft.add_learner(id, info, true).await.map_err(|err| {
            ReplicationError::RaftInternalErr {
                msg: format!("Addlearner raft execute failed: {}", err),
            }
        })?;

        Ok(())
    }

    /// Changes specified learners to members, or remove members.
    pub async fn raft_change_membership(
        &self,
        list: BTreeSet<RaftNodeId>,
    ) -> ReplicationResult<()> {
        self.raft
            .change_membership(list, false)
            .await
            .map_err(|err| ReplicationError::RaftInternalErr {
                msg: format!("Change membership raft execute failed: {}", err),
            })?;

        Ok(())
    }

    /// Get the latest metrics of the cluster
    pub fn raft_metrics(&self) -> RaftMetrics<RaftNodeId, RaftNodeInfo> {
        self.raft.metrics().borrow().clone()
    }

    pub async fn test_read_data(&self, key: &str) -> ReplicationResult<Option<String>> {
        self.engine.test_get_kv(key).await
    }

    // pub async fn raft_vote(
    //     &self,
    //     vote: VoteRequest<RaftNodeId>,
    // ) -> ReplicationResult<VoteResponse<RaftNodeId>> {
    //     let rsp = self
    //         .raft
    //         .vote(vote)
    //         .await
    //         .map_err(|err| ReplicationError::RaftInternalErr {
    //             msg: format!("Vote raft execute failed: {}", err),
    //         })?;

    //     Ok(rsp)
    // }

    // pub async fn raft_append(
    //     &self,
    //     req: AppendEntriesRequest<TypeConfig>,
    // ) -> ReplicationResult<AppendEntriesResponse<RaftNodeId>> {
    //     let rsp = self.raft.append_entries(req).await.map_err(|err| {
    //         ReplicationError::RaftInternalErr {
    //             msg: format!("Append raft execute failed: {}", err),
    //         }
    //     })?;

    //     Ok(rsp)
    // }

    // pub async fn raft_snapshot(
    //     &self,
    //     req: InstallSnapshotRequest<TypeConfig>,
    // ) -> ReplicationResult<InstallSnapshotResponse<RaftNodeId>> {
    //     let rsp = self.raft.install_snapshot(req).await.map_err(|err| {
    //         ReplicationError::RaftInternalErr {
    //             msg: format!("Snapshot raft execute failed: {}", err),
    //         }
    //     })?;

    //     Ok(rsp)
    // }

    // pub async fn test_write_data(
    //     &self,
    //     req: Request,
    // ) -> Result<
    //     ClientWriteResponse<TypeConfig>,
    //     openraft::error::RaftError<u64, ClientWriteError<u64, RaftNodeInfo>>,
    // > {
    //     let response = self.raft.client_write(req).await;

    //     response
    // }
}
