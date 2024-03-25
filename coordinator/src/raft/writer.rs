use std::sync::Arc;
use std::time::Duration;

use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use models::meta_data::*;
use protos::kv_service::{raft_write_command, RaftWriteCommand};
use protos::models_helper::to_prost_bytes;
use protos::{tskv_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use replication::errors::ReplicationResult;
use replication::raft_node::RaftNode;
use trace::debug;

use super::manager::RaftNodesManager;
use crate::errors::*;
use crate::TskvLeaderCaller;

pub struct TskvRaftWriter {
    pub meta: MetaRef,
    pub node_id: NodeId,
    pub timeout: Duration,
    pub enable_gzip: bool,
    pub total_memory: usize,
    pub memory_pool: MemoryPoolRef,
    pub raft_manager: Arc<RaftNodesManager>,

    pub request: RaftWriteCommand,
}

impl TskvRaftWriter {
    async fn pre_check_write_to_raft(&self, request: &RaftWriteCommand) -> CoordinatorResult<()> {
        if let Some(command) = &request.command {
            match command {
                raft_write_command::Command::WriteData(request) => {
                    let fb_points = flatbuffers::root::<protos::models::Points>(&request.data)
                        .map_err(|err| CoordinatorError::TskvError {
                            source: tskv::TskvError::InvalidFlatbuffer { source: err },
                        })?;

                    let _ = fb_points.tables().ok_or(CoordinatorError::TskvError {
                        source: tskv::TskvError::InvalidPointTable,
                    })?;

                    if request.data.len()
                        > self
                            .total_memory
                            .saturating_sub(self.memory_pool.reserved())
                    {
                        return Err(CoordinatorError::TskvError {
                            source: tskv::TskvError::MemoryExhausted,
                        });
                    }
                }

                raft_write_command::Command::DropTable(_request) => {}
                raft_write_command::Command::DropColumn(_request) => {}
                raft_write_command::Command::UpdateTags(_request) => {}
                raft_write_command::Command::DeleteFromTable(_request) => {}
            }
        }

        Ok(())
    }

    async fn write_to_remote(&self, leader_id: u64) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(leader_id).await.map_err(|error| {
            CoordinatorError::PreExecution {
                error: error.to_string(),
            }
        })?;
        let mut client = tskv_service_time_out_client(
            channel,
            self.timeout,
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.enable_gzip,
        );

        let cmd = tonic::Request::new(self.request.clone());
        let begin_time = models::utils::now_timestamp_millis();
        let response = client.raft_write(cmd).await?.into_inner();

        let use_time = models::utils::now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!(
                "write points to node:{}, use time too long {}",
                leader_id, use_time
            )
        }

        decode_grpc_response(response)?;
        Ok(())
    }

    async fn write_to_raft(&self, raft: Arc<RaftNode>, data: Vec<u8>) -> CoordinatorResult<()> {
        match raft.raw_raft().client_write(data).await {
            Err(err) => {
                if let Some(openraft::error::ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                }) = err.forward_to_leader()
                {
                    Err(CoordinatorError::RaftForwardToLeader {
                        leader_vnode_id: (*leader_id) as u32,
                        replica_id: leader_node.group_id,
                    })
                } else {
                    Err(CoordinatorError::RaftWriteError {
                        msg: err.to_string(),
                    })
                }
            }

            Ok(resp) => {
                let apply_result =
                    bincode::deserialize::<ReplicationResult<replication::Response>>(&resp.data)?;

                let _data = apply_result?;

                Ok(())
            }
        }
    }

    pub async fn write_to_local(&self, replica: &ReplicationSet) -> CoordinatorResult<Vec<u8>> {
        let raft = self
            .raft_manager
            .get_node_or_build(&self.request.tenant, &self.request.db_name, replica)
            .await?;

        self.pre_check_write_to_raft(&self.request).await?;
        let raft_data = to_prost_bytes(&self.request);
        self.write_to_raft(raft, raft_data).await?;

        Ok(vec![])
    }
}

#[async_trait::async_trait]
impl TskvLeaderCaller for TskvRaftWriter {
    async fn call(&self, replica: &ReplicationSet, node_id: u64) -> CoordinatorResult<Vec<u8>> {
        if node_id == self.node_id {
            self.write_to_local(replica).await?;
        } else {
            self.write_to_remote(node_id).await?;
        }

        Ok(vec![])
    }
}
