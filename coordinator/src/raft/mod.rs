use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::{NodeId, VnodeId};
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{
    DownloadFileRequest, GetFilesMetaResponse, GetVnodeSnapFilesMetaRequest, RaftWriteCommand,
};
use protos::models_helper::parse_prost_bytes;
use protos::{tskv_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use replication::errors::{ReplicationError, ReplicationResult};
use replication::{ApplyContext, ApplyStorage};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use tracing::{error, info};
use tskv::vnode_store::VnodeStorage;
use tskv::VnodeSnapshot;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::file_info::get_file_info;

pub mod manager;
pub mod writer;

pub struct TskvEngineStorage {
    node_id: NodeId,
    tenant: String,
    db_name: String,
    vnode_id: VnodeId,
    meta: MetaRef,
    vnode: Arc<VnodeStorage>,
    storage: tskv::EngineRef,
    grpc_enable_gzip: bool,
}

impl TskvEngineStorage {
    pub fn open(
        node_id: NodeId,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
        meta: MetaRef,
        vnode: Arc<VnodeStorage>,
        storage: tskv::EngineRef,
        grpc_enable_gzip: bool,
    ) -> Self {
        Self {
            meta,
            node_id,
            vnode_id,
            storage,
            vnode,
            tenant: tenant.to_owned(),
            db_name: db_name.to_owned(),
            grpc_enable_gzip,
        }
    }

    pub async fn download_snapshot(&self, snapshot: &VnodeSnapshot) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(snapshot.node_id).await?;
        let mut client = tskv_service_time_out_client(
            channel,
            Duration::from_secs(60 * 60),
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.grpc_enable_gzip,
        );

        let owner = models::schema::make_owner(&snapshot.tenant, &snapshot.database);
        let path = self.storage.get_storage_options().snapshot_sub_dir(
            &owner,
            self.vnode_id,
            &snapshot.snapshot_id,
        );
        info!("snapshot path: {:?}", path);
        if let Err(err) = self
            .download_snapshot_files(&path, snapshot, &mut client)
            .await
        {
            tokio::fs::remove_dir_all(&path).await?;
            return Err(err);
        }
        info!("success download snapshot all files");

        Ok(())
    }

    async fn download_snapshot_files(
        &self,
        data_path: &Path,
        snapshot: &VnodeSnapshot,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<()> {
        let files_meta = self.get_snapshot_files_meta(snapshot, client).await?;
        for info in files_meta.infos.iter() {
            let relative_filename = info
                .name
                .strip_prefix(&(files_meta.path.clone() + "/"))
                .unwrap();

            let filename = data_path.join(relative_filename);
            info!("begin download file: {:?} -> {:?}", info.name, filename);
            Self::download_file(&info.name, &filename, client).await?;

            let filename = filename.to_string_lossy().to_string();
            let tmp_info = get_file_info(&filename).await?;
            if tmp_info.md5 != info.md5 {
                return Err(CoordinatorError::CommonError {
                    msg: "download file md5 not match ".to_string(),
                });
            }
        }

        Ok(())
    }

    async fn download_file(
        download: &str,
        filename: &Path,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<()> {
        if let Some(dir) = filename.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(filename)
            .await?;

        let request = tonic::Request::new(DownloadFileRequest {
            filename: download.to_string(),
        });
        let mut resp_stream = client
            .download_file(request)
            .await
            .map_err(tskv::Error::from)?
            .into_inner();
        while let Some(received) = resp_stream.next().await {
            let received = received?;
            if received.code != crate::SUCCESS_RESPONSE_CODE {
                return Err(CoordinatorError::GRPCRequest {
                    msg: format!(
                        "server status: {}, {:?}",
                        received.code,
                        String::from_utf8(received.data)
                    ),
                });
            }

            file.write_all(&received.data).await?;
        }

        Ok(())
    }

    async fn get_snapshot_files_meta(
        &self,
        snapshot: &VnodeSnapshot,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<GetFilesMetaResponse> {
        let request = tonic::Request::new(GetVnodeSnapFilesMetaRequest {
            tenant: snapshot.tenant.to_string(),
            db: snapshot.database.to_string(),
            vnode_id: snapshot.vnode_id,
            snapshot_id: snapshot.snapshot_id.to_string(),
        });

        let resp = client
            .get_vnode_snap_files_meta(request)
            .await
            .map_err(tskv::Error::from)?
            .into_inner();
        info!(
            "vnode id: {}, snapshot files meta: {:?}",
            snapshot.vnode_id, resp
        );

        Ok(resp)
    }

    async fn exec_apply(
        &self,
        ctx: &ApplyContext,
        req: &replication::Request,
    ) -> ReplicationResult<replication::Response> {
        let request = parse_prost_bytes::<RaftWriteCommand>(req)?;
        if let Some(command) = request.command {
            self.vnode.apply(ctx, command).await.map_err(|err| {
                ReplicationError::ApplyEngineErr {
                    msg: err.to_string(),
                }
            })?;
        }

        Ok(vec![])
    }
}

#[async_trait::async_trait]
impl ApplyStorage for TskvEngineStorage {
    async fn apply(
        &self,
        ctx: &ApplyContext,
        req: &replication::Request,
    ) -> ReplicationResult<replication::Response> {
        let apply_result = self.exec_apply(ctx, req).await;
        if let Err(err) = &apply_result {
            error!("replication apply failed: {:?}; {:?}", ctx, err);
        }

        match bincode::serialize(&apply_result) {
            Ok(data) => Ok(data),
            Err(err) => Ok(err.to_string().into()),
        }
    }

    async fn snapshot(&self) -> ReplicationResult<Vec<u8>> {
        let mut snapshot =
            self.vnode
                .create_snapshot()
                .await
                .map_err(|err| ReplicationError::ApplyEngineErr {
                    msg: err.to_string(),
                })?;

        snapshot.node_id = self.node_id;

        let data = bincode::serialize(&snapshot)?;
        Ok(data)
    }

    async fn restore(&self, data: &[u8]) -> ReplicationResult<()> {
        let snapshot = bincode::deserialize::<VnodeSnapshot>(data)?;
        self.download_snapshot(&snapshot).await.map_err(|err| {
            ReplicationError::ApplyEngineErr {
                msg: err.to_string(),
            }
        })?;

        let owner = models::schema::make_owner(&snapshot.tenant, &snapshot.database);
        let vnode_move_dir = self
            .storage
            .get_storage_options()
            .move_dir(&owner, self.vnode_id);
        let snapshot_dir = self.storage.get_storage_options().snapshot_sub_dir(
            &owner,
            self.vnode_id,
            &snapshot.snapshot_id,
        );
        info!(
            "rename snpshot dir to move dir: {:?} -> {:?}",
            snapshot_dir, vnode_move_dir
        );
        tokio::fs::rename(&snapshot_dir, vnode_move_dir).await?;

        let mut snapshot = snapshot.clone();
        snapshot.vnode_id = self.vnode_id;
        self.vnode.apply_snapshot(snapshot).await.map_err(|err| {
            ReplicationError::ApplyEngineErr {
                msg: err.to_string(),
            }
        })?;

        Ok(())
    }

    async fn destory(&self) -> ReplicationResult<()> {
        self.storage
            .remove_tsfamily(&self.tenant, &self.db_name, self.vnode_id)
            .await
            .map_err(|err| ReplicationError::ApplyEngineErr {
                msg: err.to_string(),
            })?;

        Ok(())
    }
}
