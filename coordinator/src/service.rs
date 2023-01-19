use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use config::{ClusterConfig, HintedOffConfig};
use datafusion::arrow::record_batch::RecordBatch;
use models::consistency_level::ConsistencyLevel;
use models::meta_data::{BucketInfo, DatabaseInfo, ExpiredBucketInfo, VnodeAllInfo};
use models::predicate::domain::{ColumnDomains, PredicateRef};
use models::schema::{DatabaseSchema, TableSchema, TskvTableSchema};
use models::*;

use protos::kv_service::WritePointsRpcRequest;
use snafu::ResultExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use trace::info;
use tskv::engine::{EngineRef, MockEngine};
use tskv::TimeRange;

use meta::meta_client_mock::{MockMetaClient, MockMetaManager};
use meta::meta_manager::RemoteMetaManager;
use meta::{MetaClientRef, MetaRef};

use datafusion::arrow::datatypes::SchemaRef;
use tskv::iterator::QueryOption;

use crate::command::*;
use crate::errors::*;
use crate::file_info::*;
use crate::hh_queue::HintedOffManager;
use crate::reader::{QueryExecutor, ReaderIterator};
use crate::writer::{PointWriter, VnodeMapping};

pub type CoordinatorRef = Arc<dyn Coordinator>;

#[async_trait::async_trait]
pub trait Coordinator: Send + Sync + Debug {
    fn node_id(&self) -> u64;
    fn meta_manager(&self) -> MetaRef;
    fn store_engine(&self) -> EngineRef;
    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;
    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        request: WritePointsRpcRequest,
    ) -> CoordinatorResult<()>;

    async fn exec_admin_stat_on_all_node(
        &self,
        req: AdminStatementRequest,
    ) -> CoordinatorResult<()>;

    async fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator>;

    async fn vnode_manager(
        &self,
        tenant: &str,
        vnode_id: u32,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()>;
}

#[derive(Debug, Default)]
pub struct MockCoordinator {}

#[async_trait::async_trait]
impl Coordinator for MockCoordinator {
    fn node_id(&self) -> u64 {
        0
    }

    fn meta_manager(&self) -> MetaRef {
        Arc::new(MockMetaManager::default())
    }

    fn store_engine(&self) -> EngineRef {
        Arc::new(MockEngine::default())
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        Some(Arc::new(MockMetaClient::default()))
    }

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        req: WritePointsRpcRequest,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    async fn exec_admin_stat_on_all_node(
        &self,
        req: AdminStatementRequest,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    async fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator> {
        let (it, _) = ReaderIterator::new();
        Ok(it)
    }

    async fn vnode_manager(
        &self,
        tenant: &str,
        vnode_id: u32,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct CoordService {
    node_id: u64,
    meta: MetaRef,
    kv_inst: EngineRef,
    writer: Arc<PointWriter>,
    handoff: Arc<HintedOffManager>,
}

impl CoordService {
    pub async fn new(
        kv_inst: EngineRef,
        meta_manager: MetaRef,
        cluster: ClusterConfig,
        handoff_cfg: HintedOffConfig,
    ) -> Arc<Self> {
        let (hh_sender, hh_receiver) = mpsc::channel(1024);
        let point_writer = Arc::new(PointWriter::new(
            cluster.node_id,
            kv_inst.clone(),
            meta_manager.clone(),
            hh_sender,
        ));

        let hh_manager = Arc::new(HintedOffManager::new(handoff_cfg, point_writer.clone()).await);
        tokio::spawn(HintedOffManager::write_handoff_job(
            hh_manager.clone(),
            hh_receiver,
        ));

        let coord = Arc::new(Self {
            kv_inst,
            node_id: cluster.node_id,
            meta: meta_manager,
            writer: point_writer,
            handoff: hh_manager,
        });

        tokio::spawn(CoordService::db_ttl_service(coord.clone()));

        coord
    }

    async fn db_ttl_service(coord: Arc<CoordService>) {
        loop {
            let dur = tokio::time::Duration::from_secs(5);
            tokio::time::sleep(dur).await;

            let expired = coord.meta.expired_bucket().await;
            for info in expired.iter() {
                let result = coord.delete_expired_bucket(info).await;

                info!("delete expired bucket :{:?}, {:?}", info, result);
            }
        }
    }

    async fn delete_expired_bucket(&self, info: &ExpiredBucketInfo) -> CoordinatorResult<()> {
        for repl_set in info.bucket.shard_group.iter() {
            for vnode in repl_set.vnodes.iter() {
                let req = AdminStatementRequest {
                    tenant: info.tenant.clone(),
                    stmt: AdminStatementType::DeleteVnode {
                        db: info.database.clone(),
                        vnode_id: vnode.id,
                    },
                };

                let cmd = CoordinatorTcpCmd::AdminStatementCmd(req);
                self.exec_on_node(vnode.node_id, cmd).await?;
            }
        }

        let meta =
            self.tenant_meta(&info.tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: info.tenant.clone(),
                })?;

        meta.delete_bucket(&info.database, info.bucket.id).await?;

        Ok(())
    }

    async fn admin_broadcast_request(
        coord: Arc<CoordService>,
        req: AdminStatementRequest,
        sender: oneshot::Sender<CoordinatorResult<()>>,
    ) {
        let meta = coord.meta.admin_meta();
        let nodes = meta.data_nodes().await;

        let mut requests = vec![];
        for node in nodes.iter() {
            let cmd = CoordinatorTcpCmd::AdminStatementCmd(req.clone());
            let request = coord.exec_on_node(node.id, cmd.clone());
            requests.push(request);
        }

        match futures::future::try_join_all(requests).await {
            Ok(_) => sender.send(Ok(())).expect("success"),
            Err(err) => sender.send(Err(err)).expect("success"),
        };
    }

    async fn get_vnode_all_info(
        &self,
        tenant: &str,
        vnode_id: u32,
    ) -> CoordinatorResult<VnodeAllInfo> {
        match self.tenant_meta(tenant).await {
            Some(meta_client) => match meta_client.get_vnode_all_info(vnode_id) {
                Some(all_info) => Ok(all_info),
                None => Err(CoordinatorError::VnodeNotFound { id: vnode_id }),
            },

            None => Err(CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            }),
        }
    }

    async fn vnode_manager_request(
        &self,
        tenant: &str,
        vnode_id: u32,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()> {
        let all_info = self.get_vnode_all_info(tenant, vnode_id).await?;

        let (tcp_req, req_node_id) = match cmd_type {
            VnodeManagerCmdType::Copy(node_id) => {
                if all_info.node_id == node_id {
                    return Err(CoordinatorError::CommonError {
                        msg: format!("Vnode: {} Already in {}", all_info.vnode_id, node_id),
                    });
                }

                (
                    AdminStatementRequest {
                        tenant: tenant.to_string(),
                        stmt: AdminStatementType::CopyVnode { vnode_id },
                    },
                    node_id,
                )
            }

            VnodeManagerCmdType::Move(node_id) => {
                if all_info.node_id == node_id {
                    return Err(CoordinatorError::CommonError {
                        msg: format!("move vnode: {} already in {}", all_info.vnode_id, node_id),
                    });
                }

                (
                    AdminStatementRequest {
                        tenant: tenant.to_string(),
                        stmt: AdminStatementType::MoveVnode { vnode_id },
                    },
                    node_id,
                )
            }

            VnodeManagerCmdType::Drop => (
                AdminStatementRequest {
                    tenant: tenant.to_string(),
                    stmt: AdminStatementType::DeleteVnode {
                        db: all_info.db_name.clone(),
                        vnode_id,
                    },
                },
                all_info.node_id,
            ),
        };

        let cmd = CoordinatorTcpCmd::AdminStatementCmd(tcp_req);

        self.exec_on_node(req_node_id, cmd).await
    }

    async fn select_statement_request(
        kv_inst: EngineRef,
        meta: MetaRef,
        option: QueryOption,
        sender: Sender<CoordinatorResult<RecordBatch>>,
    ) {
        let tenant = option.tenant.as_str();

        if let Some(meta_client) = meta.tenant_manager().tenant_meta(tenant).await {
            if let Err(e) =
                meta_client
                    .limiter()
                    .check_query()
                    .map_err(|e| CoordinatorError::MetaRequest {
                        msg: format!("{}", e),
                    })
            {
                let _ = sender.send(Err(e)).await;
            }
        }

        let executor = QueryExecutor::new(option, kv_inst, meta, sender.clone());
        if let Err(err) = executor.execute().await {
            info!("select statement execute failed: {}", err.to_string());
            let _ = sender.send(Err(err)).await;
        } else {
            info!("select statement execute success");
        }
    }

    async fn exec_on_node(&self, node_id: u64, cmd: CoordinatorTcpCmd) -> CoordinatorResult<()> {
        let mut conn = self.meta.admin_meta().get_node_conn(node_id).await?;

        send_command(&mut conn, &cmd).await?;
        let rsp_cmd = recv_command(&mut conn).await?;
        if let CoordinatorTcpCmd::StatusResponseCmd(msg) = rsp_cmd {
            self.meta.admin_meta().put_node_conn(node_id, conn);
            if msg.code == crate::command::SUCCESS_RESPONSE_CODE {
                Ok(())
            } else {
                Err(CoordinatorError::WriteVnode {
                    msg: format!("code: {}, msg: {}", msg.code, msg.data),
                })
            }
        } else {
            Err(CoordinatorError::UnExpectResponse)
        }
    }
}

//***************************** Coordinator Interface ***************************************** */
#[async_trait::async_trait]
impl Coordinator for CoordService {
    fn node_id(&self) -> u64 {
        self.node_id
    }

    fn meta_manager(&self) -> MetaRef {
        self.meta.clone()
    }

    fn store_engine(&self) -> EngineRef {
        self.kv_inst.clone()
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        self.meta.tenant_manager().tenant_meta(tenant).await
    }

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        request: WritePointsRpcRequest,
    ) -> CoordinatorResult<()> {
        if let Some(meta_client) = self.meta.tenant_manager().tenant_meta(&tenant).await {
            meta_client.limiter().check_write()?;

            let data_len = request.points.len();
            meta_client.limiter().check_data_in(data_len)?;
        }

        let req = WritePointsRequest {
            tenant: tenant.clone(),
            level,
            request,
        };

        self.writer.write_points(&req).await
    }

    async fn exec_admin_stat_on_all_node(
        &self,
        req: AdminStatementRequest,
    ) -> CoordinatorResult<()> {
        let meta = self.meta.admin_meta();
        let nodes = meta.data_nodes().await;

        let mut requests = vec![];
        for node in nodes.iter() {
            let cmd = CoordinatorTcpCmd::AdminStatementCmd(req.clone());
            let request = self.exec_on_node(node.id, cmd.clone());
            requests.push(request);
        }

        match futures::future::try_join_all(requests).await {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator> {
        let (iterator, sender) = ReaderIterator::new();

        tokio::spawn(CoordService::select_statement_request(
            self.kv_inst.clone(),
            self.meta.clone(),
            option,
            sender,
        ));

        Ok(iterator)
    }

    async fn vnode_manager(
        &self,
        tenant: &str,
        vnode_id: u32,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()> {
        self.vnode_manager_request(tenant, vnode_id, cmd_type).await
    }
}
