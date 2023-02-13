use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use config::ClusterConfig;
use models::meta_data::*;
use parking_lot::RwLock;
use std::fmt::Debug;
use tokio::net::TcpStream;

use crate::{
    client::MetaHttpClient,
    error::{MetaError, MetaResult},
    store::{
        command::{self, EntryLog},
        key_path,
    },
};

#[async_trait]
pub trait AdminMeta: Send + Sync + Debug {
    // *数据节点上下线管理 */
    async fn sync_all(&self) -> MetaResult<u64>;
    async fn data_nodes(&self) -> Vec<NodeInfo>;
    async fn add_data_node(&self) -> MetaResult<()>;
    // fn del_data_node(&self, id: u64) -> MetaResult<()>;

    // fn meta_nodes(&self);
    // fn add_meta_node(&self, node: &NodeInfo) -> MetaResult<()>;
    // fn del_meta_node(&self, id: u64) -> MetaResult<()>;

    fn heartbeat(&self); // update node status

    async fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo>;
    async fn get_node_conn(&self, node_id: u64) -> MetaResult<TcpStream>;
    fn put_node_conn(&self, node_id: u64, conn: TcpStream);
    async fn retain_id(&self, count: u32) -> MetaResult<u32>;
    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()>;
}

#[derive(Debug)]
pub struct RemoteAdminMeta {
    config: ClusterConfig,
    data_nodes: RwLock<HashMap<u64, NodeInfo>>,
    conn_map: RwLock<HashMap<u64, VecDeque<TcpStream>>>,

    client: MetaHttpClient,
}

impl RemoteAdminMeta {
    pub async fn new(config: ClusterConfig) -> MetaResult<(Self, u64)> {
        let meta_url = config.meta_service_addr.clone();
        let admin = Self {
            config,
            conn_map: RwLock::new(HashMap::new()),
            data_nodes: RwLock::new(HashMap::new()),
            client: MetaHttpClient::new(1, meta_url),
        };

        let version = admin.sync_all_data_node().await?;

        Ok((admin, version))
    }

    async fn sync_all_data_node(&self) -> MetaResult<u64> {
        let req = command::ReadCommand::DataNodes(self.config.name.clone());
        let (resp, version) = self.client.read::<(Vec<NodeInfo>, u64)>(&req).await?;
        {
            let mut nodes = self.data_nodes.write();
            for item in resp.iter() {
                nodes.insert(item.id, item.clone());
            }
        }

        Ok(version)
    }

    pub fn sys_info() -> SysInfo {
        let mut info = SysInfo::default();

        if let Ok(val) = sys_info::disk_info() {
            info.disk_free = val.free;
        }

        if let Ok(val) = sys_info::mem_info() {
            info.mem_free = val.free;
        }

        if let Ok(val) = sys_info::loadavg() {
            info.cpu_load = val.one;
        }

        info
    }
}

#[async_trait::async_trait]
impl AdminMeta for RemoteAdminMeta {
    async fn sync_all(&self) -> MetaResult<u64> {
        self.sync_all_data_node().await
    }

    async fn add_data_node(&self) -> MetaResult<()> {
        let node = NodeInfo {
            status: 0,
            id: self.config.node_id,
            tcp_addr: self.config.tcp_listen_addr.clone(),
            http_addr: self.config.http_listen_addr.clone(),
        };

        let req = command::WriteCommand::AddDataNode(self.config.name.clone(), node.clone());
        let rsp = self.client.write::<command::StatusResponse>(&req).await?;
        if rsp.code != command::META_REQUEST_SUCCESS {
            return Err(MetaError::CommonError {
                msg: format!("add data node err: {} {}", rsp.code, rsp.msg),
            });
        }

        self.data_nodes.write().insert(node.id, node);

        Ok(())
    }

    async fn data_nodes(&self) -> Vec<NodeInfo> {
        let mut nodes = vec![];
        for (_, val) in self.data_nodes.read().iter() {
            nodes.push(val.clone())
        }

        nodes
    }

    async fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        if let Some(val) = self.data_nodes.read().get(&id) {
            return Ok(val.clone());
        }

        Err(MetaError::NotFoundNode { id })
    }

    async fn get_node_conn(&self, node_id: u64) -> MetaResult<TcpStream> {
        {
            let mut write = self.conn_map.write();
            let entry = write
                .entry(node_id)
                .or_insert_with(|| VecDeque::with_capacity(32));
            if let Some(val) = entry.pop_front() {
                return Ok(val);
            }
        }

        let info = self.node_info_by_id(node_id).await?;
        let client = TcpStream::connect(info.tcp_addr).await?;

        return Ok(client);
    }

    fn put_node_conn(&self, node_id: u64, conn: TcpStream) {
        let mut write = self.conn_map.write();
        let entry = write
            .entry(node_id)
            .or_insert_with(|| VecDeque::with_capacity(32));

        // close too more idle connection
        if entry.len() < 32 {
            entry.push_back(conn);
        }
    }

    async fn retain_id(&self, count: u32) -> MetaResult<u32> {
        let req = command::WriteCommand::RetainID(self.config.name.clone(), count);
        let rsp = self.client.write::<command::StatusResponse>(&req).await?;
        if rsp.code != command::META_REQUEST_SUCCESS {
            return Err(MetaError::CommonError {
                msg: format!("retain id err: {} {}", rsp.code, rsp.msg),
            });
        }

        let id = serde_json::from_str::<u32>(&rsp.msg).unwrap_or(0);
        if id == 0 {
            return Err(MetaError::CommonError {
                msg: format!("retain id err: {} ", rsp.msg),
            });
        }

        Ok(id)
    }

    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
        let strs: Vec<&str> = entry.key.split('/').collect();

        let len = strs.len();
        if len == 4 && strs[2] == key_path::DATA_NODES {
            if let Ok(node_id) = serde_json::from_str::<u64>(strs[3]) {
                if entry.tye == command::ENTRY_LOG_TYPE_SET {
                    if let Ok(info) = serde_json::from_str::<NodeInfo>(&entry.val) {
                        self.data_nodes.write().insert(node_id, info);
                    }
                } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                    self.data_nodes.write().remove(&node_id);
                    self.conn_map.write().remove(&node_id);
                }
            }
        }

        Ok(())
    }

    fn heartbeat(&self) {}
}
