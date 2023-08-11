#![allow(dead_code)]
#![allow(unused)]
use std::fmt::{Debug, Display};
use std::io::Cursor;
use std::sync::Arc;

use network_client::NetworkClient;
use node_store::NodeStorage;
use openraft::storage::Adaptor;
use openraft::{AppData, AppDataResponse, Config, RaftNetworkFactory, TokioRuntime};
use serde::{Deserialize, Serialize};

pub mod apply_store;
pub mod entry_store;
pub mod errors;

pub mod network_client;
pub mod node_store;
pub mod raft_node;
pub mod state_store;

pub type RaftNodeId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct RaftNodeInfo {
    pub group_id: u32,   // raft group id
    pub address: String, // server address
}

pub trait AppTypeConfig {
    type Request: AppData;
    type Response: AppDataResponse;
    type RaftNetwork: RaftNetworkFactory<TypeConfig>;
}

// #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
// #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
// pub struct TypeConfig {}
// impl openraft::RaftTypeConfig for TypeConfig {
//     type D = Request;
//     type R = Response;
//     type NodeId = RaftNodeId;
//     type Node = RaftNodeInfo;
//     type Entry = openraft::Entry<TypeConfig>;
//     type SnapshotData = Cursor<Vec<u8>>;
//     type AsyncRuntime = TokioRuntime;
// }
openraft::declare_raft_types!(
    /// Declare the type configuration.
    pub TypeConfig: D = Request, R = Response, NodeId = RaftNodeId, Node = RaftNodeInfo,
    Entry = openraft::Entry<TypeConfig>, SnapshotData = Cursor<Vec<u8>>, AsyncRuntime = TokioRuntime
);

type LocalLogStore = Adaptor<TypeConfig, Arc<NodeStorage>>;
type LocalStateMachineStore = Adaptor<TypeConfig, Arc<NodeStorage>>;
pub type OpenRaftNode =
    openraft::Raft<TypeConfig, NetworkClient, LocalLogStore, LocalStateMachineStore>;

//-----------------------------------------------------------------//

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum Request {
    Set { key: String, value: String },
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Response {
    pub value: Option<String>,
}
