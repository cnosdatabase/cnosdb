use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::vec;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    #[serde(default = "ClusterConfig::default_name")]
    pub name: String,
    #[serde(default = "ClusterConfig::default_meta_service_addr")]
    pub meta_service_addr: Vec<String>,

    #[serde(default = "ClusterConfig::default_http_listen_port")]
    pub http_listen_port: u16,
    #[serde(default = "ClusterConfig::default_grpc_listen_port")]
    pub grpc_listen_port: u16,
    #[serde(default = "ClusterConfig::default_flight_rpc_listen_port")]
    pub flight_rpc_listen_port: u16,
    #[serde(default = "ClusterConfig::default_tcp_listen_port")]
    pub tcp_listen_port: u16,
    #[serde(default = "ClusterConfig::default_store_metrics")]
    pub store_metrics: bool,

    #[serde(default = "ClusterConfig::default_cold_data_server")]
    pub cold_data_server: bool,
}

impl ClusterConfig {
    fn default_name() -> String {
        "cluster_xxx".to_string()
    }

    fn default_meta_service_addr() -> Vec<String> {
        vec!["127.0.0.1:8901".to_string()]
    }

    fn default_http_listen_port() -> u16 {
        8902
    }

    fn default_grpc_listen_port() -> u16 {
        8903
    }

    fn default_flight_rpc_listen_port() -> u16 {
        8904
    }

    fn default_tcp_listen_port() -> u16 {
        8905
    }

    fn default_store_metrics() -> bool {
        true
    }

    fn default_cold_data_server() -> bool {
        false
    }

    pub fn override_by_env(&mut self) {
        if let Ok(name) = std::env::var("CNOSDB_CLUSTER_NAME") {
            self.name = name;
        }
        if let Ok(meta_list) = std::env::var("CNOSDB_CLUSTER_META") {
            let mut list = Vec::new();
            for meta_addr in meta_list.split(';') {
                list.push(meta_addr.to_string());
            }

            self.meta_service_addr = list;
        }

        if let Ok(port) = std::env::var("CNOSDB_HTTP_LISTEN_PORT") {
            self.http_listen_port = port.parse::<u16>().unwrap();
        }

        if let Ok(port) = std::env::var("CNOSDB_GRPC_LISTEN_PORT") {
            self.grpc_listen_port = port.parse::<u16>().unwrap();
        }

        if let Ok(port) = std::env::var("CNOSDB_FLIGHT_RPC_LISTEN_PORT") {
            self.flight_rpc_listen_port = port.parse::<u16>().unwrap();
        }

        if let Ok(port) = std::env::var("CNOSDB_TCP_LISTEN_PORT") {
            self.flight_rpc_listen_port = port.parse::<u16>().unwrap();
        }

        if let Ok(val) = std::env::var("CNOSDB_STORE_METRICS") {
            self.store_metrics = val.parse::<bool>().unwrap();
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            name: Self::default_name(),
            meta_service_addr: Self::default_meta_service_addr(),
            http_listen_port: Self::default_http_listen_port(),
            grpc_listen_port: Self::default_grpc_listen_port(),
            flight_rpc_listen_port: Self::default_flight_rpc_listen_port(),
            tcp_listen_port: Self::default_tcp_listen_port(),
            store_metrics: Self::default_store_metrics(),
            cold_data_server: Self::default_cold_data_server(),
        }
    }
}

impl CheckConfig for ClusterConfig {
    fn check(&self, config: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("cluster".to_string());
        let mut ret = CheckConfigResult::default();

        if self.name.is_empty() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "name".to_string(),
                message: "'name' is empty".to_string(),
            });
        }

        for meta_addr in self.meta_service_addr.iter() {
            if let Err(e) = meta_addr.to_socket_addrs() {
                ret.add_error(CheckConfigItemResult {
                    config: config_name.clone(),
                    item: meta_addr.clone(),
                    message: format!("Cannot resolve 'meta_service_addr': {}", e),
                });
            }
        }

        let default_http_addr = format!("{}:{}", &config.host, self.http_listen_port);
        if let Err(e) = default_http_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: default_http_addr,
                message: format!("Cannot resolve 'http_listen_addr': {}", e),
            });
        }

        let default_grpc_addr = format!("{}:{}", &config.host, self.grpc_listen_port);
        if let Err(e) = default_grpc_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: default_grpc_addr,
                message: format!("Cannot resolve 'grpc_listen_addr': {}", e),
            });
        }

        let default_flight_rpc_addr = format!("{}:{}", &config.host, self.flight_rpc_listen_port);
        if let Err(e) = default_flight_rpc_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name,
                item: default_flight_rpc_addr,
                message: format!("Cannot resolve 'flight_rpc_listen_addr': {}", e),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
