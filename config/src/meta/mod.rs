mod cluster_config;
mod global_config;
mod heart_beat_config;
mod sys_config;

use std::collections::HashMap;
use std::path::Path;

use figment::providers::{Env, Format, Toml};
use figment::value::Uncased;
use figment::{Error, Figment};
pub use heart_beat_config::*;
use macros::EnvKeys;
use serde::{Deserialize, Serialize};

use crate::common::LogConfig;
use crate::meta::cluster_config::MetaClusterConfig;
use crate::meta::global_config::MetaGlobalConfig;
use crate::meta::sys_config::SysConfig;
use crate::EnvKeys as _;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
#[serde(default = "Default::default")]
#[derive(Default)]
pub struct Opt {
    #[serde(default)]
    pub global: MetaGlobalConfig,
    #[serde(default)]
    pub cluster: MetaClusterConfig,
    #[serde(default)]
    pub sys_config: SysConfig,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub heartbeat: HeartBeatConfig,
}

impl Opt {
    pub fn to_string_pretty(&self) -> String {
        toml::to_string_pretty(self).unwrap_or_else(|_| "Failed to stringify Config".to_string())
    }
}

pub fn get_opt(path: Option<impl AsRef<Path>>) -> Result<Opt, Error> {
    let env_keys = Opt::env_keys();
    let env_key_map = env_keys
        .into_iter()
        .map(|key| (format!("CNOSDB_META_{}", key.replace('.', "_")), key))
        .collect::<HashMap<String, String>>();

    // debug
    // println!(
    //     "-----------------------------------------meta:Environment Variable to Field Mapping:"
    // );
    // for (env_var, field_name) in &env_key_map {
    //     let value = std::env::var(env_var).unwrap_or_else(|_| "Not Set".to_string());
    //     println!(
    //         "Environment Variable: {}, Field Name: {}, Value: {}",
    //         env_var, field_name, value
    //     );
    // }

    let mut figment = Figment::new();
    if let Some(path) = path.as_ref() {
        figment = figment.merge(Toml::file(path.as_ref()));
    }

    figment = figment.merge(Env::prefixed("CNOSDB_META_").map(move |env| {
        let env_str = env.to_string();
        match env_key_map.get(&format!("CNOSDB_META_{}", env_str)) {
            Some(key) => Uncased::from_owned(key.clone()),
            None => Uncased::new(env_str.clone()),
        }
    }));

    figment.extract()
}

#[cfg(test)]
mod test {
    use crate::meta::Opt;

    #[test]
    fn test() {
        let config_str = r#"
[global]
node_id = 1
cluster_name = "cluster_xxx"
raft_node_host = "127.0.0.1"
listen_port = 8901
grpc_enable_gzip = false
data_path = "/var/lib/cnosdb/meta"

[cluster]
lmdb_max_map_size = 10485760
heartbeat_interval = 3000
raft_logs_to_keep = 10000
install_snapshot_timeout = 3600000
send_append_entries_timeout = 5000

[sys_config]
usage_schema_cache_size = 2097152
cluster_schema_cache_size = 2097152
system_database_replica = 1

[log]
level = "warn"
path = "/tmp/cnosdb/logs"


[heartbeat]
heartbeat_recheck_interval = 30
heartbeat_expired_interval = 60
"#;

        let config: Opt = toml::from_str(config_str).unwrap();
        assert!(toml::to_string_pretty(&config).is_ok());
        dbg!(config);
    }
}
