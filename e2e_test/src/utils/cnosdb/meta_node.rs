use std::collections::HashMap;
use std::ffi::OsStr;
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use config::meta::{get_opt as read_meta_store_config, Opt as MetaStoreConfig};
use meta::client::MetaHttpClient;
use metrics::metric_register::MetricsRegister;
use tokio::runtime::Runtime;

use super::FnUpdateMetaStoreConfig;
use crate::cluster_def::MetaNodeDefinition;
use crate::utils::{kill_child_process, Client, PROFILE};

pub struct CnosdbMetaTestHelper {
    pub runtime: Arc<Runtime>,
    pub workspace_dir: PathBuf,
    /// The meta test dir, usually /e2e_test/$mod/$test/meta
    pub test_dir: PathBuf,
    pub meta_node_definitions: Vec<MetaNodeDefinition>,
    pub meta_node_configs: Vec<MetaStoreConfig>,
    pub exe_path: PathBuf,

    pub client: Arc<Client>,
    pub meta_client: Arc<MetaHttpClient>,
    pub sub_processes: HashMap<String, Child>,
}

impl CnosdbMetaTestHelper {
    pub fn new(
        runtime: Arc<Runtime>,
        workspace_dir: impl AsRef<Path>,
        test_base_dir: impl AsRef<Path>,
        meta_node_definitions: Vec<MetaNodeDefinition>,
        meta_node_configs: Vec<MetaStoreConfig>,
    ) -> Self {
        let workspace_dir = workspace_dir.as_ref().to_path_buf();
        let meta_host_port = if meta_node_definitions.is_empty() {
            "127.0.0.1:8901".to_string()
        } else {
            meta_node_definitions[0].host_port.to_string()
        };
        Self {
            runtime,
            workspace_dir: workspace_dir.clone(),
            test_dir: test_base_dir.as_ref().to_path_buf(),
            meta_node_definitions,
            meta_node_configs,
            exe_path: workspace_dir
                .join("target")
                .join(PROFILE)
                .join("cnosdb-meta"),
            client: Arc::new(Client::new()),
            meta_client: Arc::new(MetaHttpClient::new(
                &meta_host_port,
                Arc::new(MetricsRegister::default()),
            )),
            sub_processes: HashMap::with_capacity(3),
        }
    }

    pub fn run_single_meta(&mut self) {
        println!("Running cnosdb-meta at '{}'", self.workspace_dir.display());
        if self.meta_node_definitions.is_empty() {
            panic!("At least 1 meta configs are needed to run singleton");
        }
        let node_def = self.meta_node_definitions[0].clone();
        println!(
            "- Running cnosdb-meta with config '{}'",
            &node_def.config_file_name
        );
        let proc = self.execute(&node_def);
        self.sub_processes.insert(node_def.config_file_name, proc);

        self.wait_startup(self.meta_node_definitions[0].host_port)
            .join()
            .unwrap();

        println!("- Init cnosdb-meta ...");
        let master_host = format!("http://{}", &self.meta_node_definitions[0].host_port);
        self.client
            .post_json(format!("{master_host}/init").as_str(), "{}")
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        self.client
            .post_json(format!("{master_host}/change-membership").as_str(), "[1]")
            .unwrap();
        thread::sleep(Duration::from_secs(1));
    }

    pub fn run_cluster(&mut self) {
        println!(
            "Running cnosdb-meta cluster at '{}'",
            self.workspace_dir.display()
        );
        if self.meta_node_definitions.len() < 2 {
            panic!("At least 2 meta configs are needed to run cluster");
        }

        let master_host = format!("http://{}", &self.meta_node_definitions[0].host_port);

        let mut wait_startup_threads = Vec::with_capacity(self.meta_node_definitions.len());
        for meta_node_def in self.meta_node_definitions.iter() {
            println!(
                "- Running cnosdb-meta with config '{}', host '{}",
                meta_node_def.config_file_name, meta_node_def.host_port
            );
            let proc = self.execute(meta_node_def);
            self.sub_processes
                .insert(meta_node_def.config_file_name.clone(), proc);
            wait_startup_threads.push(self.wait_startup(meta_node_def.host_port));
        }
        thread::sleep(Duration::from_secs(3));
        for jh in wait_startup_threads {
            jh.join().unwrap();
        }

        println!("- Installing cnosdb-meta cluster...");
        // Call $host/init for master node
        self.client
            .post_json(format!("{master_host}/init").as_str(), "{}")
            .unwrap();
        thread::sleep(Duration::from_secs(1));

        // Call $host/add-learner for all follower nodes
        let mut all_node_ids = "[1".to_string();
        for meta_node_def in self.meta_node_definitions.iter().skip(1) {
            self.client
                .post_json(
                    format!("{master_host}/add-learner").as_str(),
                    format!("[{}, \"{}\"]", meta_node_def.id, meta_node_def.host_port).as_str(),
                )
                .unwrap();
            all_node_ids.push_str(format!(", {}", meta_node_def.id).as_str());
            thread::sleep(Duration::from_secs(1));
        }
        all_node_ids.push(']');

        // Call $host/change-membership
        self.client
            .post_json(
                format!("{master_host}/change-membership").as_str(),
                all_node_ids.as_str(),
            )
            .unwrap();
        thread::sleep(Duration::from_secs(1));
    }

    /// Wait cnosdb-meta startup by checking ping api in loop
    pub fn wait_startup(&self, addr: SocketAddrV4) -> thread::JoinHandle<()> {
        let test_api = format!("http://{addr}/debug");
        let startup_time = std::time::Instant::now();
        let client = self.client.clone();
        thread::spawn(move || {
            let mut counter = 0;
            loop {
                thread::sleep(Duration::from_secs(3));
                if let Err(e) = client.get(&test_api, "") {
                    println!(
                        "HTTP get '{test_api}' failed after {} seconds: {}",
                        startup_time.elapsed().as_secs(),
                        e
                    );
                } else {
                    break;
                }
                counter += 1;
                if counter == 30 {
                    panic!("Test case failed, waiting too long for {addr} to startup");
                }
            }
        })
    }

    fn execute(&self, node_def: &MetaNodeDefinition) -> Child {
        let config_file_path = node_def.to_config_path(&self.test_dir);
        println!(
            "Executing {} -c {}",
            self.exe_path.display(),
            config_file_path.display()
        );
        Command::new(&self.exe_path)
            .args([OsStr::new("-c"), config_file_path.as_os_str()])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb-meta")
    }

    pub fn stop_one_node(&mut self, config_file_name: &str, force: bool) {
        let proc = self
            .sub_processes
            .remove(config_file_name)
            .unwrap_or_else(|| panic!("No meta node created with {}", config_file_name));
        kill_child_process(proc, force);
    }
}

impl Drop for CnosdbMetaTestHelper {
    fn drop(&mut self) {
        for (k, p) in self.sub_processes.drain() {
            println!("Killing cnosdb-meta ({k}) sub_processes: {}", p.id());
            kill_child_process(p, true);
        }
    }
}

/// Build meta store config with paths:
///
/// - data_path: $test_dir/meta/$meta_dir_name/meta
/// - log.level: INFO
/// - log.path: $test_dir/meta/$meta_dir_name/log
pub fn build_meta_node_config(test_dir: impl AsRef<Path>, meta_dir_name: &str) -> MetaStoreConfig {
    let mut config = MetaStoreConfig::default();
    let test_dir = test_dir.as_ref().display();
    config.global.data_path = format!("{test_dir}/meta/{meta_dir_name}/meta");
    config.log.level = "INFO".to_string();
    config.log.path = format!("{test_dir}/meta/{meta_dir_name}/log");

    config
}

/// Build meta store config with paths and write to test_dir.
/// Will be write to $test_dir/meta/config/$config_file_name.
pub fn write_meta_node_config_files(
    test_dir: impl AsRef<Path>,
    meta_node_definitions: &[MetaNodeDefinition],
    regenerate: bool,
    regenerate_update_config: &[Option<FnUpdateMetaStoreConfig>],
) -> Vec<MetaStoreConfig> {
    let meta_config_dir = test_dir.as_ref().join("meta").join("config");
    std::fs::create_dir_all(&meta_config_dir).unwrap();
    let mut meta_configs = Vec::with_capacity(meta_node_definitions.len());
    for (i, meta_node_def) in meta_node_definitions.iter().enumerate() {
        let config_path = meta_config_dir.join(&meta_node_def.config_file_name);
        let mut meta_config;
        if regenerate {
            meta_config = build_meta_node_config(&test_dir, &meta_node_def.config_file_name);
            meta_node_def.update_config(&mut meta_config);
            if let Some(Some(f)) = regenerate_update_config.get(i) {
                f(&mut meta_config);
            }
            std::fs::write(&config_path, meta_config.to_string_pretty()).unwrap();
        } else {
            meta_config = read_meta_store_config(Some(config_path)).unwrap();
        }
        meta_configs.push(meta_config);
    }
    meta_configs
}
