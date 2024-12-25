#![allow(dead_code, clippy::field_reassign_with_default)]

use std::process;

use clap::{Parser, Subcommand};
use config::VERSION;
use meta::meta_cluster_command::add_node::add_node;
use meta::meta_cluster_command::dump::dump;
use meta::meta_cluster_command::dumpsql::dumpsql;
use meta::meta_cluster_command::meta_init::meta_init;
use meta::meta_cluster_command::remove_node::remove_node;
use meta::meta_cluster_command::restore::restore;
use meta::meta_cluster_command::show_nodes::show_nodes;
use meta::signal;
use trace::error;
use trace::global_logging::init_global_logging;

#[derive(Debug, Parser)]
#[command(name = "cnosdb-meta", version = &VERSION[..])]
struct Cli {
    /// Subcommands
    #[command(subcommand)]
    command: Option<Commands>,

    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print the default configuration
    Config,
    /// Initialize the meta service in the cluster
    Init {
        /// Cluster initialization address
        #[arg(long)]
        bind: String,
    },
    /// Add a node to the meta service in the cluster
    AddNode {
        /// Address of the cluster leader node
        #[arg(long)]
        bind: String,
        /// Address of the node to be added
        #[arg(long)]
        addr: String,
    },
    /// Remove a node from the meta service in the cluster
    RemoveNode {
        /// Address of the cluster leader node
        #[arg(long)]
        bind: String,
        /// Address of the node to be removed
        #[arg(long)]
        addr: String,
    },
    /// Dump the cluster’s meta resources in SQL format to a file
    DumpSql {
        /// Address of a cluster node
        #[arg(long)]
        bind: String,
        /// Name of the cluster
        #[arg(long)]
        cluster: String,
        /// File path to save the backup
        #[arg(long)]
        file: String,
    },
    /// Dump the cluster's meta resources to a file
    Dump {
        /// Address of a cluster node
        #[arg(long)]
        bind: String,
        /// File path to save the exported data
        #[arg(long)]
        file: String,
    },
    /// Restore the cluster's meta resources from a file
    Restore {
        /// Address of the cluster leader node
        #[arg(long)]
        bind: String,
        /// File path to restore the data from
        #[arg(long)]
        file: String,
    },
    /// List information about all nodes in the cluster
    ShowNodes {
        /// Address of the node
        #[arg(long)]
        bind: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Config) => {
            // Print default configuration
            let default_config = config::meta::Opt::default();
            println!("Default configuration:");
            println!("{}", default_config.to_string_pretty());
        }
        Some(Commands::Init { bind }) => {
            if let Err(e) = meta_init(&bind).await {
                eprintln!("Error initializing meta service: {}", e);
            }
        }
        Some(Commands::AddNode { bind, addr }) => {
            if let Err(e) = add_node(&bind, &addr).await {
                eprintln!("Error adding node: {}", e);
            }
        }
        Some(Commands::RemoveNode { bind, addr }) => {
            if let Err(e) = remove_node(&bind, &addr).await {
                eprintln!("Error removing node: {}", e);
            }
        }
        Some(Commands::DumpSql {
            bind,
            cluster,
            file,
        }) => {
            if let Err(e) = dumpsql(&bind, &cluster, &file).await {
                eprintln!("Error backing up meta service: {}", e);
            }
        }
        Some(Commands::Restore { bind, file }) => {
            if let Err(e) = restore(&bind, &file).await {
                eprintln!("Error restoring meta service: {}", e);
            }
        }
        Some(Commands::Dump { bind, file }) => {
            if let Err(e) = dump(&bind, &file).await {
                eprintln!("Error exporting meta service: {}", e);
            }
        }
        Some(Commands::ShowNodes { bind }) => {
            if let Err(e) = show_nodes(&bind).await {
                eprintln!("Error showing nodes: {}", e);
            }
        }
        None => {
            // use --config to start cnosdb meta
            if let Some(config_path) = cli.config {
                let mut options = match config::meta::get_opt(Some(config_path)) {
                    Ok(opt) => opt,
                    Err(e) => {
                        error!("Error loading config: {}", e);
                        process::exit(1);
                    }
                };
                println!("-----------------------------------------------------------");
                println!("Start meta server with configuration:");
                println!("{}", options.to_string_pretty());
                println!("-----------------------------------------------------------");
                options.log.path = format!("{}/{}", options.log.path, options.global.node_id);
                init_global_logging(&options.log, "meta_server.log");

                if let Err(e) = meta::service::server::start_raft_node(options).await {
                    error!("Failed to start Raft node: {:?}", e);
                    process::exit(1);
                }

                signal::block_waiting_ctrl_c();
            } else {
                eprintln!("Please provide a config file or use the 'config' command.");
            }
        }
    }
}
