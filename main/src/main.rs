#![allow(dead_code)]
#![recursion_limit = "256"]

use std::fmt::Display;
use std::process;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::{command, Args, Parser, Subcommand, ValueEnum};
use config::tskv::Config;
use config::VERSION;
use memory_pool::GreedyMemoryPool;
use metrics::metric_register::MetricsRegister;
use tokio::runtime::Runtime;
use tokio::time::sleep;
use trace::global_logging::init_global_logging;
use trace::global_tracing::{finalize_global_tracing, init_global_tracing};
use trace::{error, info};

use crate::report::ReportService;

mod flight_sql;
mod http;
mod opentelemetry;
mod report;
mod rpc;
mod server;
mod signal;
mod spi;
mod tcp;

/// cli examples is here
/// <https://github.com/clap-rs/clap/blob/v3.1.3/examples/git-derive.rs>
#[derive(Debug, Parser)]
#[command(name = "cnosdb", version = & VERSION[..])]
#[command(about = "CnosDB command line tools")]
#[command(long_about = r#"CnosDB and command line tools
Examples:
    # Run the CnosDB:
    cnosdb run
    # Check configuration file:
    cnosdb check server-config ./config/config.toml"#)]
struct Cli {
    #[command(subcommand)]
    subcmd: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Run CnosDB server.
    Run(RunArgs),
    /// Print default configurations.
    Config,
    /// Check the configuration file in the given path.
    Check {
        #[command(subcommand)]
        subcmd: CheckCommand,
    },
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Number of CPUs on the system, the default value is 4
    #[arg(short, long, global = true)]
    cpu: Option<usize>,

    /// Gigabytes(G) of memory on the system, the default value is 16
    #[arg(short, long, global = true)]
    memory: Option<usize>,

    /// Path to configuration file.
    #[arg(long, global = true, default_value = "/etc/cnosdb/cnosdb.conf")]
    config: String,

    /// The deployment mode of CnosDB,
    #[arg(short = 'M', long, global = true, value_enum)]
    deployment_mode: Option<DeploymentMode>,
}

#[derive(Debug, Copy, Clone, ValueEnum)]
#[clap(rename_all = "snake_case")]
enum DeploymentMode {
    /// Default, Run query and tskv engines.
    QueryTskv,
    /// Only run the tskv engine.
    Tskv,
    /// Only run the query engine.
    Query,
    /// Stand-alone deployment.
    Singleton,
}

impl FromStr for DeploymentMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mode = match s.to_ascii_lowercase().as_str() {
            "query_tskv" => Self::QueryTskv,
            "tskv" => Self::Tskv,
            "query" => Self::Query,
            "singleton" => Self::Singleton,
            _ => {
                return Err(
                    "deployment must be one of [query_tskv, tskv, query, singleton]".to_string(),
                )
            }
        };
        Ok(mode)
    }
}

impl Display for DeploymentMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentMode::QueryTskv => write!(f, "query_tskv"),
            DeploymentMode::Tskv => write!(f, "tskv"),
            DeploymentMode::Query => write!(f, "query"),
            DeploymentMode::Singleton => write!(f, "singleton"),
        }
    }
}

#[derive(Debug, Subcommand)]
enum CheckCommand {
    /// Check server configurations.
    #[command(arg_required_else_help = false)]
    ServerConfig {
        /// Print warnings.
        #[arg(short, long)]
        show_warnings: bool,
        /// Path to configuration file.
        config: String,
    },
    // /// Check meta server configurations.
    // #[command(arg_required_else_help = false)]
    // MetaConfig {},
}

#[cfg(unix)]
#[global_allocator]
static A: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// To run cnosdb-cli:
///
/// ```bash
/// cargo run -- run
/// ```
fn main() -> Result<(), std::io::Error> {
    signal::install_crash_handler();
    let cli = Cli::parse();
    let run_args = match cli.subcmd {
        CliCommand::Run(run_args) => run_args,
        CliCommand::Config => {
            println!("{}", Config::default().to_string_pretty());
            return Ok(());
        }
        CliCommand::Check { subcmd } => match subcmd {
            CheckCommand::ServerConfig {
                config,
                show_warnings,
            } => {
                config::tskv::check_config(config, show_warnings);
                return Ok(());
            }
        },
    };

    let config = parse_config(&run_args);
    let deployment_mode = get_deployment_mode(&config.deployment.mode)?;

    init_global_logging(&config.log, "tsdb.log");
    info!("CnosDB init config: {:?}", config);

    let runtime = Arc::new(init_runtime(Some(config.deployment.cpu))?);
    let mem_bytes = run_args.memory.unwrap_or(config.deployment.memory) * 1024 * 1024 * 1024;
    let memory_pool = Arc::new(GreedyMemoryPool::new(mem_bytes));
    runtime.clone().block_on(async move {
        let mode = &config.deployment.mode;
        let node_id = config.global.node_id;
        init_global_tracing(&config.trace, format!("cnosdb_{mode}_{node_id}"));
        let builder = server::ServiceBuilder {
            cpu: config.deployment.cpu,
            config: config.clone(),
            runtime: runtime.clone(),
            memory_pool: memory_pool.clone(),
            metrics_register: Arc::new(MetricsRegister::new([(
                "node_id",
                config.global.node_id.to_string(),
            )])),
        };

        let mut server = server::Server::default();
        if config.service.enable_report {
            server.add_service(Box::new(ReportService::new()));
        }

        let _ = std::fs::create_dir_all(config.storage.path);
        let (storage, coordinator) = match deployment_mode {
            DeploymentMode::QueryTskv => handle_error(
                builder.build_query_storage(&mut server).await,
                "Failed to build query storage",
            ),
            DeploymentMode::Tskv => handle_error(
                builder.build_storage_server(&mut server).await,
                "Failed to build storage server",
            ),
            DeploymentMode::Query => handle_error(
                builder.build_query_server(&mut server).await,
                "Failed to build query server",
            ),
            DeploymentMode::Singleton => handle_error(
                builder.build_singleton(&mut server).await,
                "Failed to build singleton server",
            ),
        };

        info!("CnosDB server start as {} mode", deployment_mode);
        server.start().expect("CnosDB server start.");
        signal::block_waiting_ctrl_c();
        let raft_manager = coordinator.raft_manager();
        let writer_count = coordinator.get_writer_count();
        server.stop(true).await;
        info!(
            "Waiting for write requests, current number of requests {}",
            writer_count.load(Ordering::Relaxed)
        );
        wait_for_writers(writer_count).await;
        raft_manager.sync_wal_writer().await;

        if let Some(tskv) = storage {
            tskv.close().await;
        }

        finalize_global_tracing();

        println!("CnosDB is stopped.");
    });
    Ok(())
}
fn handle_error<T, E: std::fmt::Debug>(result: Result<T, E>, context: &str) -> T {
    result.unwrap_or_else(|e| {
        error!("{}: {:?}", context, e);
        process::exit(1);
    })
}
fn parse_config(run_args: &RunArgs) -> config::tskv::Config {
    println!("-----------------------------------------------------------");
    println!("Using Config File: {}\n", run_args.config);
    let mut config = config::tskv::get_config(&run_args.config).unwrap();
    set_cli_args_to_config(run_args, &mut config);
    println!("Start with configuration: \n{}", config.to_string_pretty());
    println!("-----------------------------------------------------------");

    config
}

fn init_runtime(cores: Option<usize>) -> Result<Runtime, std::io::Error> {
    use tokio::runtime::Builder;
    match cores {
        None => Runtime::new(),
        Some(cores) => match cores {
            0 => Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(4 * 1024 * 1024)
                .build(),
            _ => Builder::new_multi_thread()
                .enable_all()
                .worker_threads(cores)
                .thread_stack_size(4 * 1024 * 1024)
                .build(),
        },
    }
}
/// Merge the deployment configs(mode) between CLI arguments and config file,
/// values in the CLI arguments (if any) has higher priority.
fn get_deployment_mode(config_deployment_mode: &str) -> Result<DeploymentMode, std::io::Error> {
    match config_deployment_mode.parse::<DeploymentMode>() {
        Ok(mode) => Ok(mode),
        Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
    }
}

/// When the command line and the configuration file specify a setting at the same time,
/// the command line has higher priority
fn set_cli_args_to_config(args: &RunArgs, config: &mut Config) {
    if let Some(mode) = args.deployment_mode {
        config.deployment.mode = mode.to_string();
    }

    if let Some(m) = args.memory {
        config.deployment.memory = m;
    }

    if let Some(c) = args.cpu {
        config.deployment.cpu = c;
    }
}

async fn wait_for_writers(writer_count: Arc<AtomicUsize>) {
    while writer_count.load(Ordering::Relaxed) > 0 {
        sleep(Duration::from_millis(10)).await;
    }
}
