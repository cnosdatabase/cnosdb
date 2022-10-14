#![doc = include_str!("../README.md")]
pub const CNOSDB_CLI_VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod command;
pub mod config;
pub mod ctx;
pub mod exec;
pub mod functions;
pub mod helper;
pub mod print_format;
pub mod print_options;
