use std::error::Error;
use std::path::{Path, PathBuf};

use sqllogictest::{default_column_validator, default_validator};
use trace::info;

use crate::instance::{CnosDBClient, SqlClientOptions};

mod error;
mod instance;
mod utils;

const TEST_DIRECTORY: &str = "query_server/sqllogicaltests/cases";

const CNOSDB_HOST_ENV: &str = "CNOSDB_HOST";
const CNOSDB_PORT_ENV: &str = "CNOSDB_PORT";

const CNOSDB_HOST_DEFAULT: &str = "localhost";
const CNOSDB_PORT_DEFAULT: u16 = 31004;
const CNOSDB_USERNAME_DEFAULT: &str = "root";
const CNOSDB_PASSWORD_DEFAULT: &str = "";
const CNOSDB_TENANT_DEFAULT: &str = "cnosdb";
const CNOSDB_DB_DEFAULT: &str = "public";
const CNOSDB_TARGET_PARTITIONS_DEFAULT: usize = 8;

#[tokio::main]
#[cfg(target_family = "windows")]
pub async fn main() -> Result<(), Box<dyn Error>> {
    println!("Skipping test on windows");
    Ok(())
}

#[tokio::main]
#[cfg(not(target_family = "windows"))]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let options = Options::new();

    let db_options = SqlClientOptions {
        host: options.host.clone(),
        port: options.port,
        username: CNOSDB_USERNAME_DEFAULT.into(),
        password: CNOSDB_PASSWORD_DEFAULT.into(),
        tenant: CNOSDB_TENANT_DEFAULT.into(),
        db: CNOSDB_DB_DEFAULT.into(),
        target_partitions: CNOSDB_TARGET_PARTITIONS_DEFAULT,
    };

    println!("{options:?}");
    println!("{db_options:?}");

    for (path, relative_path) in read_test_files(&options) {
        if options.complete_mode {
            run_complete_file(&path, relative_path, db_options.clone()).await?;
        } else {
            run_test_file(&path, relative_path, db_options.clone()).await?;
        }
    }

    Ok(())
}

async fn run_test_file(
    path: &Path,
    relative_path: PathBuf,
    options: SqlClientOptions,
) -> Result<(), Box<dyn Error>> {
    info!("Running with DataFusion runner: {}", path.display());
    let client = CnosDBClient::new(relative_path, options);
    let mut runner = sqllogictest::Runner::new(client);
    runner.run_file_async(path).await?;
    Ok(())
}

async fn run_complete_file(
    path: &Path,
    relative_path: PathBuf,
    options: SqlClientOptions,
) -> Result<(), Box<dyn Error>> {
    info!("Using complete mode to complete: {}", path.display());

    let client = CnosDBClient::new(relative_path, options);
    let mut runner = sqllogictest::Runner::new(client);
    let col_separator = " ";
    let validator = default_validator;
    let column_validator = default_column_validator;
    runner
        .update_test_file(path, col_separator, validator, column_validator)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

fn read_test_files<'a>(options: &'a Options) -> Box<dyn Iterator<Item = (PathBuf, PathBuf)> + 'a> {
    Box::new(
        read_dir_recursive(TEST_DIRECTORY)
            .map(|path| {
                (
                    path.clone(),
                    PathBuf::from(path.to_string_lossy().strip_prefix(TEST_DIRECTORY).unwrap()),
                )
            })
            .filter(|(_, relative_path)| options.check_test_file(relative_path)),
    )
}

fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Box<dyn Iterator<Item = PathBuf>> {
    Box::new(
        std::fs::read_dir(path)
            .expect("Readable directory")
            .map(|path| path.expect("Readable entry").path())
            .flat_map(|path| {
                if path.is_dir() {
                    read_dir_recursive(path)
                } else {
                    Box::new(std::iter::once(path))
                }
            }),
    )
}

/// Parsed command line options
#[derive(Debug)]
struct Options {
    // regex like
    /// arguments passed to the program which are treated as
    /// cargo test filter (substring match on filenames)
    filters: Vec<String>,

    /// Auto complete mode to fill out expected results
    complete_mode: bool,

    host: String,
    port: u16,
}

impl Options {
    fn new() -> Self {
        let args: Vec<_> = std::env::args().collect();

        let complete_mode = args.iter().any(|a| a == "--complete");
        let host = std::env::var(CNOSDB_HOST_ENV).unwrap_or(CNOSDB_HOST_DEFAULT.into());
        let port = std::env::var(CNOSDB_PORT_ENV).map_or(CNOSDB_PORT_DEFAULT, |e| {
            e.parse::<u16>().expect("Parse CNOSDB_PORT")
        });

        // treat args after the first as filters to run (substring matching)
        let filters = if !args.is_empty() {
            args.into_iter()
                .skip(1)
                // ignore command line arguments like `--complete`
                .filter(|arg| !arg.as_str().starts_with("--"))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        Self {
            filters,
            complete_mode,
            host,
            port,
        }
    }

    fn check_test_file(&self, relative_path: &Path) -> bool {
        if self.filters.is_empty() {
            return true;
        }

        // otherwise check if any filter matches
        self.filters
            .iter()
            .any(|filter| relative_path.to_string_lossy().contains(filter))
    }
}
