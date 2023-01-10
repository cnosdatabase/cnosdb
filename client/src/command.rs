//! Command within CLI

use crate::ctx::{ResultSet, SessionContext};
use crate::functions::{display_all_functions, Function};
use crate::print_format::PrintFormat;
use crate::print_options::PrintOptions;
use clap::ArgEnum;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

/// Command
#[derive(Debug)]
pub enum Command {
    Quit,
    Help,
    ConnectDatabase(String),
    ListTables,
    DescribeTable(String),
    DescribeDatabase(String),
    ListFunctions,
    SearchFunctions(String),
    QuietMode(Option<bool>),
    OutputFormat(Option<String>),
    Write(String),
}

pub enum OutputFormat {
    ChangeFormat(String),
}

fn is_system_table_db(db: &str) -> bool {
    let db = db.to_ascii_lowercase();
    db.eq("cluster_schema") || db.eq("information_schema")
}

impl Command {
    pub async fn execute(
        &self,
        ctx: &mut SessionContext,
        print_options: &mut PrintOptions,
    ) -> std::result::Result<(), String> {
        let now = Instant::now();
        match self {
            Self::Help => print_options.print_batches(&all_commands_info(), now),
            Self::ConnectDatabase(database) => {
                if is_system_table_db(database) {
                    ctx.set_database(database);
                    return Ok(());
                }
                let old_database = ctx.get_database().to_string();
                ctx.set_database(database);
                match ctx.sql(format!("DESCRIBE DATABASE {}", database)).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        println!("Cannot connect to database {}.", database);
                        ctx.set_database(old_database.as_str());
                        Err(e)
                    }
                }
            }
            Self::ListTables => {
                let results = ctx.sql("SHOW TABLES".to_string()).await?;
                print_options.print_batches(&results, now)
            }
            Self::DescribeTable(name) => {
                let results = ctx.sql(format!("DESCRIBE TABLE {}", name)).await?;
                print_options.print_batches(&results, now)
            }
            Self::DescribeDatabase(name) => {
                let results = ctx.sql(format!("DESCRIBE DATABASE {}", name)).await?;
                print_options.print_batches(&results, now)
            }
            Self::QuietMode(quiet) => {
                if let Some(quiet) = quiet {
                    print_options.quiet = *quiet;
                    println!(
                        "Quiet mode set to {}",
                        if print_options.quiet { "true" } else { "false" }
                    );
                } else {
                    println!(
                        "Quiet mode is {}",
                        if print_options.quiet { "true" } else { "false" }
                    );
                }
                Ok(())
            }
            Self::Quit => Err("Unexpected quit, this should be handled outside".to_string()),
            Self::ListFunctions => display_all_functions(),
            Self::SearchFunctions(function) => {
                if let Ok(func) = function.parse::<Function>() {
                    let details = func.function_details()?;
                    println!("{}", details);
                    Ok(())
                } else {
                    let msg = format!("{} is not a supported function", function);
                    Err(msg)
                }
            }
            Self::OutputFormat(_) => {
                Err("Unexpected change output format, this should be handled outside".to_string())
            }
            Self::Write(path) => {
                let results = ctx.write(path).await?;
                print_options.print_batches(&results, now)
            }
        }
    }

    fn get_name_and_description(&self) -> (&'static str, &'static str) {
        match self {
            Self::Quit => ("\\q", "quit cnosdb-cli"),
            Self::ConnectDatabase(_) => ("\\c", "connect database"),
            Self::ListTables => ("\\d", "list tables"),
            Self::DescribeTable(_) => ("\\d name", "describe table"),
            Self::DescribeDatabase(_) => ("\\db name", "describe database"),
            Self::Help => ("\\?", "help"),
            Self::ListFunctions => ("\\h", "function list"),
            Self::SearchFunctions(_) => ("\\h function", "search function"),
            Self::QuietMode(_) => ("\\quiet (true|false)?", "print or set quiet mode"),
            Self::OutputFormat(_) => ("\\pset [NAME [VALUE]]", "set table output option\n(format)"),
            Self::Write(_) => ("\\w path", "line protocol"),
        }
    }
}

const ALL_COMMANDS: [Command; 11] = [
    Command::ConnectDatabase(String::new()),
    Command::ListTables,
    Command::DescribeTable(String::new()),
    Command::DescribeDatabase(String::new()),
    Command::Quit,
    Command::Help,
    Command::ListFunctions,
    Command::SearchFunctions(String::new()),
    Command::QuietMode(None),
    Command::OutputFormat(None),
    Command::Write(String::new()),
];

fn all_commands_info() -> ResultSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Command", DataType::Utf8, false),
        Field::new("Description", DataType::Utf8, false),
    ]));
    let (names, description): (Vec<&str>, Vec<&str>) = ALL_COMMANDS
        .into_iter()
        .map(|c| c.get_name_and_description())
        .unzip();
    let r = RecordBatch::try_new(
        schema,
        [names, description]
            .into_iter()
            .map(|i| Arc::new(StringArray::from(i)) as ArrayRef)
            .collect::<Vec<_>>(),
    )
    .expect("This should not fail");

    ResultSet::RecordBatches(vec![r])
}

impl FromStr for Command {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (c, arg) = if let Some((a, b)) = s.split_once(' ') {
            (a, Some(b))
        } else {
            (s, None)
        };
        Ok(match (c, arg) {
            ("q", None) => Self::Quit,
            ("c", Some(db_name)) => Self::ConnectDatabase(db_name.into()),
            ("d", None) => Self::ListTables,
            ("d", Some(name)) => Self::DescribeTable(name.into()),
            ("?", None) => Self::Help,
            ("h", None) => Self::ListFunctions,
            ("h", Some(function)) => Self::SearchFunctions(function.into()),
            ("quiet", Some("true" | "t" | "yes" | "y" | "on")) => Self::QuietMode(Some(true)),
            ("quiet", Some("false" | "f" | "no" | "n" | "off")) => Self::QuietMode(Some(false)),
            ("quiet", None) => Self::QuietMode(None),
            ("pset", Some(subcommand)) => Self::OutputFormat(Some(subcommand.to_string())),
            ("pset", None) => Self::OutputFormat(None),
            ("w", Some(path)) => Self::Write(path.into()),
            ("db", Some(db)) => Self::DescribeDatabase(db.to_string()),
            _ => return Err(()),
        })
    }
}

impl FromStr for OutputFormat {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (c, arg) = if let Some((a, b)) = s.split_once(' ') {
            (a, Some(b))
        } else {
            (s, None)
        };
        Ok(match (c, arg) {
            ("format", Some(format)) => Self::ChangeFormat(format.to_string()),
            _ => return Err(()),
        })
    }
}

impl OutputFormat {
    pub async fn execute(&self, print_options: &mut PrintOptions) -> Result<()> {
        match self {
            Self::ChangeFormat(format) => {
                if let Ok(format) = format.parse::<PrintFormat>() {
                    print_options.format = format;
                    println!("Output format is {:?}.", print_options.format);
                    Ok(())
                } else {
                    Err(DataFusionError::Execution(format!(
                        "{:?} is not a valid format type [possible values: {:?}]",
                        format,
                        PrintFormat::value_variants()
                    )))
                }
            }
        }
    }
}
