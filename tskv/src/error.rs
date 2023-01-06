use error_code::ErrorCode;
use error_code::ErrorCoder;
use meta::error::MetaError;
use models::SeriesId;
use snafu::Snafu;
use std::path::{Path, PathBuf};

use crate::schema::error::SchemaError;
use crate::{
    tsm::{ReadTsmError, WriteTsmError},
    wal,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "02")]
pub enum Error {
    Meta {
        source: MetaError,
    },

    #[snafu(display("Invalid flatbuffers: {}", source))]
    #[error_code(code = 1)]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("Tags or fields can't be empty"))]
    #[error_code(code = 2)]
    InvalidPoint,

    #[snafu(display("{}", reason))]
    #[error_code(code = 3)]
    CommonError {
        reason: String,
    },

    #[snafu(display("DataSchemaError: {}", source))]
    #[error_code(code = 4)]
    Schema {
        source: SchemaError,
    },

    // Internal Error
    #[snafu(display("{}", source))]
    IO {
        source: std::io::Error,
    },

    #[snafu(display("Unable to open file '{}': {}", path.display(), source))]
    OpenFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error with read file '{}': {}", path.display(), source))]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to write file '{}': {}", path.display(), source))]
    WriteFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to sync file: {}", source))]
    SyncFile {
        source: std::io::Error,
    },

    #[snafu(display("File {} has wrong name format: {}", file_name, message))]
    InvalidFileName {
        file_name: String,
        message: String,
    },

    #[snafu(display("File '{}' has wrong format: {}", path.display(), message))]
    InvalidFileFormat {
        path: PathBuf,
        message: String,
    },

    #[snafu(display("fails to send to channel"))]
    Send,

    #[snafu(display("fails to receive from channel"))]
    Receive {
        source: tokio::sync::oneshot::error::RecvError,
    },

    #[snafu(display("wal truncated"))]
    WalTruncated,

    #[snafu(display("read/write record file block: {}", reason))]
    RecordFileIo {
        reason: String,
    },

    #[snafu(display("Unexpected eof"))]
    Eof,

    #[snafu(display("read record file block: {}", source))]
    Encode {
        source: bincode::Error,
    },

    #[snafu(display("read record file block: {}", source))]
    Decode {
        source: bincode::Error,
    },

    #[snafu(display("Index: : {}", source))]
    IndexErr {
        source: crate::index::IndexError,
    },

    #[snafu(display("error apply edits to summary"))]
    ErrApplyEdit,

    #[snafu(display("read tsm block file error: {}", source))]
    ReadTsm {
        source: ReadTsmError,
    },

    #[snafu(display("write tsm block file error: {}", source))]
    WriteTsm {
        source: WriteTsmError,
    },

    #[snafu(display("character set error"))]
    ErrCharacterSet,

    #[snafu(display("Invalid parameter : {}", reason))]
    InvalidParam {
        reason: String,
    },

    #[snafu(display("file has no footer"))]
    NoFooter,
}

impl From<crate::index::IndexError> for Error {
    fn from(err: crate::index::IndexError) -> Self {
        Error::IndexErr { source: err }
    }
}

impl From<SchemaError> for Error {
    fn from(value: SchemaError) -> Self {
        match value {
            SchemaError::Meta { source } => Self::Meta { source },
            other => Error::Schema { source: other },
        }
    }
}

impl Error {
    pub fn error_code(&self) -> &dyn ErrorCode {
        match self {
            Error::Meta { source } => source.error_code(),
            _ => self,
        }
    }
}

#[test]
fn test_mod_code() {
    let e = Error::Schema {
        source: SchemaError::ColumnAlreadyExists {
            name: "".to_string(),
        },
    };
    assert!(e.code().starts_with("02"));
}
