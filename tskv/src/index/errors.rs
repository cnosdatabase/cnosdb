use snafu::Snafu;
use std::{fmt::Debug, io};

use crate::record_file;
use sled;

#[derive(Snafu, Debug)]
pub enum IndexError {
    #[snafu(display("Unrecognized action"))]
    Action,

    #[snafu(display("Unrecognized version"))]
    Version,

    #[snafu(display("Unrecognized FieldType: {}", msg))]
    FieldType { msg: String },

    #[snafu(display("Not Found Field"))]
    NotFoundField,

    #[snafu(display("Series not exists"))]
    SeriesNotExists,

    #[snafu(display("Decode Series ID List failed"))]
    DecodeSeriesIDList,

    #[snafu(display("Decode TableSchema failed for '{}'", table))]
    DecodeTableSchema { table: String },

    #[snafu(display("Decode Series Key failed for '{}'", msg))]
    DecodeSeriesKey { msg: String },

    #[snafu(display("index storage error: {}", msg))]
    IndexStroage { msg: String },

    #[snafu(display("roaring encode/decode error: {}", msg))]
    RoaringBitmap { msg: String },

    #[snafu(display("binlog storage error: {}", msg))]
    IOErrors { msg: String },

    #[snafu(display("file error: {}", msg))]
    FileErrors { msg: String },

    #[snafu(display("column '{}' already exists", column))]
    ColumnAlreadyExists { column: String },
}

impl From<sled::Error> for IndexError {
    fn from(err: sled::Error) -> Self {
        IndexError::IndexStroage {
            msg: err.to_string(),
        }
    }
}

impl From<io::Error> for IndexError {
    fn from(err: io::Error) -> Self {
        IndexError::IOErrors {
            msg: err.to_string(),
        }
    }
}

pub type IndexResult<T> = Result<T, IndexError>;
