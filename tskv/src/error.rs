use std::path::PathBuf;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use error_code::{ErrorCode, ErrorCoder};
use http_protocol::response::ErrorResponse;
use meta::error::MetaError;
use models::column_data_ref::ColumnDataError;
use models::meta_data::VnodeId;
use models::ModelError;
use protos::PointsError;
use snafu::{Backtrace, IntoError, Location, Snafu};
use tonic::{Code, Status};

use crate::file_system::error::FileSystemError;
use crate::index::IndexError;
use crate::record_file;
use crate::record_file::Record;
use crate::schema::error::SchemaError;
use crate::tsm::page::Page;

pub type TskvResult<T, E = TskvError> = std::result::Result<T, E>;

#[derive(Snafu, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "02")]
pub enum TskvError {
    Meta {
        source: MetaError,
    },

    #[error_code(code = 9999)]
    Unimplemented {
        msg: String,
    },

    #[snafu(display("Invalid flatbuffers: {}", source))]
    #[error_code(code = 1)]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Fields can't be empty"))]
    #[error_code(code = 2)]
    FieldsIsEmpty {
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("{}", reason))]
    #[error_code(code = 3)]
    CommonError {
        reason: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("DataSchemaError: {}", source))]
    #[error_code(code = 4)]
    Schema {
        source: SchemaError,
    },

    #[snafu(display("Memory Exhausted Retry Later"))]
    #[error_code(code = 5)]
    MemoryExhausted {
        location: Location,
        backtrace: Backtrace,
    },

    #[error_code(code = 6)]
    Arrow {
        source: ArrowError,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("read tsm block file error: {}", reason))]
    #[error_code(code = 7)]
    ReadTsm {
        reason: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Network status: {}", source))]
    #[error_code(code = 8)]
    Network {
        source: Status,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid http error response: {}", error))]
    #[error_code(code = 9)]
    ErrorResponse {
        error: ErrorResponse,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Unsupported datatype {}", dt))]
    #[error_code(code = 10)]
    UnsupportedDataType {
        dt: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Mismatched schema {}", msg))]
    #[error_code(code = 11)]
    MismatchedSchema {
        msg: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[error_code(code = 12)]
    DatafusionError {
        source: DataFusionError,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("ColumnGroupError: {}", reason))]
    #[error_code(code = 13)]
    TsmColumnGroupError {
        reason: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("TsmPageError: {}", reason))]
    #[error_code(code = 14)]
    TsmPageError {
        reason: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("DataBlockError: {}", reason))]
    #[error_code(code = 15)]
    DataBlockError {
        reason: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("TagError: {}", reason))]
    #[error_code(code = 16)]
    TagError {
        reason: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("ColumnDataError: {}", source))]
    #[error_code(code = 17)]
    ColumnDataError {
        source: ColumnDataError,
    },

    // Internal Error
    #[snafu(display("{}", source))]
    #[error_code(code = 18)]
    IO {
        source: std::io::Error,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to open file '{}': {}", path.display(), source))]
    #[error_code(code = 19)]
    OpenFile {
        path: PathBuf,
        source: std::io::Error,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Error with read file '{}': {}", path.display(), source))]
    #[error_code(code = 20)]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to write file '{}': {}", path.display(), source))]
    #[error_code(code = 21)]
    WriteFile {
        path: PathBuf,
        source: std::io::Error,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to sync file: {}", source))]
    #[error_code(code = 22)]
    SyncFile {
        source: std::io::Error,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("File {} has wrong name format: {}", file_name, message))]
    #[error_code(code = 23)]
    InvalidFileName {
        file_name: String,
        message: String,
        location: Location,
        backtrace: Backtrace,
    },

    /// WAL file is truncated, it's because CnosDB didn't shutdown properly.
    ///
    /// This error is handled by WAL module:
    /// just stop the current WAL file reading, go to the next WAL file.
    #[snafu(display("Internal handled: WAL truncated"))]
    #[error_code(code = 24)]
    WalTruncated {
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("eof"))]
    #[error_code(code = 25)]
    Eof,

    #[snafu(display("Failed to encode record file block: {}", source))]
    #[error_code(code = 26)]
    RecordFileEncode {
        source: bincode::Error,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Faield to decode record file block: {}", source))]
    #[error_code(code = 27)]
    RecordFileDecode {
        source: bincode::Error,
        location: Location,
        backtrace: Backtrace,
    },

    /// This error is handled by the caller of record_file::Reader::read_record()
    #[snafu(display("Record data at [{pos}..{}] is invalid", pos + * len as u64))]
    #[error_code(code = 28)]
    RecordFileInvalidDataSize {
        pos: u64,
        len: u32,
        location: Location,
        backtrace: Backtrace,
    },

    /// This error is handled by the caller of record_file::Reader::read_record()
    #[snafu(display(
        "Record CRC not match at [{}..{}], expected: {crc}, calculated: {crc_calculated}",
        record.pos,
        record.pos + record.data.len() as u64 + record_file::RECORD_HEADER_LEN as u64,
    ))]
    #[error_code(code = 29)]
    RecordFileHashCheckFailed {
        crc: u32,
        crc_calculated: u32,
        record: Record,
        location: Location,
        backtrace: Backtrace,
    },

    /// This error is handled by the caller of tsm_file::page
    #[snafu(display("TSM Page CRC not match , expected: {crc}, calculated: {crc_calculated}",))]
    #[error_code(code = 30)]
    TsmPageFileHashCheckFailed {
        crc: u32,
        crc_calculated: u32,
        page: Page,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to do encode: {}", source))]
    #[error_code(code = 31)]
    Encode {
        source: Box<dyn std::error::Error + Send + Sync>,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to do decode: {}", source))]
    #[error_code(code = 32)]
    Decode {
        source: Box<dyn std::error::Error + Send + Sync>,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Index: {}", source))]
    #[error_code(code = 33)]
    IndexErr {
        source: IndexError,
    },

    #[snafu(display("Invalid parameter : {}", reason))]
    #[error_code(code = 34)]
    InvalidParam {
        reason: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("file has no footer"))]
    #[error_code(code = 35)]
    NoFooter,

    #[snafu(display("invalid points : '{}'", source))]
    #[error_code(code = 36)]
    Points {
        source: PointsError,
    },

    #[snafu(display("non-UTF-8 string '{message}': {source}"))]
    #[error_code(code = 37)]
    InvalidUtf8 {
        message: String,
        source: std::str::Utf8Error,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Vnode {vnode_id} not found"))]
    #[error_code(code = 38)]
    VnodeNotFound {
        vnode_id: VnodeId,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Table {table} not found"))]
    #[error_code(code = 39)]
    TableNotFound {
        table: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[error_code(code = 40)]
    #[snafu(display("Column {} not found", column))]
    ColumnNotFound {
        column: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[error_code(code = 41)]
    #[snafu(display("{}", message))]
    Tombstone {
        message: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[error_code(code = 42)]
    #[snafu(display("{}", reason))]
    RecordFileIO {
        reason: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Table name can't be empty"))]
    #[error_code(code = 53)]
    InvalidPointTable {
        location: Location,
        backtrace: Backtrace,
    },

    #[error_code(code = 54)]
    #[snafu(display("ColumnId {} not found", column))]
    ColumnIdNotFound {
        column: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[error_code(code = 55)]
    #[snafu(display("Columns of FlatBufferTable is missing"))]
    FlatBufColumnsMiss {
        location: Location,
        backtrace: Backtrace,
    },

    // Internal Error
    #[error_code(code = 56)]
    #[snafu(display("{}", source))]
    Serialize {
        source: Box<dyn std::error::Error + Send + Sync>,
        location: Location,
        backtrace: Backtrace,
    },

    // Internal Error
    #[error_code(code = 57)]
    #[snafu(display("{}", source))]
    Deserialize {
        source: Box<dyn std::error::Error + Send + Sync>,
        location: Location,
        backtrace: Backtrace,
    },

    #[error_code(code = 58)]
    #[snafu(display("file system error: {}", source))]
    FileSystemError {
        source: FileSystemError,
    },

    #[snafu(display("ModelError: {}", source))]
    #[error_code(code = 89)]
    ModelError {
        source: ModelError,
    },
}

impl From<PointsError> for TskvError {
    fn from(value: PointsError) -> Self {
        TskvError::Points { source: value }
    }
}

impl From<SchemaError> for TskvError {
    fn from(value: SchemaError) -> Self {
        match value {
            SchemaError::Meta { source } => Self::Meta { source },
            other => TskvError::Schema { source: other },
        }
    }
}

impl From<DataFusionError> for TskvError {
    fn from(source: DataFusionError) -> Self {
        match source {
            DataFusionError::External(e) if e.downcast_ref::<MetaError>().is_some() => {
                MetaSnafu.into_error(*e.downcast::<MetaError>().unwrap())
            }
            DataFusionError::External(e) if e.downcast_ref::<Self>().is_some() => {
                match *e.downcast::<Self>().unwrap() {
                    Self::DatafusionError { source, .. } => Self::from(source),
                    e => e,
                }
            }

            DataFusionError::External(e) if e.downcast_ref::<ArrowError>().is_some() => {
                let arrow_error = *e.downcast::<ArrowError>().unwrap();
                ArrowSnafu.into_error(arrow_error)
            }

            DataFusionError::External(e) if e.downcast_ref::<DataFusionError>().is_some() => {
                let datafusion_error = *e.downcast::<DataFusionError>().unwrap();
                Self::from(datafusion_error)
            }

            DataFusionError::ArrowError(e) => ArrowSnafu.into_error(e),
            v => DatafusionSnafu.into_error(v),
        }
    }
}

impl TskvError {
    pub fn error_code(&self) -> &dyn ErrorCode {
        match self {
            TskvError::Meta { source } => source.error_code(),
            TskvError::ErrorResponse { error, .. } => error,
            _ => self,
        }
    }

    pub fn vnode_broken_code(code: &str) -> bool {
        const VNODE_BROKEN_CODES: [&str; 5] = [
            TskvError::code_read_tsm(),
            TskvError::code_record_file_hash_check_failed(),
            TskvError::code_tsm_page_file_hash_check_failed(),
            TskvError::code_open_file(),
            TskvError::code_decode(),
        ];
        for error_code in VNODE_BROKEN_CODES {
            if error_code == code {
                return true;
            }
        }
        false
    }

    pub fn is_file_not_found_error(&self) -> bool {
        match self {
            Self::OpenFile { source, .. } => source.kind() == std::io::ErrorKind::NotFound,
            _ => false,
        }
    }
}

// default conversion from CoordinatorError to tonic treats everything
// other than `Status` as an internal error
impl From<TskvError> for Status {
    fn from(value: TskvError) -> Self {
        let error_resp = ErrorResponse::new(value.error_code());

        match serde_json::to_string(&error_resp) {
            Ok(err) => Status::internal(err),
            Err(err) => {
                let error_str = format!("Serialize TskvError, error: {}", err);
                trace::error!(error_str);
                Status::unknown(error_str)
            }
        }
    }
}

impl From<Status> for TskvError {
    fn from(source: Status) -> Self {
        let error_message = source.message();
        match source.code() {
            Code::Internal => {
                match serde_json::from_str::<ErrorResponse>(error_message) {
                    Ok(error) => ErrorResponseSnafu { error }.build(),
                    Err(err) => {
                        // Deserialization tskv error failed, maybe a bug
                        CommonSnafu {
                            reason: err.to_string(),
                        }
                        .build()
                    }
                }
            }
            Code::Unknown => {
                // The server will return this exception if serialization of tskv error fails, maybe a bug
                trace::error!("The server will return this exception if serialization of tskv error fails, maybe a bug");
                CommonSnafu {
                    reason: error_message.to_string(),
                }
                .build()
            }
            _ => NetworkSnafu.into_error(source),
        }
    }
}
