use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use meta::error::MetaError;
use models::error_code::{ErrorCode, ErrorCoder};
use snafu::Snafu;
use std::{fmt::Debug, io};

#[derive(Snafu, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "05")]
pub enum CoordinatorError {
    TskvError {
        source: tskv::Error,
    },

    Meta {
        source: MetaError,
    },

    ArrowError {
        source: ArrowError,
    },

    #[snafu(display("Meta request error: {}", msg))]
    #[error_code(code = 1)]
    MetaRequest {
        msg: String,
    },

    #[snafu(display("Io error: {}", msg))]
    #[error_code(code = 2)]
    IOErrors {
        msg: String,
    },

    #[snafu(display("Invalid serde message: {}", err))]
    #[error_code(code = 3)]
    InvalidSerdeMsg {
        err: String,
    },

    #[snafu(display("Fails to send to channel: {}", msg))]
    #[error_code(code = 4)]
    ChannelSend {
        msg: String,
    },

    #[snafu(display("Fails to recv from channel: {}", msg))]
    #[error_code(code = 5)]
    ChannelRecv {
        msg: String,
    },

    #[snafu(display("Write vnode error: {}", msg))]
    #[error_code(code = 6)]
    WriteVnode {
        msg: String,
    },

    #[snafu(display("Error from models: {}", source))]
    #[error_code(code = 7)]
    ModelsError {
        source: models::Error,
    },

    #[snafu(display("Not found tenant: {}", name))]
    #[error_code(code = 9)]
    TenantNotFound {
        name: String,
    },

    #[snafu(display("Invalid flatbuffers: {}", source))]
    #[error_code(code = 10)]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("Unknow coordinator command: {}", cmd))]
    #[error_code(code = 11)]
    UnKnownCoordCmd {
        cmd: u32,
    },

    #[snafu(display("Coordinator command parse failed"))]
    #[error_code(code = 12)]
    CoordCommandParseErr,

    #[snafu(display("Unexpect response message"))]
    #[error_code(code = 13)]
    UnExpectResponse,

    #[snafu(display("{}", msg))]
    #[error_code(code = 14)]
    CommonError {
        msg: String,
    },

    #[snafu(display("Vnode not found: {}", id))]
    #[error_code(code = 15)]
    VnodeNotFound {
        id: u32,
    },
}

impl From<meta::error::MetaError> for CoordinatorError {
    fn from(err: meta::error::MetaError) -> Self {
        CoordinatorError::MetaRequest {
            msg: err.to_string(),
        }
    }
}

impl From<io::Error> for CoordinatorError {
    fn from(err: io::Error) -> Self {
        CoordinatorError::IOErrors {
            msg: err.to_string(),
        }
    }
}

impl From<tskv::Error> for CoordinatorError {
    fn from(err: tskv::Error) -> Self {
        match err {
            tskv::Error::Meta { source } => CoordinatorError::Meta { source },

            other => CoordinatorError::TskvError { source: other },
        }
    }
}

impl From<ArrowError> for CoordinatorError {
    fn from(err: ArrowError) -> Self {
        match err {
            ArrowError::ExternalError(e) if e.downcast_ref::<CoordinatorError>().is_some() => {
                *e.downcast::<CoordinatorError>().unwrap()
            }
            ArrowError::ExternalError(e) if e.downcast_ref::<MetaError>().is_some() => {
                CoordinatorError::Meta {
                    source: *e.downcast::<MetaError>().unwrap(),
                }
            }
            ArrowError::ExternalError(e) if e.downcast_ref::<tskv::Error>().is_some() => {
                CoordinatorError::TskvError {
                    source: *e.downcast::<tskv::Error>().unwrap(),
                }
            }
            ArrowError::ExternalError(e) if e.downcast_ref::<ArrowError>().is_some() => {
                let arrow_error = *e.downcast::<ArrowError>().unwrap();
                arrow_error.into()
            }

            other => CoordinatorError::ArrowError { source: other },
        }
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for CoordinatorError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        CoordinatorError::ChannelSend {
            msg: err.to_string(),
        }
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for CoordinatorError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        CoordinatorError::ChannelRecv {
            msg: err.to_string(),
        }
    }
}

impl From<models::Error> for CoordinatorError {
    fn from(err: models::Error) -> Self {
        CoordinatorError::ModelsError { source: err }
    }
}

impl CoordinatorError {
    pub fn error_code(&self) -> &dyn ErrorCode {
        match self {
            CoordinatorError::Meta { source } => source.error_code(),
            CoordinatorError::TskvError { source } => source.error_code(),
            _ => self,
        }
    }
}

pub type CoordinatorResult<T> = Result<T, CoordinatorError>;

#[test]
fn test_mod_code() {
    let e = CoordinatorError::UnExpectResponse;
    assert!(e.code().starts_with("05"));
}
