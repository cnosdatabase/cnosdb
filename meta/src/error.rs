use crate::{client, ClusterNodeId};
use openraft::{AnyError, ErrorSubject, ErrorVerb, StorageError, StorageIOError};
use snafu::Snafu;
use std::error::Error;
use std::io;

pub type StorageIOResult<T> = Result<T, StorageIOError<ClusterNodeId>>;
pub type StorageResult<T> = Result<T, StorageError<ClusterNodeId>>;
pub type MetaResult<T> = Result<T, MetaError>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum MetaError {
    #[snafu(display("The member {} of tenant {} already exists", member_name, tenant_name))]
    MemberAlreadyExists {
        member_name: String,
        tenant_name: String,
    },

    #[snafu(display("The member {} of tenant {} not found", member_name, tenant_name))]
    MemberNotFound {
        member_name: String,
        tenant_name: String,
    },

    #[snafu(display("The privilege {} already exists", name))]
    PrivilegeAlreadyExists { name: String },

    #[snafu(display("The privilege {} not found", name))]
    PrivilegeNotFound { name: String },

    #[snafu(display("The role {} already exists", role))]
    RoleAlreadyExists { role: String },

    #[snafu(display("The role {} not found", role))]
    RoleNotFound { role: String },

    #[snafu(display("The user {} already exists", user))]
    UserAlreadyExists { user: String },

    #[snafu(display("The user {} not found", user))]
    UserNotFound { user: String },

    #[snafu(display("The tenant {} already exists", tenant))]
    TenantAlreadyExists { tenant: String },

    #[snafu(display("The tenant {} not found", tenant))]
    TenantNotFound { tenant: String },

    #[snafu(display("Not Found Field"))]
    NotFoundField,

    #[snafu(display("index storage error: {}", msg))]
    IndexStroage { msg: String },

    #[snafu(display("Not Found DB: {}", db))]
    NotFoundDb { db: String },

    #[snafu(display("Not Found Data Node: {}", id))]
    NotFoundNode { id: u64 },

    #[snafu(display("Request meta cluster error: {}", msg))]
    MetaClientErr { msg: String },

    #[snafu(display("Error: {}", msg))]
    CommonError { msg: String },

    #[snafu(display("Database not found: {:?}", database))]
    DatabaseNotFound { database: String },

    #[snafu(display("Database {:?} already exists", database))]
    DatabaseAlreadyExists { database: String },

    #[snafu(display("Table not found: {:?}", table))]
    TableNotFound { table: String },

    #[snafu(display("Table {} already exists.", table_name))]
    TableAlreadyExists { table_name: String },

    #[snafu(display("module raft error reason: {}", source))]
    Raft {
        source: StorageIOError<ClusterNodeId>,
    },
    #[snafu(display("module sled error reason: {}", source))]
    SledConflict {
        source: sled::transaction::ConflictableTransactionError<AnyError>,
    },
    #[snafu(display("module raft network error reason: {}", source))]
    RaftConnect { source: tonic::transport::Error },

    #[snafu(display("{} fail: {} reached limit, the maximum is {}", action, name, max))]
    Limit {
        action: String,
        name: String,
        max: String,
    }, // RaftRPC{
       //     source: RPCError<ClusterNodeId, ClusterNode, Err>
       // }
}

pub fn sm_r_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        AnyError::new(&e),
    )
}
pub fn sm_w_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        AnyError::new(&e),
    )
}
pub fn s_r_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e))
}
pub fn s_w_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e))
}
pub fn v_r_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Read, AnyError::new(&e))
}
pub fn v_w_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Write, AnyError::new(&e))
}
pub fn l_r_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e))
}
pub fn l_w_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e))
}

pub fn ct_err<E: Error + 'static>(e: E) -> MetaError {
    MetaError::SledConflict {
        source: sled::transaction::ConflictableTransactionError::Abort(AnyError::new(&e)),
    }
}

impl From<StorageIOError<ClusterNodeId>> for MetaError {
    fn from(err: StorageIOError<ClusterNodeId>) -> Self {
        MetaError::Raft { source: err }
    }
}

impl From<io::Error> for MetaError {
    fn from(err: io::Error) -> Self {
        MetaError::CommonError {
            msg: err.to_string(),
        }
    }
}

impl From<client::WriteError> for MetaError {
    fn from(err: client::WriteError) -> Self {
        MetaError::MetaClientErr {
            msg: err.to_string(),
        }
    }
}
