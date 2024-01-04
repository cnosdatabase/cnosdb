use std::fmt::Display;

use models::auth::user::User;
use models::oid::uuid_u64;
use models::schema::{DEFAULT_CATALOG, DEFAULT_DATABASE, DEFAULT_PRECISION};
use serde::{Deserialize, Serialize};
use trace::{SpanRecorder, SpanRecorderExt};

use crate::query::config::StreamTriggerInterval;
use crate::query::execution::Output;
use crate::query::session::CnosSessionConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryId(u64);

impl QueryId {
    pub fn next_id() -> Self {
        Self(uuid_u64())
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<QueryId> for u64 {
    fn from(val: QueryId) -> Self {
        val.0
    }
}

impl From<u64> for QueryId {
    fn from(u: u64) -> Self {
        QueryId(u)
    }
}

impl TryFrom<Vec<u8>> for QueryId {
    type Error = String;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        if bytes.len() != 8 {
            return Err(format!("Incorrect content: {:?}", &bytes));
        }

        let len_bytes: [u8; 8] = unsafe { bytes[0..8].try_into().unwrap_unchecked() };

        Ok(Self(u64::from_le_bytes(len_bytes)))
    }
}

impl From<QueryId> for Vec<u8> {
    fn from(val: QueryId) -> Self {
        val.0.to_le_bytes().into()
    }
}

impl Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone)]
pub struct Context {
    // todo
    // user info
    // security certification info
    // ...
    user: User,
    tenant: String,
    database: String,
    precision: String,
    chunked: bool,
    session_config: CnosSessionConfig,
}

impl Context {
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn precision(&self) -> &str {
        &self.precision
    }

    pub fn user(&self) -> &User {
        &self.user
    }

    pub fn session_config(&self) -> &CnosSessionConfig {
        &self.session_config
    }
    pub fn chunked(&self) -> bool {
        self.chunked
    }
}

impl SpanRecorderExt for Context {
    fn record(&self, span_recorder: &mut SpanRecorder) {
        if span_recorder.span().is_some() {
            span_recorder.set_metadata("user", self.user().desc().name());
            span_recorder.set_metadata("tenant", self.tenant());
            span_recorder.set_metadata("database", self.database());
            span_recorder.set_metadata("chunked", self.chunked());
        }
    }
}

pub struct ContextBuilder {
    user: User,
    tenant: String,
    database: String,
    precision: String,
    chunked: bool,
    session_config: CnosSessionConfig,
}

impl ContextBuilder {
    pub fn new(user: User) -> Self {
        Self {
            user,
            precision: DEFAULT_PRECISION.to_string(),
            tenant: DEFAULT_CATALOG.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            chunked: Default::default(),
            session_config: Default::default(),
        }
    }

    pub fn with_tenant(mut self, tenant: Option<String>) -> Self {
        if let Some(tenant) = tenant {
            self.tenant = tenant
        }
        self
    }

    pub fn with_database(mut self, database: Option<String>) -> Self {
        if let Some(db) = database {
            self.database = db
        }
        self
    }

    pub fn with_precision(mut self, precision: Option<String>) -> Self {
        if let Some(precision) = precision {
            self.precision = precision
        }
        self
    }

    pub fn with_target_partitions(mut self, target_partitions: Option<usize>) -> Self {
        if let Some(dbtarget_partitions) = target_partitions {
            self.session_config = self
                .session_config
                .with_target_partitions(dbtarget_partitions);
        }
        self
    }

    pub fn with_stream_trigger_interval(mut self, interval: Option<StreamTriggerInterval>) -> Self {
        if let Some(interval) = interval {
            self.session_config = self.session_config.with_stream_trigger_interval(interval);
        }
        self
    }

    pub fn with_chunked(mut self, chunked: Option<bool>) -> Self {
        if let Some(chunked) = chunked {
            self.chunked = chunked;
        }
        self
    }

    pub fn build(self) -> Context {
        Context {
            user: self.user,
            tenant: self.tenant,
            database: self.database,
            precision: self.precision,
            chunked: self.chunked,
            session_config: self.session_config,
        }
    }
}

#[derive(Clone)]
pub struct Query {
    context: Context,
    content: String,
}

impl Query {
    #[inline(always)]
    pub fn new(context: Context, content: String) -> Self {
        Self { context, content }
    }

    pub fn context(&self) -> &Context {
        &self.context
    }

    pub fn content(&self) -> &str {
        self.content.as_str()
    }
}

impl SpanRecorderExt for Query {
    fn record(&self, span_recorder: &mut SpanRecorder) {
        if span_recorder.span().is_some() {
            self.context().record(span_recorder);
        }
    }
}

// #[derive(Clone)]
pub struct QueryHandle {
    id: QueryId,
    query: Query,
    result: Output,
}

impl QueryHandle {
    pub fn new(id: QueryId, query: Query, result: Output) -> Self {
        Self { id, query, result }
    }

    pub fn id(&self) -> QueryId {
        self.id
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn result(self) -> Output {
        self.result
    }
}
