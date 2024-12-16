use std::sync::Arc;

pub use error_code;
pub use errors::{ModelError, ModelResult};
use parking_lot::RwLock;
pub use record_batch::*;
pub use series_info::SeriesKey;
pub use tag::Tag;
pub use value_type::{PhysicalDType, ValueType};

pub mod codec;
pub mod consistency_level;
pub mod errors;
pub mod meta_data;
pub mod node_info;
mod series_info;
pub mod tag;
pub mod utils;
mod value_type;
#[macro_use]
// pub mod error_code;
pub mod arrow_array;
pub mod arrow;
pub mod auth;
pub mod column_data_ref;
pub mod datafusion;
pub mod duration;
pub mod field_value;
pub mod gis;
pub mod object_reference;
pub mod oid;
pub mod predicate;
pub mod record_batch;
pub mod runtime;
pub mod schema;
pub mod snappy;
pub mod sql;

pub type ShardId = u64;
pub type CatalogId = u64;
pub type SchemaVersion = u64;
pub type SeriesId = u32;
pub type TableId = u64;
pub type ColumnId = u32;

pub type TagKey = Vec<u8>;
pub type TagValue = Vec<u8>;

pub type FieldId = u64;
pub type FieldName = Vec<u8>;

pub type Timestamp = i64;

pub type RwLockRef<T> = Arc<RwLock<T>>;
