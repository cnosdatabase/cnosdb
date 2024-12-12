pub use bkdr_hash::BkdrHasher;
pub use bloom_filter::BloomFilter;
pub use dedup::{dedup_front_by, dedup_front_by_key};

pub mod backtrace;
mod bkdr_hash;
mod bloom_filter;
mod dedup;

pub mod byte_utils;

pub mod byte_nums;
pub mod duration;
pub mod id_generator;
pub mod precision;

pub type Timestamp = i64;

#[cfg(unix)]
pub mod pprof_tools;

pub fn to_hex_string(data: &[u8]) -> String {
    format!("{:x?}", data)
}
