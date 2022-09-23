mod nullable;
pub use nullable::*;
mod bitmap;
pub use bitmap::*;

use std::{fmt::Display, mem::size_of, ops::Index};

use models::{Timestamp, ValueType};
use protos::models::FieldType;
use trace::error;

use crate::{
    compaction::overlaps_tuples,
    memcache::{DataType, FieldVal},
    tseries_family::TimeRange,
    tsm::codec::{
        get_bool_codec, get_f64_codec, get_i64_codec, get_str_codec, get_ts_codec, get_u64_codec,
        Encoding,
    },
};

pub trait ByTimeRange {
    fn time_range(&self) -> Option<TimeRange>;
    fn time_range_by_range(&self, start: usize, end: usize) -> TimeRange;
    fn exclude(&mut self, time_range: &TimeRange);
}

#[derive(Debug, Clone)]
pub enum DataBlock {
    U64 {
        ts: Vec<i64>,
        val: Vec<u64>,
        code_type_id: u8,
    },
    I64 {
        ts: Vec<i64>,
        val: Vec<i64>,
        code_type_id: u8,
    },
    Str {
        ts: Vec<i64>,
        val: Vec<Vec<u8>>,
        code_type_id: u8,
    },
    F64 {
        ts: Vec<i64>,
        val: Vec<f64>,
        code_type_id: u8,
    },
    Bool {
        ts: Vec<i64>,
        val: Vec<bool>,
        code_type_id: u8,
    },
}

impl PartialEq for DataBlock {
    fn eq(&self, other: &Self) -> bool {
        match other {
            DataBlock::U64 {
                ts: ts_other,
                val: val_other,
                ..
            } => {
                if let Self::U64 { ts, val, .. } = self {
                    return ts.eq(ts_other) && val.eq(val_other);
                } else {
                    false
                }
            }
            DataBlock::I64 {
                ts: ts_other,
                val: val_other,
                ..
            } => {
                if let Self::I64 { ts, val, .. } = self {
                    return ts.eq(ts_other) && val.eq(val_other);
                } else {
                    false
                }
            }
            DataBlock::Str {
                ts: ts_other,
                val: val_other,
                ..
            } => {
                if let Self::Str { ts, val, .. } = self {
                    return ts.eq(ts_other) && val.eq(val_other);
                } else {
                    false
                }
            }
            DataBlock::F64 {
                ts: ts_other,
                val: val_other,
                ..
            } => {
                if let Self::F64 { ts, val, .. } = self {
                    return ts.eq(ts_other) && val.eq(val_other);
                } else {
                    false
                }
            }
            DataBlock::Bool {
                ts: ts_other,
                val: val_other,
                ..
            } => {
                if let Self::Bool { ts, val, .. } = self {
                    return ts.eq(ts_other) && val.eq(val_other);
                } else {
                    false
                }
            }
        }
    }
}

impl DataBlock {
    pub fn new(size: usize, field_type: ValueType) -> Self {
        match field_type {
            ValueType::Unsigned => Self::U64 {
                ts: Vec::with_capacity(size),
                val: Vec::with_capacity(size),
                code_type_id: 0,
            },
            ValueType::Integer => Self::I64 {
                ts: Vec::with_capacity(size),
                val: Vec::with_capacity(size),
                code_type_id: 0,
            },
            ValueType::Float => Self::F64 {
                ts: Vec::with_capacity(size),
                val: Vec::with_capacity(size),
                code_type_id: 0,
            },
            ValueType::String => Self::Str {
                ts: Vec::with_capacity(size),
                val: Vec::with_capacity(size),
                code_type_id: 0,
            },
            ValueType::Boolean => Self::Bool {
                ts: Vec::with_capacity(size),
                val: Vec::with_capacity(size),
                code_type_id: 0,
            },
            ValueType::Unknown => {
                todo!()
            }
        }
    }

    /// Inserts new timestamp and value wrapped by `DataType` to this `DataBlock`.
    pub fn insert(&mut self, data: &DataType) {
        match data {
            DataType::Bool(ts_in, val_in) => {
                if let Self::Bool { ts, val, .. } = self {
                    ts.push(*ts_in);
                    val.push(*val_in);
                }
            }
            DataType::U64(ts_in, val_in) => {
                if let Self::U64 { ts, val, .. } = self {
                    ts.push(*ts_in);
                    val.push(*val_in);
                }
            }
            DataType::I64(ts_in, val_in) => {
                if let Self::I64 { ts, val, .. } = self {
                    ts.push(*ts_in);
                    val.push(*val_in);
                }
            }
            DataType::Str(ts_in, val_in) => {
                if let Self::Str { ts, val, .. } = self {
                    ts.push(*ts_in);
                    val.push(val_in.clone());
                }
            }
            DataType::F64(ts_in, val_in) => {
                if let Self::F64 { ts, val, .. } = self {
                    ts.push(*ts_in);
                    val.push(*val_in);
                }
            }
        }
    }

    /// Inserts new timestamps and values wrapped by `&[DataType]` to this `DataBlock`.
    pub fn batch_insert(&mut self, cells: &[DataType]) {
        for iter in cells.iter() {
            self.insert(iter);
        }
    }

    /// Returns the code type id of the data block
    pub fn code_type_id(&self) -> u8 {
        match &self {
            Self::U64 { code_type_id, .. } => *code_type_id,
            Self::I64 { code_type_id, .. } => *code_type_id,
            Self::F64 { code_type_id, .. } => *code_type_id,
            Self::Str { code_type_id, .. } => *code_type_id,
            Self::Bool { code_type_id, .. } => *code_type_id,
        }
    }

    /// Returns the length of the timestamps array of this `DataBlock`.
    pub fn len(&self) -> usize {
        match &self {
            Self::U64 { ts, .. } => ts.len(),
            Self::I64 { ts, .. } => ts.len(),
            Self::F64 { ts, .. } => ts.len(),
            Self::Str { ts, .. } => ts.len(),
            Self::Bool { ts, .. } => ts.len(),
        }
    }

    /// Returns the `ValueType` by this `DataBlock` variant.
    pub fn field_type(&self) -> ValueType {
        match &self {
            DataBlock::U64 { .. } => ValueType::Unsigned,
            DataBlock::I64 { .. } => ValueType::Integer,
            DataBlock::Str { .. } => ValueType::String,
            DataBlock::F64 { .. } => ValueType::Float,
            DataBlock::Bool { .. } => ValueType::Boolean,
        }
    }

    /// Returns a slice containing the entire timestamps of this `DataBlock`.
    pub fn ts(&self) -> &[i64] {
        match self {
            DataBlock::U64 { ts, .. } => ts.as_slice(),
            DataBlock::I64 { ts, .. } => ts.as_slice(),
            DataBlock::Str { ts, .. } => ts.as_slice(),
            DataBlock::F64 { ts, .. } => ts.as_slice(),
            DataBlock::Bool { ts, .. } => ts.as_slice(),
        }
    }

    /// Returns `true` if the `DataBlock` contains no elements(DataBlock::ts::is_empty()).
    pub fn is_empty(&self) -> bool {
        match &self {
            DataBlock::U64 { ts, .. } => ts.is_empty(),
            DataBlock::I64 { ts, .. } => ts.is_empty(),
            DataBlock::Str { ts, .. } => ts.is_empty(),
            DataBlock::F64 { ts, .. } => ts.is_empty(),
            DataBlock::Bool { ts, .. } => ts.is_empty(),
        }
    }

    /// Returns the (ts, val) wrapped by `DataType` at the index 'i'
    pub fn get(&self, i: usize) -> Option<DataType> {
        match self {
            DataBlock::U64 { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::U64(ts[i], val[i]))
                }
            }
            DataBlock::I64 { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::I64(ts[i], val[i]))
                }
            }
            DataBlock::Str { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::Str(ts[i], val[i].clone()))
                }
            }
            DataBlock::F64 { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::F64(ts[i], val[i]))
                }
            }
            DataBlock::Bool { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::Bool(ts[i], val[i]))
                }
            }
        }
    }

    /// Set the (ts, val) wrapped by `DataType` at the index 'i'
    pub fn set(&mut self, i: usize, data_type: DataType) {
        match (self, data_type) {
            (DataBlock::U64 { ts, val, .. }, DataType::U64(ts_in, val_in)) => {
                ts[i] = ts_in;
                val[i] = val_in;
            }
            (DataBlock::I64 { ts, val, .. }, DataType::I64(ts_in, val_in)) => {
                ts[i] = ts_in;
                val[i] = val_in;
            }
            (DataBlock::Str { ts, val, .. }, DataType::Str(ts_in, val_in)) => {
                ts[i] = ts_in;
                val[i] = val_in;
            }
            (DataBlock::F64 { ts, val, .. }, DataType::F64(ts_in, val_in)) => {
                ts[i] = ts_in;
                val[i] = val_in;
            }
            (DataBlock::Bool { ts, val, .. }, DataType::Bool(ts_in, val_in)) => {
                ts[i] = ts_in;
                val[i] = val_in;
            }
            _ => {}
        }
    }

    pub fn set_code_type_id(&mut self, new_code_type_id: u8) {
        match self {
            DataBlock::U64 { code_type_id, .. } => {
                *code_type_id = new_code_type_id;
            }
            DataBlock::I64 { code_type_id, .. } => {
                *code_type_id = new_code_type_id;
            }
            DataBlock::Str { code_type_id, .. } => {
                *code_type_id = new_code_type_id;
            }
            DataBlock::F64 { code_type_id, .. } => {
                *code_type_id = new_code_type_id;
            }
            DataBlock::Bool { code_type_id, .. } => {
                *code_type_id = new_code_type_id;
            }
        }
    }

    /// Merges one or many `DataBlock`s into some `DataBlock` with fixed length,
    /// sorted by timestamp, if many (timestamp, value) conflict with the same
    /// timestamp, use the last value.
    pub fn merge_blocks(mut blocks: Vec<Self>, max_block_size: u32) -> Vec<Self> {
        if blocks.is_empty() {
            return vec![];
        }
        if blocks.len() == 1 {
            return vec![blocks.remove(0)];
        }
        let data_blocks = match blocks.first() {
            None => {
                error!("failed to get data block");
                return vec![];
            }
            Some(v) => v,
        };
        let capacity = data_blocks.len();
        let field_type = data_blocks.field_type();

        let mut res = vec![];
        let mut blk = Self::new(capacity, field_type);
        let mut buf = vec![None; blocks.len()];
        let mut offsets = vec![0_usize; blocks.len()];
        loop {
            match Self::next_min(&mut blocks, &mut buf, &mut offsets) {
                Some(min) => {
                    let mut data = None;
                    for item in &mut buf {
                        if let Some(it) = item {
                            if it.timestamp() == min {
                                data = item.take();
                            }
                        }
                    }
                    if let Some(it) = data {
                        blk.insert(&it);
                        if max_block_size != 0 && blk.len() >= max_block_size as usize {
                            res.push(blk);
                            blk = Self::new(capacity, field_type);
                        }
                    }
                }
                None => {
                    if !blk.is_empty() {
                        res.push(blk);
                    }
                    return res;
                }
            }
        }
    }

    /// Remove (ts, val) in this `DatBlock` where index is greater equal than `min`
    /// and less than `max`.
    ///
    /// **Panics** if min or max is out of range of the ts or val in this `DataBlock`.
    fn exclude_by_index(&mut self, min: usize, max: usize) {
        match self {
            DataBlock::U64 { ts, val, .. } => {
                exclude_fast(ts, min, max);
                exclude_fast(val, min, max);
            }
            DataBlock::I64 { ts, val, .. } => {
                exclude_fast(ts, min, max);
                exclude_fast(val, min, max);
            }
            DataBlock::Str { ts, val, .. } => {
                exclude_fast(ts, min, max);
                exclude_slow(val, min, max);
            }
            DataBlock::F64 { ts, val, .. } => {
                exclude_fast(ts, min, max);
                exclude_fast(val, min, max);
            }
            DataBlock::Bool { ts, val, .. } => {
                exclude_fast(ts, min, max);
                exclude_fast(val, min, max);
            }
        }
    }

    /// Extract `DataBlock`s to `DataType`s,
    /// returns the minimum timestamp in a series of `DataBlock`s
    fn next_min(
        blocks: &mut [Self],
        dst: &mut Vec<Option<DataType>>,
        offsets: &mut [usize],
    ) -> Option<i64> {
        let mut min_ts = None;
        for (i, (block, dst)) in blocks.iter_mut().zip(dst).enumerate() {
            if dst.is_none() {
                *dst = block.get(offsets[i]);
                offsets[i] += 1;
            }

            if let Some(pair) = dst {
                match min_ts {
                    Some(min) => {
                        if pair.timestamp() < min {
                            min_ts = Some(pair.timestamp());
                        }
                    }
                    None => min_ts = Some(pair.timestamp()),
                }
            };
        }
        min_ts
    }

    // todo:
    /// Encodes timestamps and values of this `DataBlock` to bytes.
    pub fn encode(
        &self,
        start: usize,
        end: usize,
        ts_compress_algo: Encoding,
        other_compress_algo: Encoding,
    ) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
        let mut ts_buf = vec![];
        let mut data_buf = vec![];
        let ts_coder = get_ts_codec(ts_compress_algo);
        match self {
            DataBlock::Bool { ts, val, .. } => {
                ts_coder.encode(&ts[start..end], &mut ts_buf)?;
                let val_coder = get_bool_codec(other_compress_algo);
                val_coder.encode(&val[start..end], &mut data_buf)?
            }
            DataBlock::U64 { ts, val, .. } => {
                ts_coder.encode(&ts[start..end], &mut ts_buf)?;
                let val_coder = get_u64_codec(other_compress_algo);
                val_coder.encode(&val[start..end], &mut data_buf)?
            }
            DataBlock::I64 { ts, val, .. } => {
                ts_coder.encode(&ts[start..end], &mut ts_buf)?;
                let val_coder = get_i64_codec(other_compress_algo);
                val_coder.encode(&val[start..end], &mut data_buf)?
            }
            DataBlock::Str { ts, val, .. } => {
                ts_coder.encode(&ts[start..end], &mut ts_buf)?;
                let strs: Vec<&[u8]> = val.iter().map(|str| &str[..]).collect();
                let val_coder = get_str_codec(other_compress_algo);
                val_coder.encode(&strs[start..end], &mut data_buf)?;
            }
            DataBlock::F64 { ts, val, .. } => {
                ts_coder.encode(&ts[start..end], &mut ts_buf)?;
                let val_coder = get_f64_codec(other_compress_algo);
                val_coder.encode(&val[start..end], &mut data_buf)?;
            }
        }
        Ok((ts_buf, data_buf))
    }

    pub fn decode() {}
}

impl ByTimeRange for DataBlock {
    fn time_range(&self) -> Option<TimeRange> {
        if self.is_empty() {
            return None;
        }
        let end = self.len();
        match self {
            DataBlock::U64 { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned()).into()),
            DataBlock::I64 { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned()).into()),
            DataBlock::Str { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned()).into()),
            DataBlock::F64 { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned()).into()),
            DataBlock::Bool { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned()).into()),
        }
    }

    /// Returns (`timestamp[start]`, `timestamp[end]`) from this `DataBlock` at the specified
    /// indexes.
    fn time_range_by_range(&self, start: usize, end: usize) -> TimeRange {
        match self {
            DataBlock::U64 { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()).into(),
            DataBlock::I64 { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()).into(),
            DataBlock::Str { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()).into(),
            DataBlock::F64 { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()).into(),
            DataBlock::Bool { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()).into(),
        }
    }

    /// Remove (ts, val) in this `DataBlock` where ts is greater equal than min_ts
    /// and ts is less equal than the max_ts
    fn exclude(&mut self, time_range: &TimeRange) {
        if self.is_empty() {
            return;
        }
        let TimeRange { min_ts, max_ts } = *time_range;
        let ts_sli = self.ts();
        let (min_idx, has_min) = binary_search(ts_sli, &min_ts);
        let (mut max_idx, has_max) = binary_search(ts_sli, &max_ts);
        // If ts_sli doesn't contain supported time range then return.
        if min_idx > max_idx || min_idx == max_idx && !has_min && !has_max {
            return;
        }
        if max_idx < ts_sli.len() {
            max_idx += 1;
        }

        self.exclude_by_index(min_idx, max_idx);
    }
}

impl Display for DataBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataBlock::U64 { ts, val, .. } => {
                if !ts.is_empty() {
                    write!(
                        f,
                        "U64 {{ len: {}, min_ts: {}, max_ts: {} }}",
                        ts.len(),
                        ts.first().unwrap(),
                        ts.last().unwrap()
                    )
                } else {
                    write!(f, "U64 {{ len: {}, min_ts: NONE, max_ts: NONE }}", ts.len())
                }
            }
            DataBlock::I64 { ts, val, .. } => {
                if !ts.is_empty() {
                    write!(
                        f,
                        "I64 {{ len: {}, min_ts: {}, max_ts: {} }}",
                        ts.len(),
                        ts.first().unwrap(),
                        ts.last().unwrap()
                    )
                } else {
                    write!(f, "I64 {{ len: {}, min_ts: NONE, max_ts: NONE }}", ts.len())
                }
            }
            DataBlock::Str { ts, val, .. } => {
                if !ts.is_empty() {
                    write!(
                        f,
                        "Str {{ len: {}, min_ts: {}, max_ts: {} }}",
                        ts.len(),
                        ts.first().unwrap(),
                        ts.last().unwrap()
                    )
                } else {
                    write!(f, "Str {{ len: {}, min_ts: NONE, max_ts: NONE }}", ts.len())
                }
            }
            DataBlock::F64 { ts, val, .. } => {
                if !ts.is_empty() {
                    write!(
                        f,
                        "F64 {{ len: {}, min_ts: {}, max_ts: {} }}",
                        ts.len(),
                        ts.first().unwrap(),
                        ts.last().unwrap()
                    )
                } else {
                    write!(f, "F64 {{ len: {}, min_ts: NONE, max_ts: NONE }}", ts.len())
                }
            }
            DataBlock::Bool { ts, val, .. } => {
                if !ts.is_empty() {
                    write!(
                        f,
                        "Bool {{ len: {}, min_ts: {}, max_ts: {} }}",
                        ts.len(),
                        ts.first().unwrap(),
                        ts.last().unwrap()
                    )
                } else {
                    write!(
                        f,
                        "Bool {{ len: {}, min_ts: NONE, max_ts: NONE }}",
                        ts.len()
                    )
                }
            }
        }
    }
}

impl From<NullableDataBlock> for DataBlock {
    fn from(block: NullableDataBlock) -> Self {
        let mut data_block = DataBlock::new(block.field_values().len(), block.field_type());
        if let Some(bitmap) = block.bitmap() {
            for (i, t) in block.timestamps().iter().enumerate() {
                if bitmap.get(i) {
                    data_block.insert(&block.field_values().data_value(i, *t));
                }
            }
        }
        data_block
    }
}

/// Returns possible position of ts in sli,
/// and if ts is not found, and position is at the bounds of sli, return (pos, false).
fn binary_search(sli: &[i64], val: &i64) -> (usize, bool) {
    match sli.binary_search(val) {
        Ok(i) => (i, true),
        Err(i) => (i, false),
    }
}

fn exclude_fast<T: Sized + Copy>(v: &mut Vec<T>, min_idx: usize, max_idx: usize) {
    if v.is_empty() {
        return;
    }
    if min_idx == max_idx {
        v.remove(min_idx);
        return;
    }
    let a = v.as_mut_ptr();
    // SAFETY: min_idx and max_idx must not out of the bounds of v
    unsafe {
        assert!(min_idx <= v.len());
        assert!(max_idx <= v.len());
        let b = a.add(min_idx);
        let c = a.add(max_idx);
        c.copy_to(b, v.len() - max_idx);
        v.set_len(v.len() + min_idx - max_idx);
    }
}

fn exclude_slow(v: &mut Vec<Vec<u8>>, min_idx: usize, max_idx: usize) {
    if min_idx == max_idx {
        v.remove(min_idx);
    }
    let len = v.len() + min_idx - max_idx;
    for i in min_idx..len {
        v[i] = v[max_idx - min_idx + i].clone();
    }
    v.truncate(len);
}

#[cfg(test)]
pub mod test {
    use models::Timestamp;
    use std::mem::size_of;

    use crate::memcache::FieldVal;
    use crate::{
        memcache::DataType,
        tseries_family::TimeRange,
        tsm::{block::exclude_fast, ByTimeRange, DataBlock, NullableDataBlock},
    };

    pub(crate) fn check_data_block(block: &DataBlock, pattern: &[DataType]) {
        assert_eq!(block.len(), pattern.len());
        for j in 0..block.len() {
            assert_eq!(block.get(j).unwrap(), pattern[j]);
        }
    }

    #[test]
    fn test_merge_blocks() {
        #[rustfmt::skip]
        let res = DataBlock::merge_blocks(
            vec![
                DataBlock::U64 { ts: vec![1, 2, 3, 4, 5], val: vec![10, 20, 30, 40, 50],code_type_id: 0 },
                DataBlock::U64 { ts: vec![2, 3, 4], val: vec![12, 13, 15],code_type_id: 0 },

            ],
            0
        );

        #[rustfmt::skip]
        assert_eq!(res, vec![
            DataBlock::U64 { ts: vec![1, 2, 3, 4, 5], val: vec![10, 12, 13, 15, 50],code_type_id: 0 },
        ]);
    }

    #[test]
    fn test_data_block_exclude_1() {
        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            val: vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
           code_type_id: 0
        };
        blk.exclude(&TimeRange::from((2, 3)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 4, 5, 6, 7, 8, 9],
                val: vec![10, 11, 14, 15, 16, 17, 18, 19],
                code_type_id: 0
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            val: vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
           code_type_id: 0
        };
        blk.exclude(&TimeRange::from((2, 8)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 9],
                val: vec![10, 11, 19],
                code_type_id: 0
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::Str {
            ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            val: vec![vec![10], vec![11], vec![12], vec![13], vec![14], vec![15], vec![16], vec![17], vec![18], vec![19]],
           code_type_id: 0
        };
        blk.exclude(&TimeRange::from((2, 3)));
        assert_eq!(
            blk,
            DataBlock::Str {
                ts: vec![0, 1, 4, 5, 6, 7, 8, 9],
                val: vec![
                    vec![10],
                    vec![11],
                    vec![14],
                    vec![15],
                    vec![16],
                    vec![17],
                    vec![18],
                    vec![19]
                ],
                code_type_id: 0
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::Str {
            ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            val: vec![vec![10], vec![11], vec![12], vec![13], vec![14], vec![15], vec![16], vec![17], vec![18], vec![19]],
           code_type_id: 0
        };
        blk.exclude(&TimeRange::from((2, 8)));
        assert_eq!(
            blk,
            DataBlock::Str {
                ts: vec![0, 1, 9],
                val: vec![vec![10], vec![11], vec![19]],
                code_type_id: 0
            }
        )
    }

    #[test]
    fn test_data_block_exclude_2() {
        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3], val: vec![10, 11, 12, 13],
           code_type_id: 0
        };
        blk.exclude(&TimeRange::from((-2, 0)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![1, 2, 3],
                val: vec![11, 12, 13],
                code_type_id: 0
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3], val: vec![10, 11, 12, 13],
           code_type_id: 0
        };
        blk.exclude(&TimeRange::from((3, 5)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 2],
                val: vec![10, 11, 12],
                code_type_id: 0
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3], val: vec![10, 11, 12, 13],
           code_type_id: 0
        };
        blk.exclude(&TimeRange::from((-3, -1)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 2, 3],
                val: vec![10, 11, 12, 13],
                code_type_id: 0
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3], val: vec![10, 11, 12, 13],
            code_type_id: 0
        };
        blk.exclude(&TimeRange::from((5, 7)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 2, 3],
                val: vec![10, 11, 12, 13],
                code_type_id: 0
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3, 7, 8, 9, 10], val: vec![10, 11, 12, 13, 17, 18, 19, 20],
            code_type_id: 0
        };
        blk.exclude(&TimeRange::from((5, 6)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 2, 3, 7, 8, 9, 10],
                val: vec![10, 11, 12, 13, 17, 18, 19, 20],
                code_type_id: 0
            }
        );
    }
}
