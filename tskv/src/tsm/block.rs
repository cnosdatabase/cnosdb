use std::cmp::min;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use minivec::MiniVec;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::{Timestamp, ValueType};
use trace::error;

use crate::memcache::DataType;
use crate::tsm::codec::{
    get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec, get_ts_codec,
    get_u64_codec, DataBlockEncoding,
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
        enc: DataBlockEncoding,
    },
    I64 {
        ts: Vec<i64>,
        val: Vec<i64>,
        enc: DataBlockEncoding,
    },
    Str {
        ts: Vec<i64>,
        val: Vec<MiniVec<u8>>,
        enc: DataBlockEncoding,
    },
    F64 {
        ts: Vec<i64>,
        val: Vec<f64>,
        enc: DataBlockEncoding,
    },
    Bool {
        ts: Vec<i64>,
        val: Vec<bool>,
        enc: DataBlockEncoding,
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
                    ts.eq(ts_other) && val.eq(val_other)
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
                    ts.eq(ts_other) && val.eq(val_other)
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
                    ts.eq(ts_other) && val.eq(val_other)
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
                    ts.eq(ts_other) && val.eq(val_other)
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
                    ts.eq(ts_other) && val.eq(val_other)
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
                enc: DataBlockEncoding::default(),
            },
            ValueType::Integer => Self::I64 {
                ts: Vec::with_capacity(size),
                val: Vec::with_capacity(size),
                enc: DataBlockEncoding::default(),
            },
            ValueType::Float => Self::F64 {
                ts: Vec::with_capacity(size),
                val: Vec::with_capacity(size),
                enc: DataBlockEncoding::default(),
            },
            ValueType::String => Self::Str {
                ts: Vec::with_capacity(size),
                val: Vec::with_capacity(size),
                enc: DataBlockEncoding::default(),
            },
            ValueType::Boolean => Self::Bool {
                ts: Vec::with_capacity(size),
                val: Vec::with_capacity(size),
                enc: DataBlockEncoding::default(),
            },
            ValueType::Unknown => {
                todo!()
            }
        }
    }

    /// Inserts new timestamp and value wrapped by `DataType` to this `DataBlock`.
    pub fn insert(&mut self, data: DataType) {
        match data {
            DataType::Bool(ts_in, val_in) => {
                if let Self::Bool { ts, val, .. } = self {
                    ts.push(ts_in);
                    val.push(val_in);
                }
            }
            DataType::U64(ts_in, val_in) => {
                if let Self::U64 { ts, val, .. } = self {
                    ts.push(ts_in);
                    val.push(val_in);
                }
            }
            DataType::I64(ts_in, val_in) => {
                if let Self::I64 { ts, val, .. } = self {
                    ts.push(ts_in);
                    val.push(val_in);
                }
            }
            DataType::Str(ts_in, val_in) => {
                if let Self::Str { ts, val, .. } = self {
                    ts.push(ts_in);
                    val.push(val_in);
                }
            }
            DataType::F64(ts_in, val_in) => {
                if let Self::F64 { ts, val, .. } = self {
                    ts.push(ts_in);
                    val.push(val_in);
                }
            }
            DataType::StrRef(ts_in, val_in) => {
                if let Self::Str { ts, val, .. } = self {
                    ts.push(ts_in);
                    val.push(MiniVec::from(val_in.as_slice()))
                }
            }
        }
    }

    pub fn clear(&mut self) {
        match self {
            DataBlock::U64 { ts, val, .. } => {
                ts.clear();
                val.clear();
            }
            DataBlock::I64 { ts, val, .. } => {
                ts.clear();
                val.clear();
            }
            DataBlock::Str { ts, val, .. } => {
                ts.clear();
                val.clear();
            }
            DataBlock::F64 { ts, val, .. } => {
                ts.clear();
                val.clear();
            }
            DataBlock::Bool { ts, val, .. } => {
                ts.clear();
                val.clear();
            }
        }
    }

    pub fn time_range(&self) -> Option<(Timestamp, Timestamp)> {
        if self.is_empty() {
            return None;
        }
        let end = self.len();
        match self {
            DataBlock::U64 { ts, .. } => Some((ts[0], ts[end - 1])),
            DataBlock::I64 { ts, .. } => Some((ts[0], ts[end - 1])),
            DataBlock::Str { ts, .. } => Some((ts[0], ts[end - 1])),
            DataBlock::F64 { ts, .. } => Some((ts[0], ts[end - 1])),
            DataBlock::Bool { ts, .. } => Some((ts[0], ts[end - 1])),
        }
    }

    /// Returns (`timestamp[start]`, `timestamp[end]`) from this `DataBlock` at the specified
    /// indexes.
    pub fn time_range_by_range(&self, start: usize, end: usize) -> (Timestamp, Timestamp) {
        match self {
            DataBlock::U64 { ts, .. } => (ts[start], ts[end - 1]),
            DataBlock::I64 { ts, .. } => (ts[start], ts[end - 1]),
            DataBlock::Str { ts, .. } => (ts[start], ts[end - 1]),
            DataBlock::F64 { ts, .. } => (ts[start], ts[end - 1]),
            DataBlock::Bool { ts, .. } => (ts[start], ts[end - 1]),
        }
    }

    /// Inserts new timestamps and values wrapped by `&[DataType]` to this `DataBlock`.
    pub fn batch_insert(&mut self, cells: &[DataType]) {
        for iter in cells.iter() {
            self.insert(iter.clone());
        }
    }

    /// Returns the encodings for timestamps and values of this `DataBlock`.
    pub fn encodings(&self) -> DataBlockEncoding {
        match &self {
            Self::U64 { enc, .. } => *enc,
            Self::I64 { enc, .. } => *enc,
            Self::F64 { enc, .. } => *enc,
            Self::Str { enc, .. } => *enc,
            Self::Bool { enc, .. } => *enc,
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

    pub fn set_encoding(&mut self, encoding: DataBlockEncoding) {
        match self {
            DataBlock::U64 { enc, .. } => {
                *enc = encoding;
            }
            DataBlock::I64 { enc, .. } => {
                *enc = encoding;
            }
            DataBlock::Str { enc, .. } => {
                *enc = encoding;
            }
            DataBlock::F64 { enc, .. } => {
                *enc = encoding;
            }
            DataBlock::Bool { enc, .. } => {
                *enc = encoding;
            }
        }
    }

    pub fn merge(&self, other: Self) -> Self {
        let (mut i, mut j) = (0_usize, 0_usize);
        let len_1 = self.len();
        let len_2 = other.len();
        let ts_1 = self.ts();
        let ts_2 = other.ts();
        let mut blk = Self::new(self.len() + other.len(), self.field_type());
        while i < len_1 && j < len_2 {
            match ts_1[i].cmp(&ts_2[j]) {
                std::cmp::Ordering::Less => {
                    if let Some(t) = self.get(i) {
                        blk.insert(t);
                    }
                    i += 1;
                }
                std::cmp::Ordering::Equal => {
                    if let Some(t) = self.get(i) {
                        blk.insert(t);
                    }
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Greater => {
                    if let Some(t) = other.get(j) {
                        blk.insert(t);
                    }
                    j += 1;
                }
            }
        }
        if i < len_1 {
            for i1 in i..len_1 {
                if let Some(t) = self.get(i1) {
                    blk.insert(t);
                }
            }
        } else if j < len_2 {
            for j1 in j..len_2 {
                if let Some(t) = other.get(j1) {
                    blk.insert(t);
                }
            }
        }

        blk
    }

    /// Split data block at index `i`, returns (block[..i]. block[i..])
    pub fn split_at(self, i: usize) -> (DataBlock, DataBlock) {
        match &self {
            DataBlock::U64 { ts, val, .. } => {
                if i >= ts.len() {
                    (self, DataBlock::new(0, ValueType::Unsigned))
                } else {
                    (
                        DataBlock::U64 {
                            ts: ts[..i].to_vec(),
                            val: val[..i].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                        DataBlock::U64 {
                            ts: ts[i..].to_vec(),
                            val: val[i..].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                    )
                }
            }
            DataBlock::I64 { ts, val, .. } => {
                if i >= ts.len() {
                    (self, DataBlock::new(0, ValueType::Integer))
                } else {
                    (
                        DataBlock::I64 {
                            ts: ts[..i].to_vec(),
                            val: val[..i].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                        DataBlock::I64 {
                            ts: ts[i..].to_vec(),
                            val: val[i..].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                    )
                }
            }
            DataBlock::Str { ts, val, .. } => {
                if i >= ts.len() {
                    (self, DataBlock::new(0, ValueType::String))
                } else {
                    (
                        DataBlock::Str {
                            ts: ts[..i].to_vec(),
                            val: val[..i].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                        DataBlock::Str {
                            ts: ts[i..].to_vec(),
                            val: val[i..].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                    )
                }
            }
            DataBlock::F64 { ts, val, .. } => {
                if i >= ts.len() {
                    (self, DataBlock::new(0, ValueType::Float))
                } else {
                    (
                        DataBlock::F64 {
                            ts: ts[..i].to_vec(),
                            val: val[..i].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                        DataBlock::F64 {
                            ts: ts[i..].to_vec(),
                            val: val[i..].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                    )
                }
            }
            DataBlock::Bool { ts, val, .. } => {
                if i >= ts.len() {
                    (self, DataBlock::new(0, ValueType::Boolean))
                } else {
                    (
                        DataBlock::Bool {
                            ts: ts[..i].to_vec(),
                            val: val[..i].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                        DataBlock::Bool {
                            ts: ts[i..].to_vec(),
                            val: val[i..].to_vec(),
                            enc: DataBlockEncoding::default(),
                        },
                    )
                }
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
                        blk.insert(it);
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

    /// Extract `DataBlock`s to `DataType`s,
    /// returns the minimum timestamp in a series of `DataBlock`s
    fn next_min(
        blocks: &mut [Self],
        dst: &mut [Option<DataType>],
        offsets: &mut [usize],
    ) -> Option<Timestamp> {
        let mut min_ts = None;
        for (i, (block, dst)) in blocks.iter_mut().zip(dst).enumerate() {
            if dst.is_none() {
                *dst = block.get(offsets[i]);
                offsets[i] += 1;
            }

            if let Some(pair) = dst {
                min_ts = min_ts
                    .map(|ts| min(pair.timestamp(), ts))
                    .or_else(|| Some(pair.timestamp()));
            };
        }
        min_ts
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

    /// Remove (ts, val) in this `DataBlock` where ts is greater equal than min_ts
    /// and ts is less equal than the max_ts
    pub fn exclude(&mut self, time_range: &TimeRange) {
        if self.is_empty() {
            return;
        }
        if let Some((min_idx, max_idx)) = self.index_range(time_range) {
            self.exclude_by_index(min_idx, max_idx + 1);
        }
    }

    pub fn index_range(&self, time_range: &TimeRange) -> Option<(usize, usize)> {
        if self.is_empty() {
            return None;
        }
        let TimeRange { min_ts, max_ts } = *time_range;

        /// Returns possible position of ts in sli,
        /// and if ts is not found, and position is at the bounds of sli, return (pos, false).
        fn binary_search(sli: &[i64], ts: &i64) -> (usize, bool) {
            match sli.binary_search(ts) {
                Ok(i) => (i, true),
                Err(i) => (i, false),
            }
        }

        let ts_sli = self.ts();
        let (mut min_idx, has_min) = binary_search(ts_sli, &min_ts);
        let (mut max_idx, has_max) = binary_search(ts_sli, &max_ts);
        // If ts_sli doesn't contain supported time range then return.
        if min_idx > max_idx
            || max_idx == 0 && !has_max
            || min_idx == ts_sli.len()
            || max_idx == min_idx && !has_max && !has_min
        {
            return None;
        }
        if !has_max {
            max_idx -= 1;
        }
        min_idx = min(min_idx, ts_sli.len() - 1);
        max_idx = min(max_idx, ts_sli.len() - 1);
        Some((min_idx, max_idx))
    }

    #[rustfmt::skip]
    pub fn extend(&mut self, mut data_block: DataBlock) {
        match (self, &mut data_block) {
            (DataBlock::U64 { ts: ta, val: va, .. }, DataBlock::U64 { ts: tb, val: vb, .. }) => {
                ta.append(tb);
                va.append(vb);
            }
            (DataBlock::I64 { ts: ta, val: va, .. }, DataBlock::I64 { ts: tb, val: vb, .. }) => {
                ta.append(tb);
                va.append(vb);
            }
            (DataBlock::Str { ts: ta, val: va, .. }, DataBlock::Str { ts: tb, val: vb, .. }) => {
                ta.append(tb);
                va.append(vb);
            }
            (DataBlock::F64 { ts: ta, val: va, .. }, DataBlock::F64 { ts: tb, val: vb, .. }) => {
                ta.append(tb);
                va.append(vb);
            }
            (DataBlock::Bool { ts: ta, val: va, .. }, DataBlock::Bool { ts: tb, val: vb, .. }) => {
                ta.append(tb);
                va.append(vb);
            }
            _ => {}
        }
    }

    /// Returns a subset of `DataBlock` with timestamps included in the specified time range.
    pub fn intersection(self, time_range: &TimeRange) -> Option<DataBlock> {
        self.index_range(time_range).map(|(min, max)| {
            if min == 0 && max >= self.len() {
                self
            } else {
                match self {
                    DataBlock::U64 { ts, val, enc } => DataBlock::U64 {
                        ts: ts[min..=max].to_vec(),
                        val: val[min..=max].to_vec(),
                        enc,
                    },
                    DataBlock::I64 { ts, val, enc } => DataBlock::I64 {
                        ts: ts[min..=max].to_vec(),
                        val: val[min..=max].to_vec(),
                        enc,
                    },
                    DataBlock::Str { ts, val, enc } => DataBlock::Str {
                        ts: ts[min..=max].to_vec(),
                        val: val[min..=max].to_vec(),
                        enc,
                    },
                    DataBlock::F64 { ts, val, enc } => DataBlock::F64 {
                        ts: ts[min..=max].to_vec(),
                        val: val[min..=max].to_vec(),
                        enc,
                    },
                    DataBlock::Bool { ts, val, enc } => DataBlock::Bool {
                        ts: ts[min..=max].to_vec(),
                        val: val[min..=max].to_vec(),
                        enc,
                    },
                }
            }
        })
    }

    /// Encodes timestamps and values of this `DataBlock` to bytes.
    pub fn encode(
        &self,
        start: usize,
        end: usize,
        encodings: DataBlockEncoding,
    ) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
        let (ts_enc, val_enc) = encodings.split();
        let ts_codec = get_ts_codec(ts_enc);

        let mut ts_buf = vec![];
        let mut data_buf = vec![];
        match self {
            DataBlock::Bool { ts, val, .. } => {
                ts_codec.encode(&ts[start..end], &mut ts_buf)?;
                let val_codec = get_bool_codec(val_enc);
                val_codec.encode(&val[start..end], &mut data_buf)?
            }
            DataBlock::U64 { ts, val, .. } => {
                ts_codec.encode(&ts[start..end], &mut ts_buf)?;
                let val_codec = get_u64_codec(val_enc);
                val_codec.encode(&val[start..end], &mut data_buf)?
            }
            DataBlock::I64 { ts, val, .. } => {
                ts_codec.encode(&ts[start..end], &mut ts_buf)?;
                let val_codec = get_i64_codec(val_enc);
                val_codec.encode(&val[start..end], &mut data_buf)?
            }
            DataBlock::Str { ts, val, .. } => {
                ts_codec.encode(&ts[start..end], &mut ts_buf)?;
                let strs: Vec<&[u8]> = val.iter().map(|str| &str[..]).collect();
                let val_codec = get_str_codec(val_enc);
                val_codec.encode(&strs[start..end], &mut data_buf)?
            }
            DataBlock::F64 { ts, val, .. } => {
                ts_codec.encode(&ts[start..end], &mut ts_buf)?;
                let val_codec = get_f64_codec(val_enc);
                val_codec.encode(&val[start..end], &mut data_buf)?
            }
        }
        Ok((ts_buf, data_buf))
    }

    pub fn decode(
        ts: &[u8],
        val: &[u8],
        count: u32,
        field_type: ValueType,
    ) -> Result<DataBlock, Box<dyn Error + Send + Sync>> {
        let mut decoded_ts = Vec::with_capacity(count as usize);
        let ts_encoding = get_encoding(ts);
        let ts_codec = get_ts_codec(ts_encoding);
        ts_codec.decode(ts, &mut decoded_ts)?;

        match field_type {
            ValueType::Float => {
                // values will be same length as time-stamps.
                let mut decoded_val = Vec::with_capacity(count as usize);
                let val_encoding = get_encoding(val);
                let val_codec = get_f64_codec(val_encoding);
                val_codec.decode(val, &mut decoded_val)?;
                Ok(DataBlock::F64 {
                    ts: decoded_ts,
                    val: decoded_val,
                    enc: DataBlockEncoding::new(ts_encoding, val_encoding),
                })
            }
            ValueType::Integer => {
                // values will be same length as time-stamps.
                let mut decoded_val = Vec::with_capacity(count as usize);
                let val_encoding = get_encoding(val);
                let val_codec = get_i64_codec(val_encoding);
                val_codec.decode(val, &mut decoded_val)?;
                Ok(DataBlock::I64 {
                    ts: decoded_ts,
                    val: decoded_val,
                    enc: DataBlockEncoding::new(ts_encoding, val_encoding),
                })
            }
            ValueType::Boolean => {
                // values will be same length as time-stamps.
                let mut decoded_val = Vec::with_capacity(count as usize);
                let val_encoding = get_encoding(val);
                let val_codec = get_bool_codec(val_encoding);
                val_codec.decode(val, &mut decoded_val)?;
                Ok(DataBlock::Bool {
                    ts: decoded_ts,
                    val: decoded_val,
                    enc: DataBlockEncoding::new(ts_encoding, val_encoding),
                })
            }
            ValueType::String => {
                // values will be same length as time-stamps.
                let mut decoded_val = Vec::with_capacity(count as usize);
                let val_encoding = get_encoding(val);
                let val_codec = get_str_codec(val_encoding);
                val_codec.decode(val, &mut decoded_val)?;
                Ok(DataBlock::Str {
                    ts: decoded_ts,
                    val: decoded_val,
                    enc: DataBlockEncoding::new(ts_encoding, val_encoding),
                })
            }
            ValueType::Unsigned => {
                // values will be same length as time-stamps.
                let mut decoded_val = Vec::with_capacity(count as usize);
                let val_encoding = get_encoding(val);
                let val_codec = get_u64_codec(val_encoding);
                val_codec.decode(val, &mut decoded_val)?;
                Ok(DataBlock::U64 {
                    ts: decoded_ts,
                    val: decoded_val,
                    enc: DataBlockEncoding::new(ts_encoding, val_encoding),
                })
            }
            _ => Err(format!(
                "cannot decode block {:?} with no unknown value type",
                field_type
            )
            .into()),
        }
    }
}

impl Display for DataBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataBlock::U64 { ts, .. } => {
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
            DataBlock::I64 { ts, .. } => {
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
            DataBlock::Str { ts, .. } => {
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
            DataBlock::F64 { ts, .. } => {
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
            DataBlock::Bool { ts, .. } => {
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

fn exclude_slow(v: &mut Vec<MiniVec<u8>>, min_idx: usize, max_idx: usize) {
    if min_idx == max_idx {
        v.remove(min_idx);
    }
    let len = v.len() + min_idx - max_idx;
    for i in min_idx..len {
        v[i] = v[max_idx - min_idx + i].clone();
    }
    v.truncate(len);
}

pub struct DataBlockReader {
    data_block: DataBlock,
    idx: usize,
    end_idx: usize,
    intersected_time_ranges: TimeRanges,
    intersected_time_ranges_i: usize,
}

impl Debug for DataBlockReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataBlockReader")
            .field("data_block_range", &self.data_block.time_range())
            .field("idx", &self.idx)
            .field("end_idx", &self.end_idx)
            .field("intersected_time_ranges", &self.intersected_time_ranges)
            .field("intersected_time_ranges_i", &self.intersected_time_ranges_i)
            .finish()
    }
}

impl Default for DataBlockReader {
    fn default() -> Self {
        Self {
            data_block: DataBlock::Bool {
                ts: vec![],
                val: vec![],
                enc: Default::default(),
            },
            idx: 1,
            end_idx: 0,
            intersected_time_ranges: TimeRanges::empty(),
            intersected_time_ranges_i: 0,
        }
    }
}

impl DataBlockReader {
    pub fn new_uninit(value_type: ValueType) -> Self {
        let data_block = DataBlock::new(0, value_type);
        Self {
            data_block,
            idx: 1,
            end_idx: 0,
            intersected_time_ranges: TimeRanges::empty(),
            intersected_time_ranges_i: 0,
        }
    }

    pub fn new(data_block: DataBlock, time_ranges: TimeRanges) -> Self {
        let mut res = Self {
            data_block,
            idx: 1,
            end_idx: 0,
            intersected_time_ranges: time_ranges,
            intersected_time_ranges_i: 0,
        };
        if res.set_index_from_time_ranges() {
            res
        } else {
            res.idx = usize::MAX;
            res
        }
    }

    /// Iterates the ramaining TimeRange in `intersected_time_ranges`, if there are no remaning TimeRange's.
    /// then return false.
    ///
    /// If there are overlaped time range of DataBlock and TimeRanges, set iteration range of `data_block`
    /// and return true, otherwise set the iteration range a zero-length range `[1, 0]` and return false.
    ///
    fn set_index_from_time_ranges(&mut self) -> bool {
        if self.intersected_time_ranges.is_empty()
            || self.intersected_time_ranges_i >= self.intersected_time_ranges.len()
        {
            false
        } else {
            let tr_idx_start = self.intersected_time_ranges_i;
            for tr in self
                .intersected_time_ranges
                .time_ranges()
                .skip(tr_idx_start)
            {
                self.intersected_time_ranges_i += 1;
                // Check if the DataBlock matches one of the intersected time ranges.
                // TODO: sometimes the comparison in loop can stop earily.
                if let Some((min, max)) = self.data_block.index_range(&tr) {
                    self.idx = min;
                    self.end_idx = max;
                    return true;
                }
            }
            false
        }
    }
    pub fn has_next(&mut self) -> bool {
        if self.idx > self.end_idx {
            self.set_index_from_time_ranges();
        }
        self.idx < self.data_block.len()
    }
}

impl Iterator for DataBlockReader {
    type Item = DataType;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx > self.end_idx && !self.set_index_from_time_ranges() {
            return None;
        }
        let res = self.data_block.get(self.idx);
        self.idx += 1;
        res
    }
}

#[derive(Debug, Clone)]
pub struct EncodedDataBlock {
    pub ts: Vec<u8>,
    pub val: Vec<u8>,
    pub enc: DataBlockEncoding,
    pub count: u32,
    pub field_type: ValueType,
    pub time_range: Option<TimeRange>,
}

impl PartialEq for EncodedDataBlock {
    fn eq(&self, other: &Self) -> bool {
        self.field_type == other.field_type
            && self.enc == other.enc
            && self.ts == other.ts
            && self.val == other.val
    }
}

impl Display for EncodedDataBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time_range = self.time_range.unwrap_or(TimeRange::none());
        write!(
            f,
            "{} {{ len: {}, min_ts: {}, max_ts: {} }}",
            self.field_type, self.count, time_range.min_ts, time_range.max_ts
        )
    }
}

impl EncodedDataBlock {
    pub fn encode(
        data_block: &DataBlock,
        start: usize,
        end: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let ts_sli = data_block.ts();
        let min_ts = ts_sli[start];
        let max_ts = ts_sli[end - 1];
        let (ts, val) = data_block.encode(start, end, data_block.encodings())?;
        Ok(Self {
            ts,
            val,
            enc: data_block.encodings(),
            count: (end - start) as u32,
            field_type: data_block.field_type(),
            time_range: Some(TimeRange::new(min_ts, max_ts)),
        })
    }

    pub fn decode(&self) -> Result<DataBlock, Box<dyn std::error::Error + Send + Sync>> {
        DataBlock::decode(&self.ts, &self.val, self.count, self.field_type)
    }
}

#[cfg(test)]
pub mod test {
    use minivec::{mini_vec, MiniVec};
    use models::predicate::domain::{TimeRange, TimeRanges};
    use models::ValueType;

    use crate::memcache::DataType;
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::{DataBlock, DataBlockReader, EncodedDataBlock};

    pub(crate) fn check_data_block(block: &DataBlock, pattern: &[DataType]) {
        assert_eq!(block.len(), pattern.len());

        for (j, item) in pattern.iter().enumerate().take(block.len()) {
            assert_eq!(&block.get(j).unwrap(), item);
        }
    }

    #[test]
    fn test_merge_blocks() {
        #[rustfmt::skip]
        let res = DataBlock::merge_blocks(
            vec![
                DataBlock::U64 { ts: vec![1, 2, 3, 4, 5], val: vec![10, 20, 30, 40, 50], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![2, 3, 4], val: vec![12, 13, 15], enc: DataBlockEncoding::default() },
            ],
            0,
        );

        #[rustfmt::skip]
        assert_eq!(res, vec![
            DataBlock::U64 { ts: vec![1, 2, 3, 4, 5], val: vec![10, 12, 13, 15, 50], enc: DataBlockEncoding::default() },
        ]);
    }

    #[test]
    fn test_data_block_split_at() {
        let cases = vec![
            (1, (1..=5), 0, vec![], vec![1, 2, 3, 4, 5]),
            (2, (1..=5), 1, vec![1], vec![2, 3, 4, 5]),
            (3, (1..=5), 2, vec![1, 2], vec![3, 4, 5]),
            (4, (1..=5), 4, vec![1, 2, 3, 4], vec![5]),
            (5, (1..=5), 5, vec![1, 2, 3, 4, 5], vec![]),
        ];
        for (i, ts_range, split_idx, left, right) in cases {
            {
                let u64_blk = DataBlock::U64 {
                    ts: ts_range.clone().collect(),
                    val: ts_range.clone().map(|t| t as u64).collect(),
                    enc: DataBlockEncoding::default(),
                };
                let (a, b) = u64_blk.split_at(split_idx);
                assert_eq!(
                    a,
                    DataBlock::U64 {
                        ts: left.clone(),
                        val: left.iter().map(|d| *d as u64).collect(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, u64, the left is different",
                );
                assert_eq!(
                    b,
                    DataBlock::U64 {
                        ts: right.clone(),
                        val: right.iter().map(|d| *d as u64).collect(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, u64, the right is different",
                );
            }
            {
                let i64_blk = DataBlock::I64 {
                    ts: ts_range.clone().collect(),
                    val: ts_range.clone().collect(),
                    enc: DataBlockEncoding::default(),
                };
                let (a, b) = i64_blk.split_at(split_idx);
                assert_eq!(
                    a,
                    DataBlock::I64 {
                        ts: left.clone(),
                        val: left.clone(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, i64, the left is different",
                );
                assert_eq!(
                    b,
                    DataBlock::I64 {
                        ts: right.clone(),
                        val: right.clone(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, i64, the right is different",
                );
            }
            {
                let str_blk = DataBlock::Str {
                    ts: ts_range.clone().collect(),
                    val: ts_range
                        .clone()
                        .map(|d| MiniVec::from(d.to_string().as_bytes()))
                        .collect(),
                    enc: DataBlockEncoding::default(),
                };
                let (a, b) = str_blk.split_at(split_idx);
                assert_eq!(
                    a,
                    DataBlock::Str {
                        ts: left.clone(),
                        val: left
                            .iter()
                            .map(|d| MiniVec::from(d.to_string().as_bytes()))
                            .collect(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, str, the left is different",
                );
                assert_eq!(
                    b,
                    DataBlock::Str {
                        ts: right.clone(),
                        val: right
                            .iter()
                            .map(|d| MiniVec::from(d.to_string().as_bytes()))
                            .collect(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, str, the right is different",
                );
            }
            {
                let f64_blk = DataBlock::F64 {
                    ts: ts_range.clone().collect(),
                    val: ts_range.clone().map(|t| t as f64).collect(),
                    enc: DataBlockEncoding::default(),
                };
                let (a, b) = f64_blk.split_at(split_idx);
                assert_eq!(
                    a,
                    DataBlock::F64 {
                        ts: left.clone(),
                        val: left.iter().map(|t| *t as f64).collect(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, f64, the left is different",
                );
                assert_eq!(
                    b,
                    DataBlock::F64 {
                        ts: right.clone(),
                        val: right.iter().map(|t| *t as f64).collect(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, f64, the right is different",
                );
            }
            {
                let bool_blk = DataBlock::Bool {
                    ts: ts_range.clone().collect(),
                    val: ts_range.clone().map(|t| t % 2 == 0).collect(),
                    enc: DataBlockEncoding::default(),
                };
                let (a, b) = bool_blk.split_at(split_idx);
                assert_eq!(
                    a,
                    DataBlock::Bool {
                        ts: left.clone(),
                        val: left.iter().map(|t| *t % 2 == 0).collect(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, bool, the left is different",
                );
                assert_eq!(
                    b,
                    DataBlock::Bool {
                        ts: right.clone(),
                        val: right.iter().map(|t| *t % 2 == 0).collect(),
                        enc: DataBlockEncoding::default()
                    },
                    "for case: {i}, bool, the right is different",
                );
            }
        }
    }

    #[test]
    fn test_data_block_exclude_1() {
        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            val: vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
            enc: DataBlockEncoding::default(),
        };
        blk.exclude(&TimeRange::from((2, 3)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 4, 5, 6, 7, 8, 9],
                val: vec![10, 11, 14, 15, 16, 17, 18, 19],
                enc: DataBlockEncoding::default(),
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            val: vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
            enc: DataBlockEncoding::default(),
        };
        blk.exclude(&TimeRange::from((2, 8)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 9],
                val: vec![10, 11, 19],
                enc: DataBlockEncoding::default(),
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::Str {
            ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            val: vec![mini_vec![10], mini_vec![11], mini_vec![12], mini_vec![13], mini_vec![14],
                      mini_vec![15], mini_vec![16], mini_vec![17], mini_vec![18], mini_vec![19]],
            enc: DataBlockEncoding::default(),
        };
        blk.exclude(&TimeRange::from((2, 3)));
        assert_eq!(
            blk,
            DataBlock::Str {
                ts: vec![0, 1, 4, 5, 6, 7, 8, 9],
                val: vec![
                    mini_vec![10],
                    mini_vec![11],
                    mini_vec![14],
                    mini_vec![15],
                    mini_vec![16],
                    mini_vec![17],
                    mini_vec![18],
                    mini_vec![19],
                ],
                enc: DataBlockEncoding::default(),
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::Str {
            ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            val: vec![mini_vec![10], mini_vec![11], mini_vec![12], mini_vec![13], mini_vec![14],
                      mini_vec![15], mini_vec![16], mini_vec![17], mini_vec![18], mini_vec![19]],
            enc: DataBlockEncoding::default(),
        };
        blk.exclude(&TimeRange::from((2, 8)));
        assert_eq!(
            blk,
            DataBlock::Str {
                ts: vec![0, 1, 9],
                val: vec![mini_vec![10], mini_vec![11], mini_vec![19]],
                enc: DataBlockEncoding::default(),
            }
        );
    }

    #[test]
    fn test_data_block_exclude_2() {
        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3], val: vec![10, 11, 12, 13],
            enc: DataBlockEncoding::default()
        };
        blk.exclude(&TimeRange::from((-2, 0)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![1, 2, 3],
                val: vec![11, 12, 13],
                enc: DataBlockEncoding::default()
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3], val: vec![10, 11, 12, 13],
            enc: DataBlockEncoding::default()
        };
        blk.exclude(&TimeRange::from((3, 5)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 2],
                val: vec![10, 11, 12],
                enc: DataBlockEncoding::default(),
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3], val: vec![10, 11, 12, 13],
            enc: DataBlockEncoding::default()
        };
        blk.exclude(&TimeRange::from((-3, -1)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 2, 3],
                val: vec![10, 11, 12, 13],
                enc: DataBlockEncoding::default(),
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3], val: vec![10, 11, 12, 13],
            enc: DataBlockEncoding::default()
        };
        blk.exclude(&TimeRange::from((5, 7)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 2, 3],
                val: vec![10, 11, 12, 13],
                enc: DataBlockEncoding::default(),
            }
        );

        #[rustfmt::skip]
        let mut blk = DataBlock::U64 {
            ts: vec![0, 1, 2, 3, 7, 8, 9, 10], val: vec![10, 11, 12, 13, 17, 18, 19, 20],
            enc: DataBlockEncoding::default()
        };
        blk.exclude(&TimeRange::from((5, 6)));
        assert_eq!(
            blk,
            DataBlock::U64 {
                ts: vec![0, 1, 2, 3, 7, 8, 9, 10],
                val: vec![10, 11, 12, 13, 17, 18, 19, 20],
                enc: DataBlockEncoding::default(),
            }
        );
    }

    #[test]
    fn test_data_block_extend() {
        {
            let mut block_1 = DataBlock::U64 {
                ts: vec![0, 1, 2],
                val: vec![10, 11, 12],
                enc: DataBlockEncoding::default(),
            };
            let block_2 = DataBlock::U64 {
                ts: vec![3, 4, 5],
                val: vec![13, 14, 15],
                enc: DataBlockEncoding::default(),
            };
            block_1.extend(block_2);
            assert_eq!(
                block_1,
                DataBlock::U64 {
                    ts: vec![0, 1, 2, 3, 4, 5],
                    val: vec![10, 11, 12, 13, 14, 15],
                    enc: DataBlockEncoding::default(),
                }
            );
        }
        {
            let mut block_1 = DataBlock::I64 {
                ts: vec![0, 1, 2],
                val: vec![10, 11, 12],
                enc: DataBlockEncoding::default(),
            };
            let block_2 = DataBlock::I64 {
                ts: vec![3, 4, 5],
                val: vec![13, 14, 15],
                enc: DataBlockEncoding::default(),
            };
            block_1.extend(block_2);
            assert_eq!(
                block_1,
                DataBlock::I64 {
                    ts: vec![0, 1, 2, 3, 4, 5],
                    val: vec![10, 11, 12, 13, 14, 15],
                    enc: DataBlockEncoding::default(),
                }
            );
        }
        {
            let mut block_1 = DataBlock::Str {
                ts: vec![0, 1, 2],
                val: vec![
                    MiniVec::from("10".as_bytes()),
                    MiniVec::from("11".as_bytes()),
                    MiniVec::from("12".as_bytes()),
                ],
                enc: DataBlockEncoding::default(),
            };
            let block_2 = DataBlock::Str {
                ts: vec![3, 4, 5],
                val: vec![
                    MiniVec::from("13".as_bytes()),
                    MiniVec::from("14".as_bytes()),
                    MiniVec::from("15".as_bytes()),
                ],
                enc: DataBlockEncoding::default(),
            };
            block_1.extend(block_2);
            assert_eq!(
                block_1,
                DataBlock::Str {
                    ts: vec![0, 1, 2, 3, 4, 5],
                    val: vec![
                        MiniVec::from("10".as_bytes()),
                        MiniVec::from("11".as_bytes()),
                        MiniVec::from("12".as_bytes()),
                        MiniVec::from("13".as_bytes()),
                        MiniVec::from("14".as_bytes()),
                        MiniVec::from("15".as_bytes()),
                    ],
                    enc: DataBlockEncoding::default(),
                }
            );
        }
        {
            let mut block_1 = DataBlock::F64 {
                ts: vec![0, 1, 2],
                val: vec![10.0, 11.0, 12.0],
                enc: DataBlockEncoding::default(),
            };
            let block_2 = DataBlock::F64 {
                ts: vec![3, 4, 5],
                val: vec![13.0, 14.0, 15.0],
                enc: DataBlockEncoding::default(),
            };
            block_1.extend(block_2);
            assert_eq!(
                block_1,
                DataBlock::F64 {
                    ts: vec![0, 1, 2, 3, 4, 5],
                    val: vec![10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
                    enc: DataBlockEncoding::default(),
                }
            );
        }
        {
            let mut block_1 = DataBlock::Bool {
                ts: vec![0, 1, 2],
                val: vec![true, true, true],
                enc: DataBlockEncoding::default(),
            };
            let block_2 = DataBlock::Bool {
                ts: vec![3, 4, 5],
                val: vec![false, false, false],
                enc: DataBlockEncoding::default(),
            };
            block_1.extend(block_2);
            assert_eq!(
                block_1,
                DataBlock::Bool {
                    ts: vec![0, 1, 2, 3, 4, 5],
                    val: vec![true, true, true, false, false, false],
                    enc: DataBlockEncoding::default(),
                }
            );
        }
        {
            // Test extend with different value type.
            let mut block_1 = DataBlock::U64 {
                ts: vec![0, 1, 2],
                val: vec![10, 11, 12],
                enc: DataBlockEncoding::default(),
            };
            let block_2 = DataBlock::Bool {
                ts: vec![3, 4, 5],
                val: vec![false, false, false],
                enc: DataBlockEncoding::default(),
            };
            block_1.extend(block_2);
            assert_eq!(
                block_1,
                DataBlock::U64 {
                    ts: vec![0, 1, 2],
                    val: vec![10, 11, 12],
                    enc: DataBlockEncoding::default(),
                }
            );
        }
    }

    #[test]
    fn test_data_block_intersection() {
        let cases = vec![
            (1, (1..=5), (1, 5), Some(1..=5)),
            (2, (1..=5), (0, 6), Some(1..=5)),
            (3, (1..=5), (0, 1), Some(1..=1)),
            (4, (1..=5), (1, 2), Some(1..=2)),
            (5, (1..=5), (-1, 0), None),
            (6, (1..=5), (4, 5), Some(4..=5)),
            (7, (1..=5), (5, 6), Some(5..=5)),
            (8, (1..=5), (6, 7), None),
        ];
        for (i, block_time_range, out_time_range, expect_block_time_range) in cases {
            {
                let u64_blk = DataBlock::U64 {
                    ts: block_time_range.clone().collect(),
                    val: block_time_range.clone().map(|t| t as u64).collect(),
                    enc: DataBlockEncoding::default(),
                };
                let intersection_ret = u64_blk.intersection(&out_time_range.into());
                match &expect_block_time_range {
                    Some(exp_time_range) => {
                        assert!(
                            intersection_ret.is_some(),
                            "for case: {i}, u64, the result is not a Some(_)",
                        );
                        let blk_ret = intersection_ret.unwrap();
                        assert!(
                            blk_ret.time_range().is_some(),
                            "for case: {i}, u64, the result's time_range is not a Some(_)",
                        );
                        let (min_ts, max_ts) = blk_ret.time_range().unwrap();
                        assert_eq!(
                            &(min_ts..=max_ts),
                            exp_time_range,
                            "for case: {i}, u64, the result's time_range is not as expected",
                        );
                    }
                    None => assert!(intersection_ret.is_none()),
                }
            }
            {
                let i64_blk = DataBlock::I64 {
                    ts: block_time_range.clone().collect(),
                    val: block_time_range.clone().collect(),
                    enc: DataBlockEncoding::default(),
                };
                let intersection_ret = i64_blk.intersection(&out_time_range.into());
                match &expect_block_time_range {
                    Some(exp_time_range) => {
                        assert!(
                            intersection_ret.is_some(),
                            "for case: {i}, i64, the result is not a Some(_)",
                        );
                        let blk_ret = intersection_ret.unwrap();
                        assert!(
                            blk_ret.time_range().is_some(),
                            "for case: {i}, i64, the result's time_range is not a Some(_)",
                        );
                        let (min_ts, max_ts) = blk_ret.time_range().unwrap();
                        assert_eq!(
                            &(min_ts..=max_ts),
                            exp_time_range,
                            "for case: {i}, i64, the result's time_range is not as expected",
                        );
                    }
                    None => assert!(intersection_ret.is_none()),
                }
            }
            {
                let str_blk = DataBlock::Str {
                    ts: block_time_range.clone().collect(),
                    val: block_time_range
                        .clone()
                        .map(|d| MiniVec::from(d.to_string().as_bytes()))
                        .collect(),
                    enc: DataBlockEncoding::default(),
                };
                let intersection_ret = str_blk.intersection(&out_time_range.into());
                match &expect_block_time_range {
                    Some(exp_time_range) => {
                        assert!(
                            intersection_ret.is_some(),
                            "for case: {i}, str, the result is not a Some(_)",
                        );
                        let blk_ret = intersection_ret.unwrap();
                        assert!(
                            blk_ret.time_range().is_some(),
                            "for case: {i}, str, the result's time_range is not a Some(_)",
                        );
                        let (min_ts, max_ts) = blk_ret.time_range().unwrap();
                        assert_eq!(
                            &(min_ts..=max_ts),
                            exp_time_range,
                            "for case: {i}, str, the result's time_range is not as expected",
                        );
                    }
                    None => assert!(intersection_ret.is_none()),
                }
            }
            {
                let f64_blk = DataBlock::F64 {
                    ts: block_time_range.clone().collect(),
                    val: block_time_range.clone().map(|t| t as f64).collect(),
                    enc: DataBlockEncoding::default(),
                };
                let intersection_ret = f64_blk.intersection(&out_time_range.into());
                match &expect_block_time_range {
                    Some(exp_time_range) => {
                        assert!(
                            intersection_ret.is_some(),
                            "for case: {i}, f64, the result is not a Some(_)",
                        );
                        let blk_ret = intersection_ret.unwrap();
                        assert!(
                            blk_ret.time_range().is_some(),
                            "for case: {i}, f64, the result's time_range is not a Some(_)",
                        );
                        let (min_ts, max_ts) = blk_ret.time_range().unwrap();
                        assert_eq!(
                            &(min_ts..=max_ts),
                            exp_time_range,
                            "for case: {i}, f64, the result's time_range is not as expected",
                        );
                    }
                    None => assert!(intersection_ret.is_none()),
                }
            }
            {
                let bool_blk = DataBlock::Bool {
                    ts: block_time_range.clone().collect(),
                    val: block_time_range.clone().map(|t| t % 2 == 0).collect(),
                    enc: DataBlockEncoding::default(),
                };
                let intersection_ret = bool_blk.intersection(&out_time_range.into());
                match &expect_block_time_range {
                    Some(exp_time_range) => {
                        assert!(
                            intersection_ret.is_some(),
                            "for case: {i}, bool, the result is not a Some(_)",
                        );
                        let blk_ret = intersection_ret.unwrap();
                        assert!(
                            blk_ret.time_range().is_some(),
                            "for case: {i}, bool, the result's time_range is not a Some(_)",
                        );
                        let (min_ts, max_ts) = blk_ret.time_range().unwrap();
                        assert_eq!(
                            &(min_ts..=max_ts),
                            exp_time_range,
                            "for case: {i}, bool, the result's time_range is not as expected",
                        );
                    }
                    None => assert!(intersection_ret.is_none()),
                }
            }
        }
    }

    #[test]
    fn test_data_block_reader() {
        {
            let mut blk_reader = DataBlockReader::new_uninit(ValueType::Float);
            assert_eq!(blk_reader.next(), None);
        }
        {
            let data_block = DataBlock::U64 {
                ts: vec![0, 1, 2, 3],
                val: vec![10, 11, 12, 13],
                enc: DataBlockEncoding::default(),
            };
            let time_ranges = TimeRanges::all();
            let mut blk_reader = DataBlockReader::new(data_block, time_ranges);
            assert_eq!(blk_reader.next(), Some(DataType::U64(0, 10)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(1, 11)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(2, 12)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(3, 13)));
            assert_eq!(blk_reader.next(), None);
        }
        {
            let data_block = DataBlock::U64 {
                ts: vec![0, 1, 2, 3],
                val: vec![10, 11, 12, 13],
                enc: DataBlockEncoding::default(),
            };
            let time_ranges = TimeRanges::empty();
            let mut blk_reader = DataBlockReader::new(data_block, time_ranges);
            assert_eq!(blk_reader.next(), None);
        }
        {
            let data_block = DataBlock::U64 {
                ts: vec![0, 1, 2, 10, 11, 12, 100, 101, 102, 1000, 1001, 1002],
                val: vec![0, 3, 6, 30, 33, 36, 300, 303, 306, 3000, 3003, 3006],
                enc: DataBlockEncoding::default(),
            };
            let time_ranges = TimeRanges::new(vec![
                (-3, -1).into(),
                (0, 0).into(),
                (1, 1).into(),
                (2, 2).into(),
                (10, 12).into(),
                (99, 100).into(),
                (102, 103).into(),
                (999, 1003).into(),
            ]);
            let mut blk_reader = DataBlockReader::new(data_block, time_ranges);
            assert_eq!(blk_reader.next(), Some(DataType::U64(0, 0)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(1, 3)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(2, 6)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(10, 30)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(11, 33)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(12, 36)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(100, 300)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(102, 306)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(1000, 3000)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(1001, 3003)));
            assert_eq!(blk_reader.next(), Some(DataType::U64(1002, 3006)));
            assert_eq!(blk_reader.next(), None);
        }
    }

    #[test]
    fn test_encoded_data_block() {
        let blk = DataBlock::U64 {
            ts: vec![1, 3, 5, 7, 9],
            val: vec![10, 30, 50, 70, 90],
            enc: DataBlockEncoding::default(),
        };

        {
            let blk_enc = EncodedDataBlock::encode(&blk, 0, blk.len()).unwrap();
            let blk_dec = blk_enc.decode().unwrap();
            assert_eq!(blk, blk_dec);
        }
        {
            let blk = blk.clone();
            let blk_enc = EncodedDataBlock::encode(&blk, 0, 1).unwrap();
            let blk_dec = blk_enc.decode().unwrap();
            let (blk_exp, _) = blk.split_at(1);
            assert_eq!(blk_exp, blk_dec);
        }
        {
            let blk_enc = EncodedDataBlock::encode(&blk, 1, 2).unwrap();
            let blk_dec = blk_enc.decode().unwrap();
            let (_, blk_exp) = blk.split_at(1);
            let (blk_exp, _) = blk_exp.split_at(1);
            assert_eq!(blk_exp, blk_dec);
        }
    }
}
