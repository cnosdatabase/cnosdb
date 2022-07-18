use std::{cmp, fmt::Display, io::SeekFrom, sync::Arc};

use models::{FieldId, Timestamp, ValueType};

use super::{BlockMetaIterator, BLOCK_META_SIZE, FOOTER_SIZE, INDEX_META_SIZE};
use crate::{
    byte_utils::{self, decode_be_i64, decode_be_u16, decode_be_u64},
    direct_io::File,
    error::{Error, Result},
};

pub trait IndexT {}

#[derive(Debug, Clone)]
pub struct Index {
    /// In-memory index-block data
    ///
    /// ```text
    /// +-------------+---------+
    /// | field_id    | 8 bytes |
    /// | field_type  | 1 bytes |
    /// | block_count | 2 bytes |
    /// | blocks      | -       |
    /// +-------------+---------+
    /// ```
    data: Vec<u8>,
    /// Sorted FieldId
    field_ids: Vec<FieldId>,
    /// Sorted index-block offsets for each `FieldId` in `data`
    offsets: Vec<u64>,
}

impl Index {
    #[inline(always)]
    pub fn new(data: Vec<u8>, field_ids: Vec<FieldId>, offsets: Vec<u64>) -> Self {
        Self { data, field_ids, offsets }
    }

    #[inline(always)]
    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    #[inline(always)]
    pub fn field_ids(&self) -> &[FieldId] {
        self.field_ids.as_slice()
    }

    #[inline(always)]
    pub fn offsets(&self) -> &[u64] {
        self.offsets.as_slice()
    }
}

pub struct IndexMeta {
    index_ref: Arc<Index>,
    /// Array index in `Index::offsets`
    index_idx: usize,

    field_id: FieldId,
    field_type: ValueType,
    block_count: u16,
}

impl IndexMeta {
    pub fn block_iterator(&self) -> BlockMetaIterator {
        let index_offset = self.index_ref.offsets()[self.index_idx] as usize;
        BlockMetaIterator::new(self.index_ref.clone(),
                               index_offset,
                               self.field_id,
                               self.field_type,
                               self.block_count)
    }

    pub fn block_iterator_opt(&self, min_ts: Timestamp, max_ts: Timestamp) -> BlockMetaIterator {
        let index_offset = self.index_ref.offsets()[self.index_idx] as usize;
        let mut iter = BlockMetaIterator::new(self.index_ref.clone(),
                                              index_offset,
                                              self.field_id,
                                              self.field_type,
                                              self.block_count);
        iter.filter_timerange(min_ts, max_ts);
        iter
    }

    #[inline(always)]
    pub fn field_id(&self) -> FieldId {
        self.field_id
    }

    #[inline(always)]
    pub fn field_type(&self) -> ValueType {
        self.field_type
    }

    #[inline(always)]
    pub fn block_count(&self) -> u16 {
        self.block_count
    }

    #[inline(always)]
    pub fn timerange(&self) -> (Timestamp, Timestamp) {
        if self.block_count == 0 {
            return (Timestamp::MIN, Timestamp::MIN);
        }
        let first_blk_beg = self.index_ref.offsets()[self.index_idx] as usize + INDEX_META_SIZE;
        let min_ts =
            byte_utils::decode_be_i64(&self.index_ref.data[first_blk_beg..first_blk_beg + 8]);
        let last_blk_beg = first_blk_beg + BLOCK_META_SIZE * (self.block_count as usize - 1);
        let max_ts =
            byte_utils::decode_be_i64(&self.index_ref.data[last_blk_beg + 8..last_blk_beg + 16]);
        (min_ts, max_ts)
    }
}

#[derive(Debug, Clone)]
pub struct BlockMeta {
    index_ref: Arc<Index>,
    /// Array index in `Index::data` which current `BlockMeta` starts.
    field_id: FieldId,
    block_offset: usize,
    field_type: ValueType,

    min_ts: Timestamp,
    max_ts: Timestamp,
}

impl PartialEq for BlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.field_id == other.field_id
        && self.block_offset == other.block_offset
        && self.field_type == other.field_type
        && self.min_ts == other.min_ts
        && self.max_ts == other.max_ts
    }
}

impl Eq for BlockMeta {}

impl PartialOrd for BlockMeta {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlockMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.field_id.cmp(&other.field_id) {
            cmp::Ordering::Equal => match self.min_ts.cmp(&other.min_ts) {
                cmp::Ordering::Equal => self.max_ts.cmp(&other.max_ts),
                other => other.reverse(),
            },
            other => other.reverse(),
        }
    }
}

impl BlockMeta {
    fn new(index: Arc<Index>,
           field_id: FieldId,
           field_type: ValueType,
           block_offset: usize)
           -> Self {
        let min_ts = decode_be_i64(&index.data()[block_offset..block_offset + 8]);
        let max_ts = decode_be_i64(&&index.data()[block_offset + 8..block_offset + 16]);
        Self { index_ref: index, field_id, block_offset, field_type, min_ts, max_ts }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.index_ref.data()[self.block_offset..]
    }

    #[inline(always)]
    pub fn field_id(&self) -> FieldId {
        self.field_id
    }

    #[inline(always)]
    pub fn field_type(&self) -> ValueType {
        self.field_type
    }

    #[inline(always)]
    pub fn min_ts(&self) -> Timestamp {
        self.min_ts
    }

    #[inline(always)]
    pub fn max_ts(&self) -> Timestamp {
        self.max_ts
    }

    #[inline(always)]
    pub fn offset(&self) -> u64 {
        decode_be_u64(&self.index_ref.data()[self.block_offset + 16..self.block_offset + 24])
    }

    #[inline(always)]
    pub fn size(&self) -> u64 {
        decode_be_u64(&self.index_ref.data()[self.block_offset + 24..self.block_offset + 32])
    }

    #[inline(always)]
    pub fn val_off(&self) -> u64 {
        decode_be_u64(&self.index_ref.data()[self.block_offset + 32..self.block_offset + 40])
    }
}

impl Display for BlockMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "BlockMeta: {{ field_id: {}, field_type: {:?}, min_ts: {}, max_ts: {}, offset: {}, val_off: {} }}",
               self.field_id,
               self.field_type,
               self.min_ts,
               self.max_ts,
               self.offset(),
               self.val_off())
    }
}

pub(crate) fn get_index_meta_unchecked(index: Arc<Index>, idx: usize) -> IndexMeta {
    let off = index.offsets()[idx] as usize;

    let field_id = decode_be_u64(&index.data()[off..off + 8]);
    let block_type = ValueType::from(index.data()[off + 8]);
    let block_count = decode_be_u16(&index.data()[off + 9..off + 11]);

    IndexMeta { index_ref: index, index_idx: idx, field_id, field_type: block_type, block_count }
}

pub(crate) fn get_data_block_meta_unchecked(index: Arc<Index>,
                                            index_offset: usize,
                                            block_idx: usize,
                                            field_id: FieldId,
                                            field_type: ValueType)
                                            -> BlockMeta {
    let base = index_offset + INDEX_META_SIZE + block_idx * BLOCK_META_SIZE;
    BlockMeta::new(index, field_id, field_type, base)
}
