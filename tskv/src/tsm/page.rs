use std::mem::size_of;
use std::u64;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow_buffer::buffer::BooleanBuffer;
use arrow_buffer::builder::BooleanBufferBuilder;
use arrow_schema::{DataType, TimeUnit};
use models::column_data_ref::PrimaryColumnDataRef;
use models::schema::tskv_table_schema::{ColumnType, TableColumn};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use super::mutable_column_ref::MutableColumnRef;
use super::statistics::ValueStatistics;
use crate::byte_utils::{decode_be_u32, decode_be_u64};
use crate::error::{
    EncodeSnafu, TskvResult, TsmPageFileHashCheckFailedSnafu, TsmPageSnafu,
    UnsupportedDataTypeSnafu,
};
use crate::tsm::codec::{
    get_bool_codec, get_f64_codec, get_i64_codec, get_str_codec, get_ts_codec, get_u64_codec,
};
use crate::tsm::reader::data_buf_to_arrow_array;

#[derive(Debug)]
pub struct Page {
    /// 4 bits for bitset len
    /// 8 bits for data len
    /// 4 bits for crc32 len
    /// bitset len bits for BitSet
    /// the bits of rest for data
    pub(crate) bytes: bytes::Bytes,
    pub(crate) meta: PageMeta,
}

impl Page {
    pub fn new(bytes: bytes::Bytes, meta: PageMeta) -> Self {
        Self { bytes, meta }
    }

    pub fn bytes(&self) -> &bytes::Bytes {
        &self.bytes
    }

    pub fn meta(&self) -> &PageMeta {
        &self.meta
    }

    pub fn desc(&self) -> &TableColumn {
        &self.meta.column
    }

    pub fn crc_validation(&self) -> TskvResult<Page> {
        let bytes = self.bytes().clone();
        let meta = self.meta().clone();
        let data_crc = decode_be_u32(&bytes[12..16]);
        let mut hasher = crc32fast::Hasher::new();
        let bitset_len = decode_be_u32(&self.bytes[0..4]) as usize;
        hasher.update(&bytes[16 + bitset_len..]);
        let data_crc_calculated = hasher.finalize();
        if data_crc != data_crc_calculated {
            // If crc not match, try to return error.
            return Err(TsmPageFileHashCheckFailedSnafu {
                crc: data_crc,
                crc_calculated: data_crc_calculated,
                page: Page { bytes, meta },
            }
            .build());
        }
        Ok(Page { bytes, meta })
    }

    pub fn null_bitset(&self) -> BooleanBufferBuilder {
        let data_len = decode_be_u64(&self.bytes[4..12]) as usize;
        let bitset_buffer = self.null_bitset_slice();
        let mut builder = BooleanBufferBuilder::new(data_len);
        builder.append_packed_range(0..data_len, bitset_buffer);
        builder
    }

    pub fn null_bitset_slice(&self) -> &[u8] {
        let bitset_len = decode_be_u32(&self.bytes[0..4]) as usize;
        &self.bytes[16..16 + bitset_len]
    }

    pub fn data_buffer(&self) -> &[u8] {
        let bitset_len = decode_be_u32(&self.bytes[0..4]) as usize;
        &self.bytes[16 + bitset_len..]
    }

    pub fn to_arrow_array(&self) -> TskvResult<ArrayRef> {
        data_buf_to_arrow_array(self)
    }

    pub fn arrow_array_to_page(array: ArrayRef, table_column: TableColumn) -> TskvResult<Page> {
        let data_len = array.len() as u64;
        let bit_set_buffer = match array.nulls() {
            None => BooleanBuffer::new_set(data_len as usize).values().to_vec(),
            Some(nulls) => nulls.buffer().to_vec(),
        };
        let mut buf = vec![];
        let statistics = match array.data_type() {
            DataType::Boolean => {
                let column = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        TsmPageSnafu {
                            reason: "Arrow array is not BooleanArray".to_string(),
                        }
                        .build()
                    })?;
                let target_column = column.iter().flatten().collect::<Vec<_>>();
                let encoder = get_bool_codec(table_column.encoding());
                encoder
                    .encode(&target_column, &mut buf)
                    .context(EncodeSnafu)?;
                PageStatistics::Bool(ValueStatistics::new(
                    None,
                    None,
                    None,
                    (array.len() - target_column.len()) as u64,
                ))
            }
            DataType::Int64 => {
                let column = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    TsmPageSnafu {
                        reason: "Arrow array is not Int64Array".to_string(),
                    }
                    .build()
                })?;
                let mut max = i64::MIN;
                let mut min = i64::MAX;
                let mut target_column = Vec::with_capacity(column.len());
                for val in column.iter().flatten() {
                    target_column.push(val);
                    if val > max {
                        max = val;
                    }
                    if val < min {
                        min = val;
                    }
                }
                let encoder = get_i64_codec(table_column.encoding());
                encoder
                    .encode(&target_column, &mut buf)
                    .context(EncodeSnafu)?;
                PageStatistics::I64(ValueStatistics::new(
                    Some(min),
                    Some(max),
                    None,
                    (array.len() - target_column.len()) as u64,
                ))
            }
            DataType::Timestamp(unit, _) => {
                let target_column = match unit {
                    TimeUnit::Second => {
                        let column = array
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .ok_or_else(|| {
                                TsmPageSnafu {
                                    reason: "Arrow array is not Int64Array".to_string(),
                                }
                                .build()
                            })?;
                        column.iter().flatten().collect::<Vec<_>>()
                    }
                    TimeUnit::Millisecond => {
                        let column = array
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .ok_or_else(|| {
                                TsmPageSnafu {
                                    reason: "Arrow array is not Int64Array".to_string(),
                                }
                                .build()
                            })?;
                        column.iter().flatten().collect::<Vec<_>>()
                    }
                    TimeUnit::Microsecond => {
                        let column = array
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .ok_or_else(|| {
                                TsmPageSnafu {
                                    reason: "Arrow array is not Int64Array".to_string(),
                                }
                                .build()
                            })?;
                        column.iter().flatten().collect::<Vec<_>>()
                    }
                    TimeUnit::Nanosecond => {
                        let column = array
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(|| {
                                TsmPageSnafu {
                                    reason: "Arrow array is not Int64Array".to_string(),
                                }
                                .build()
                            })?;
                        column.iter().flatten().collect::<Vec<_>>()
                    }
                };

                let mut max = i64::MIN;
                let mut min = i64::MAX;
                for val in target_column.iter() {
                    if *val > max {
                        max = *val;
                    }
                    if *val < min {
                        min = *val;
                    }
                }
                let encoder = get_ts_codec(table_column.encoding());
                encoder
                    .encode(&target_column, &mut buf)
                    .context(EncodeSnafu)?;
                PageStatistics::I64(ValueStatistics::new(
                    Some(min),
                    Some(max),
                    None,
                    (array.len() - target_column.len()) as u64,
                ))
            }
            DataType::UInt64 => {
                let column = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        TsmPageSnafu {
                            reason: "Arrow array is not UInt64Array".to_string(),
                        }
                        .build()
                    })?;
                let mut target_column = Vec::with_capacity(column.len());
                let mut max = u64::MIN;
                let mut min = u64::MAX;
                for val in column.iter().flatten() {
                    target_column.push(val);
                    if val > max {
                        max = val;
                    }
                    if val < min {
                        min = val;
                    }
                }
                let encoder = get_u64_codec(table_column.encoding());
                encoder
                    .encode(&target_column, &mut buf)
                    .context(EncodeSnafu)?;
                PageStatistics::U64(ValueStatistics::new(
                    Some(min),
                    Some(max),
                    None,
                    (array.len() - target_column.len()) as u64,
                ))
            }
            DataType::Float64 => {
                let column = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        TsmPageSnafu {
                            reason: "Arrow array is not Float64Array".to_string(),
                        }
                        .build()
                    })?;
                let mut target_column = Vec::with_capacity(column.len());
                let mut max = f64::MIN;
                let mut min = f64::MAX;
                for val in column.iter().flatten() {
                    target_column.push(val);
                    if val > max {
                        max = val;
                    }
                    if val < min {
                        min = val;
                    }
                }
                let encoder = get_f64_codec(table_column.encoding());
                encoder
                    .encode(&target_column, &mut buf)
                    .context(EncodeSnafu)?;
                PageStatistics::F64(ValueStatistics::new(
                    Some(min),
                    Some(max),
                    None,
                    (array.len() - target_column.len()) as u64,
                ))
            }
            DataType::Utf8 => {
                let column = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        TsmPageSnafu {
                            reason: "Arrow array is not StringArray".to_string(),
                        }
                        .build()
                    })?;
                let target_column = column
                    .iter()
                    .filter_map(|v| v.map(|v| v.as_bytes()))
                    .collect::<Vec<_>>();
                let max = target_column.iter().max().map(|value| value.to_vec());
                let min = target_column.iter().min().map(|value| value.to_vec());
                let encoder = get_str_codec(table_column.encoding());
                encoder
                    .encode(&target_column, &mut buf)
                    .context(EncodeSnafu)?;
                PageStatistics::Bytes(ValueStatistics::new(
                    min,
                    max,
                    None,
                    (array.len() - target_column.len()) as u64,
                ))
            }
            _ => {
                return Err(UnsupportedDataTypeSnafu {
                    dt: array.data_type().to_string(),
                }
                .build());
            }
        };

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let data_crc = hasher.finalize().to_be_bytes();

        let mut data = Vec::with_capacity(
            size_of::<u32>() + size_of::<u64>() + data_crc.len() + bit_set_buffer.len() + buf.len(),
        );
        data.extend_from_slice(&(bit_set_buffer.len() as u32).to_be_bytes());
        data.extend_from_slice(&data_len.to_be_bytes());
        data.extend_from_slice(&data_crc);
        data.extend_from_slice(&bit_set_buffer);
        data.extend_from_slice(&buf);
        let bytes = bytes::Bytes::from(data);
        let meta = PageMeta {
            num_values: data_len as u32,
            column: table_column,
            statistics,
        };
        Ok(Page { bytes, meta })
    }

    pub fn colref_to_page(column: MutableColumnRef) -> TskvResult<Page> {
        let table_column = column.column_desc;
        let len_bitset = ((column.column_data.valid.len() + 7) >> 3) as u32;
        let column_data_len = column.column_data.valid.len() as u64;

        let mut buffer = vec![];
        let statistics = match column.column_data.primary_data {
            PrimaryColumnDataRef::Bool(values, min, max) => {
                let encoder = get_bool_codec(table_column.encoding());
                encoder.encode(&values, &mut buffer).context(EncodeSnafu)?;
                PageStatistics::Bool(ValueStatistics::new(
                    Some(min),
                    Some(max),
                    None,
                    column_data_len - values.len() as u64,
                ))
            }

            PrimaryColumnDataRef::F64(values, min, max) => {
                let encoder = get_f64_codec(table_column.encoding());
                encoder.encode(&values, &mut buffer).context(EncodeSnafu)?;
                PageStatistics::F64(ValueStatistics::new(
                    Some(min),
                    Some(max),
                    None,
                    column_data_len - values.len() as u64,
                ))
            }

            PrimaryColumnDataRef::I64(values, min, max) => {
                match table_column.column_type {
                    ColumnType::Time(_) => {
                        let encoder = get_ts_codec(table_column.encoding());
                        encoder.encode(&values, &mut buffer).context(EncodeSnafu)?;
                    }
                    _ => {
                        let encoder = get_i64_codec(table_column.encoding());
                        encoder.encode(&values, &mut buffer).context(EncodeSnafu)?;
                    }
                };
                PageStatistics::I64(ValueStatistics::new(
                    Some(min),
                    Some(max),
                    None,
                    column_data_len - values.len() as u64,
                ))
            }

            PrimaryColumnDataRef::U64(values, min, max) => {
                let encoder = get_u64_codec(table_column.encoding());
                encoder.encode(&values, &mut buffer).context(EncodeSnafu)?;
                PageStatistics::U64(ValueStatistics::new(
                    Some(min),
                    Some(max),
                    None,
                    column_data_len - values.len() as u64,
                ))
            }

            PrimaryColumnDataRef::String(values, min, max) => {
                let encoder = get_str_codec(table_column.encoding());
                encoder.encode(&values, &mut buffer).context(EncodeSnafu)?;
                PageStatistics::Bytes(ValueStatistics::new(
                    Some(min.to_vec()),
                    Some(max.to_vec()),
                    None,
                    column_data_len - values.len() as u64,
                ))
            }
        };

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buffer);
        let buf_crc32 = hasher.finalize().to_be_bytes();

        let mut page_data = vec![];
        page_data.extend_from_slice(&len_bitset.to_be_bytes());
        page_data.extend_from_slice(&column_data_len.to_be_bytes());
        page_data.extend_from_slice(&buf_crc32);
        page_data.extend_from_slice(column.column_data.valid.as_slice());
        page_data.extend_from_slice(&buffer);

        Ok(Page {
            bytes: bytes::Bytes::from(page_data),
            meta: PageMeta {
                statistics,
                column: table_column,
                num_values: column_data_len as u32,
            },
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PageMeta {
    pub(crate) num_values: u32,
    pub(crate) column: TableColumn,
    pub(crate) statistics: PageStatistics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PageStatistics {
    Bool(ValueStatistics<bool>),
    F64(ValueStatistics<f64>),
    I64(ValueStatistics<i64>),
    U64(ValueStatistics<u64>),
    Bytes(ValueStatistics<Vec<u8>>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PageWriteSpec {
    pub(crate) offset: u64,
    pub(crate) size: u64,
    pub(crate) meta: PageMeta,
}

impl PageWriteSpec {
    pub fn new(offset: u64, size: u64, meta: PageMeta) -> Self {
        Self { offset, size, meta }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// todo: dont copy meta
    pub fn meta(&self) -> &PageMeta {
        &self.meta
    }
}

#[cfg(test)]
mod test {
    use arrow::datatypes::ToByteSlice;
    use arrow_buffer::BooleanBufferBuilder;
    use models::schema::tskv_table_schema::{ColumnType, TableColumn};
    use models::ValueType;

    use crate::tsm::page::{Page, PageMeta, PageStatistics};
    use crate::tsm::statistics::ValueStatistics;

    fn create_test_page() -> Page {
        let field_column = TableColumn::new(
            1,
            "field1".to_string(),
            ColumnType::Field(ValueType::Integer),
            Default::default(),
        );

        let pagemeta = PageMeta {
            num_values: 1,
            column: field_column,
            statistics: PageStatistics::I64(ValueStatistics::new(Some(1), Some(3), None, 1)),
        };

        let buf = b"hello world".to_byte_slice();
        let data_len = 1_u64;
        let valid = BooleanBufferBuilder::new(0);
        let len_bitset = ((valid.len() + 7) / 8) as u32;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(buf);
        let data_crc = hasher.finalize().to_be_bytes();

        let mut data = vec![];
        data.extend_from_slice(&len_bitset.to_be_bytes());
        data.extend_from_slice(&data_len.to_be_bytes());
        data.extend_from_slice(&data_crc);
        data.extend_from_slice(valid.as_slice());
        data.extend_from_slice(buf);

        let bytes = bytes::Bytes::from(data);
        Page::new(bytes, pagemeta.clone())
    }

    #[test]
    fn test_page_crc_validation() {
        let page = create_test_page();
        let result = page.crc_validation();
        assert!(result.is_ok());
    }
}
