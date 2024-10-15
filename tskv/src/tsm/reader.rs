use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use models::codec::Encoding;
use models::column_data::PrimaryColumnData;
use models::field_value::FieldVal;
use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::{ColumnType, PhysicalCType, TskvTableSchemaRef};
use models::{PhysicalDType, SeriesId};
use snafu::{location, Backtrace, GenerateImplicitData, Location, OptionExt, ResultExt};
use utils::bitset::{BitSet, NullBitset};

use crate::error::{
    ArrowSnafu, CommonSnafu, DecodeSnafu, ReadTsmSnafu, TskvResult, TsmPageSnafu,
    UnsupportedDataTypeSnafu,
};
use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use crate::file_system::file::stream_reader::FileStreamReader;
use crate::file_system::FileSystem;
use crate::tsm::chunk::Chunk;
use crate::tsm::chunk_group::{ChunkGroup, ChunkGroupMeta};
use crate::tsm::codec::{
    get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec, get_ts_codec,
    get_u64_codec,
};
use crate::tsm::footer::Footer;
use crate::tsm::mutable_column::MutableColumn;
use crate::tsm::page::{Page, PageMeta, PageWriteSpec};
use crate::tsm::{ColumnGroupID, TsmTombstone, FOOTER_SIZE};
use crate::{file_utils, ColumnFileId, TskvError};

pub struct TsmMetaData {
    footer: Arc<Footer>,
    chunk_group_meta: Arc<ChunkGroupMeta>,
    chunk_group: BTreeMap<String, Arc<ChunkGroup>>,
    chunk: BTreeMap<SeriesId, Arc<Chunk>>,
}

impl TsmMetaData {
    pub fn new(
        footer: Arc<Footer>,
        chunk_group_meta: Arc<ChunkGroupMeta>,
        chunk_group: BTreeMap<String, Arc<ChunkGroup>>,
        chunk: BTreeMap<SeriesId, Arc<Chunk>>,
    ) -> Self {
        Self {
            footer,
            chunk_group_meta,
            chunk_group,
            chunk,
        }
    }

    pub fn footer(&self) -> Arc<Footer> {
        self.footer.clone()
    }

    pub fn chunk_group_meta(&self) -> Arc<ChunkGroupMeta> {
        self.chunk_group_meta.clone()
    }

    pub fn chunk_group(&self) -> &BTreeMap<String, Arc<ChunkGroup>> {
        &self.chunk_group
    }

    pub fn chunk(&self) -> &BTreeMap<SeriesId, Arc<Chunk>> {
        &self.chunk
    }

    pub fn table_schema(&self, table_name: &str) -> Option<TskvTableSchemaRef> {
        self.chunk_group_meta.table_schema(table_name)
    }

    pub fn table_schema_by_sid(&self, series_id: SeriesId) -> Option<TskvTableSchemaRef> {
        let table_name = self.table_name(series_id)?;
        self.table_schema(table_name)
    }

    pub fn table_name(&self, series_id: SeriesId) -> Option<&str> {
        for (table_name, series_map) in self.chunk_group.iter() {
            if series_map
                .chunks()
                .iter()
                .any(|c| c.series_id() == series_id)
            {
                return Some(table_name.as_ref());
            }
        }
        None
    }
}

pub struct TsmReader {
    file_id: ColumnFileId,
    reader: Box<FileStreamReader>,
    tsm_meta: Arc<TsmMetaData>,
    tombstone: Arc<TsmTombstone>,
}

impl TsmReader {
    pub async fn open(tsm_path: impl AsRef<Path>) -> TskvResult<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let file_system = LocalFileSystem::new(LocalFileType::ThreadPool);
        let reader = file_system
            .open_file_reader(&path)
            .await
            .map_err(|e| TskvError::FileSystemError { source: e })?;

        let file_id = file_utils::get_tsm_file_id_by_path(&path)?;

        let footer = Arc::new(read_footer(&reader).await?);
        let chunk_group_meta = Arc::new(read_chunk_group_meta(&reader, &footer).await?);
        let chunk_group = read_chunk_groups(&reader, &chunk_group_meta).await?;
        let chunk = read_chunk(&reader, &chunk_group).await?;

        let tombstone_path = path.parent().unwrap_or_else(|| Path::new("/"));
        let tombstone = Arc::new(TsmTombstone::open(tombstone_path, file_id).await?);

        let tsm_meta = Arc::new(TsmMetaData::new(
            footer,
            chunk_group_meta,
            chunk_group,
            chunk,
        ));

        Ok(Self {
            // file_location: path,
            file_id,
            reader,
            tsm_meta,
            tombstone,
        })
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn footer(&self) -> &Footer {
        &self.tsm_meta.footer
    }

    pub fn has_tombstone(&self) -> bool {
        !self.tombstone.is_empty()
    }

    pub fn chunk_group_meta(&self) -> &ChunkGroupMeta {
        &self.tsm_meta.chunk_group_meta
    }

    pub fn chunk_group(&self) -> &BTreeMap<String, Arc<ChunkGroup>> {
        &self.tsm_meta.chunk_group
    }

    pub fn chunk(&self) -> &BTreeMap<SeriesId, Arc<Chunk>> {
        &self.tsm_meta.chunk
    }

    pub fn tsm_meta_data(&self) -> Arc<TsmMetaData> {
        self.tsm_meta.clone()
    }

    pub fn tombstone(&self) -> Arc<TsmTombstone> {
        self.tombstone.clone()
    }

    pub async fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_range: TimeRange,
    ) -> TskvResult<BTreeMap<SeriesId, Vec<(ColumnGroupID, Vec<PageMeta>)>>> {
        let meta = self.tsm_meta.clone();
        let mut map = BTreeMap::new();
        for series_id in series_ids {
            let mut column_groups = vec![];
            if meta.footer().maybe_series_exist(series_id) {
                if let Some(chunk) = meta.chunk().get(series_id) {
                    for (id, column_group) in chunk.column_group() {
                        let page_time_range = column_group.time_range();
                        let mut pages = vec![];
                        for page in column_group.pages() {
                            if page_time_range.overlaps(&time_range) {
                                pages.push(page.meta().clone());
                            }
                        }
                        if !pages.is_empty() {
                            column_groups.push((*id, pages));
                        }
                    }
                }
            }
            map.insert(*series_id, column_groups);
        }
        Ok(map)
    }

    pub async fn read_page(&self, page_spec: &PageWriteSpec) -> TskvResult<Page> {
        read_page(&self.reader, page_spec).await
    }

    pub async fn read_series_pages(
        &self,
        series_id: SeriesId,
        column_group_id: ColumnGroupID,
    ) -> TskvResult<Vec<Page>> {
        let chunk = self.chunk();
        let reader = &self.reader;
        if let Some(chunk) = chunk.get(&series_id) {
            for (id, column_group) in chunk.column_group() {
                if *id != column_group_id {
                    continue;
                }
                let mut res_page = Vec::with_capacity(column_group.pages().len());
                for page in column_group.pages() {
                    let page = read_page(reader, page).await?;
                    res_page.push(page);
                }
                return Ok(res_page);
            }
        }
        Ok(vec![])
    }

    pub async fn read_datablock_raw(
        &self,
        series_id: SeriesId,
        column_group_id: ColumnGroupID,
    ) -> TskvResult<Vec<u8>> {
        let chunk = self.chunk();
        if let Some(chunk) = chunk.get(&series_id) {
            for (id, column_group) in chunk.column_group() {
                if *id != column_group_id {
                    continue;
                }
                let mut res_column_group = vec![0u8; column_group.size() as usize];
                self.reader
                    .read_at(column_group.pages_offset() as usize, &mut res_column_group)
                    .await
                    .map_err(|e| {
                        ReadTsmSnafu {
                            reason: e.to_string(),
                        }
                        .build()
                    })?;
                return Ok(res_column_group);
            }
        }
        Ok(vec![])
    }

    pub async fn read_record_batch(
        &self,
        series_id: SeriesId,
        column_group_id: ColumnGroupID,
    ) -> TskvResult<RecordBatch> {
        let column_group = self.read_series_pages(series_id, column_group_id).await?;
        let schema = self
            .tsm_meta
            .table_schema_by_sid(series_id)
            .context(CommonSnafu {
                reason: format!("table schema for series id : {} not found", series_id),
            })?;
        let record_batch = decode_pages(
            column_group,
            schema,
            Some((self.tombstone.clone(), series_id)),
        )?;
        Ok(record_batch)
    }

    pub fn table_schema(&self, table_name: &str) -> Option<TskvTableSchemaRef> {
        self.tsm_meta.chunk_group_meta.table_schema(table_name)
    }
}

pub fn decode_buf_to_pages(
    chunk: Arc<Chunk>,
    column_group_id: ColumnGroupID,
    pages_buf: &[u8],
) -> TskvResult<Vec<Page>> {
    let column_group = chunk
        .column_group()
        .get(&column_group_id)
        .context(CommonSnafu {
            reason: format!(
                "column group for column group id : {} not found",
                column_group_id
            ),
        })?;
    let mut pages = Vec::with_capacity(column_group.pages().len());

    for page in column_group.pages() {
        let offset = (page.offset() - column_group.pages_offset()) as usize;
        let end = offset + page.size as usize;
        let page_buf = pages_buf.get(offset..end).context(CommonSnafu {
            reason: "page_buf get error".to_string(),
        })?;
        let page = Page {
            meta: page.meta.clone(),
            bytes: Bytes::from(page_buf.to_vec()),
        };
        let page_result = page.crc_validation()?;
        pages.push(page_result);
    }
    Ok(pages)
}

impl Debug for TsmReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TsmReader")
            .field("file_id", &self.file_id)
            .field("footer", &self.tsm_meta.footer)
            .field("chunk_group_meta", &self.tsm_meta.chunk_group_meta)
            .field("chunk_group", &self.tsm_meta.chunk_group)
            .field("chunk", &self.tsm_meta.chunk)
            .finish()
    }
}

pub async fn read_footer(reader: &FileStreamReader) -> TskvResult<Footer> {
    if reader.len() < FOOTER_SIZE {
        return Err(ReadTsmSnafu {
            reason: "file is too small".to_string(),
        }
        .build());
    };
    let pos = reader.len() - FOOTER_SIZE;
    let mut buffer = vec![0u8; FOOTER_SIZE];
    reader.read_at(pos, &mut buffer).await.map_err(|e| {
        ReadTsmSnafu {
            reason: e.to_string(),
        }
        .build()
    })?;
    Footer::deserialize(&buffer)
}

pub async fn read_chunk_group_meta(
    reader: &FileStreamReader,
    footer: &Footer,
) -> TskvResult<ChunkGroupMeta> {
    let pos = footer.table().chunk_group_offset();
    let mut buffer = vec![0u8; footer.table().chunk_group_size() as usize];
    reader
        .read_at(pos as usize, &mut buffer)
        .await
        .map_err(|e| {
            ReadTsmSnafu {
                reason: e.to_string(),
            }
            .build()
        })?; // read chunk group meta
    let specs = ChunkGroupMeta::deserialize(&buffer)?;
    Ok(specs)
}

pub async fn read_chunk_groups(
    reader: &FileStreamReader,
    chunk_group_meta: &ChunkGroupMeta,
) -> TskvResult<BTreeMap<String, Arc<ChunkGroup>>> {
    let mut specs = BTreeMap::new();
    for chunk in chunk_group_meta.tables().values() {
        let pos = chunk.chunk_group_offset();
        let mut buffer = vec![0u8; chunk.chunk_group_size() as usize];
        reader
            .read_at(pos as usize, &mut buffer)
            .await
            .map_err(|e| {
                ReadTsmSnafu {
                    reason: e.to_string(),
                }
                .build()
            })?; // read chunk group meta
        let group = Arc::new(ChunkGroup::deserialize(&buffer)?);
        specs.insert(chunk.name().to_string(), group);
    }
    Ok(specs)
}

pub async fn read_chunk(
    reader: &FileStreamReader,
    chunk_group: &BTreeMap<String, Arc<ChunkGroup>>,
) -> TskvResult<BTreeMap<SeriesId, Arc<Chunk>>> {
    let mut chunks = BTreeMap::new();
    for group in chunk_group.values() {
        for chunk_spec in group.chunks() {
            let pos = chunk_spec.chunk_offset();
            let mut buffer = vec![0u8; chunk_spec.chunk_size() as usize];
            reader
                .read_at(pos as usize, &mut buffer)
                .await
                .map_err(|e| {
                    ReadTsmSnafu {
                        reason: e.to_string(),
                    }
                    .build()
                })?;
            let chunk = Arc::new(Chunk::deserialize(&buffer)?);
            chunks.insert(chunk_spec.series_id(), chunk);
        }
    }
    Ok(chunks)
}

async fn read_page(reader: &FileStreamReader, page_spec: &PageWriteSpec) -> TskvResult<Page> {
    let pos = page_spec.offset();
    let mut buffer = vec![0u8; page_spec.size() as usize];
    reader
        .read_at(pos as usize, &mut buffer)
        .await
        .map_err(|e| {
            ReadTsmSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
    let page = Page {
        meta: page_spec.meta().clone(),
        bytes: Bytes::from(buffer),
    };
    page.crc_validation()
}

pub fn decode_pages(
    pages: Vec<Page>,
    table_schema: TskvTableSchemaRef,
    tomb: Option<(Arc<TsmTombstone>, SeriesId)>,
) -> TskvResult<RecordBatch> {
    let mut target_arrays = Vec::new();
    if let Some((tomb, series_id)) = tomb {
        let time_column = {
            let col_id = table_schema.time_column().id;
            let time_page =
                pages
                    .iter()
                    .find(|f| f.meta.column.id == col_id)
                    .context(CommonSnafu {
                        reason: "time field not found".to_string(),
                    })?;
            // time_page.to_column()?
            time_page_to_column(
                time_page.data_buffer(),
                time_page.meta(),
                &NullBitset::Ref(time_page.null_bitset()),
            )?
        };

        let time_range = match time_column.data() {
            PrimaryColumnData::I64(_, min, max) => TimeRange::new(*min, *max),
            _ => {
                return Err(CommonSnafu {
                    reason: "time column data type error".to_string(),
                }
                .build())
            }
        };
        let fields = pages
            .iter()
            .map(|page| Field::from(&page.meta.column))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(fields, table_schema.meta()));
        for page in pages {
            let null_bits = if tomb.overlaps(series_id, page.meta.column.id, &time_range) {
                let null_bitset = update_nullbits_by_tombstone(
                    &time_column,
                    &tomb,
                    series_id,
                    &time_range,
                    &page,
                )?;
                NullBitset::Own(null_bitset)
            } else {
                NullBitset::Ref(page.null_bitset())
            };
            let array = data_buf_to_arrow_array(&page, null_bits)?;
            target_arrays.push(array);
        }
        let record_batch = RecordBatch::try_new(schema, target_arrays).context(ArrowSnafu)?;
        Ok(record_batch)
    } else {
        let fields = pages
            .iter()
            .map(|page| Field::from(&page.meta.column))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(fields, table_schema.meta()));
        for page in pages {
            let array = page.to_arrow_array()?;
            target_arrays.push(array);
        }
        let record_batch = RecordBatch::try_new(schema, target_arrays).context(ArrowSnafu)?;
        Ok(record_batch)
    }
}

pub fn time_page_to_column(
    data_buffer: &[u8],
    meta: &PageMeta,
    bitset: &NullBitset,
) -> TskvResult<MutableColumn> {
    let col_type = &meta.column.column_type;
    match col_type {
        ColumnType::Time(_) => {
            let mut col =
                MutableColumn::empty_with_cap(meta.column.clone(), meta.num_values as usize)?;
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_ts_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .context(DecodeSnafu)?;

            let mut target = target.into_iter();
            for i in 0..bitset.len() {
                if bitset.get(i) {
                    col.push(Some(FieldVal::Integer(target.next().context(
                        TsmPageSnafu {
                            reason: "data buffer not enough".to_string(),
                        },
                    )?)))?;
                } else {
                    col.push(None)?;
                }
            }
            Ok(col)
        }
        _ => Err(TskvError::CommonError {
            reason: format!(
                "Invalid column type for time_page_to_column. Expected: Time, Actual: {:?}",
                col_type
            ),
            location: location!(),
            backtrace: Backtrace::generate(),
        }),
    }
}

pub fn decode_pages_buf(
    pages_buf: &[u8],
    chunk: Arc<Chunk>,
    column_group_id: ColumnGroupID,
    table_schema: TskvTableSchemaRef,
) -> TskvResult<RecordBatch> {
    let pages = decode_buf_to_pages(chunk, column_group_id, pages_buf)?;
    let data_block = decode_pages(pages, table_schema, None)?;
    Ok(data_block)
}

pub async fn page_to_arrow_array_with_tomb(
    page: Page,
    reader: Arc<TsmReader>,
    series_id: SeriesId,
    time_page_meta: Arc<PageWriteSpec>,
    time_range: TimeRange,
) -> TskvResult<ArrayRef> {
    let tombstone = reader.tombstone();
    let null_bitset = if tombstone.overlaps(series_id, page.meta.column.id, &time_range) {
        let time_page = reader.read_page(&time_page_meta).await?;
        // let column = time_page.to_column()?;
        let column = time_page_to_column(
            time_page.data_buffer(),
            time_page.meta(),
            &NullBitset::Ref(time_page.null_bitset()),
        )?;
        let null_bitset =
            update_nullbits_by_tombstone(&column, &tombstone, series_id, &time_range, &page)?;
        NullBitset::Own(null_bitset)
    } else {
        NullBitset::Ref(page.null_bitset())
    };

    data_buf_to_arrow_array(&page, null_bitset)
}

fn update_nullbits_by_tombstone(
    time_column: &MutableColumn,
    tomb: &TsmTombstone,
    series_id: SeriesId,
    time_range: &TimeRange,
    page: &Page,
) -> TskvResult<BitSet> {
    let time_ranges = tomb.get_overlapped_time_ranges(series_id, page.meta.column.id, time_range);
    let mut null_bitset = page.null_bitset().to_bitset();
    for time_range in time_ranges {
        let start_index = time_column
            .data()
            .binary_search_for_i64_col(time_range.min_ts)
            .map_err(|e| TskvError::ColumnDataError { source: e })?
            .unwrap_or_else(|x| x);
        let end_index = time_column
            .data()
            .binary_search_for_i64_col(time_range.max_ts)
            .map_err(|e| TskvError::ColumnDataError { source: e })?
            .map(|x| x + 1)
            .unwrap_or_else(|x| x);
        null_bitset.clear_bits(start_index, end_index);
    }
    Ok(null_bitset)
}

pub fn data_buf_to_arrow_array(page: &Page, null_bitset: NullBitset) -> TskvResult<ArrayRef> {
    let data_buffer = page.data_buffer();
    let num_values = page.meta.num_values as usize;
    let encoding = get_encoding(data_buffer);

    let buffer = Buffer::from_vec(null_bitset.null_bitset_slice());
    let null_buffer = NullBuffer::new(BooleanBuffer::new(buffer, 0, num_values));

    let page_buffer = Buffer::from_vec(page.null_bitset().bytes().to_vec());
    let page_null_buffer = NullBuffer::new(BooleanBuffer::new(page_buffer, 0, num_values));

    match page.meta().column.column_type.to_physical_type() {
        PhysicalCType::Time(precision) => decode_to_arrow_timestamp(
            data_buffer,
            encoding,
            &page_null_buffer,
            precision,
            &null_buffer,
        ),
        PhysicalCType::Field(PhysicalDType::Integer) => {
            let encoding = get_encoding(data_buffer);
            let codec = get_i64_codec(encoding);
            let array_ref = codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .unwrap();
            updated_nullbuffer(array_ref, &page_null_buffer, &null_buffer)
        }
        PhysicalCType::Field(PhysicalDType::Float) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_f64_codec(encoding);
            let array_ref = ts_codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .unwrap();
            updated_nullbuffer(array_ref, &page_null_buffer, &null_buffer)
        }
        PhysicalCType::Field(PhysicalDType::Unsigned) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_u64_codec(encoding);
            let array_ref = ts_codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .unwrap();
            updated_nullbuffer(array_ref, &page_null_buffer, &null_buffer)
        }
        PhysicalCType::Field(PhysicalDType::Boolean) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_bool_codec(encoding);
            let array_ref = ts_codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .unwrap();
            updated_nullbuffer(array_ref, &page_null_buffer, &null_buffer)
        }
        PhysicalCType::Field(PhysicalDType::String) | PhysicalCType::Tag => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_str_codec(encoding);
            let array_ref = ts_codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .unwrap();
            updated_nullbuffer(array_ref, &page_null_buffer, &null_buffer)
        }
        PhysicalCType::Field(PhysicalDType::Unknown) => Err(UnsupportedDataTypeSnafu {
            dt: "unknown".to_string(),
        }
        .build()),
    }
}

fn decode_to_arrow_timestamp(
    data: &[u8],
    encoding: Encoding,
    null_bitset: &NullBuffer,
    precision: TimeUnit,
    updated_nullbuffer: &NullBuffer,
) -> TskvResult<ArrayRef> {
    let codec = get_ts_codec(encoding);
    let array_ref = codec
        .decode_to_array(data, null_bitset)
        .context(DecodeSnafu)?;
    let array = array_ref
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            TsmPageSnafu {
                reason: "Arrow array is not Int64Array".to_string(),
            }
            .build()
        })?;

    let array = if !updated_nullbuffer.eq(null_bitset) {
        let data = array.to_data();
        let nulls = updated_nullbuffer.buffer().clone();
        let new_data = ArrayData::builder(data.data_type().clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .unwrap();
        Int64Array::from(new_data)
    } else {
        array.clone()
    };

    let timestamp_array: ArrayRef = match precision {
        TimeUnit::Second => Arc::new(array.reinterpret_cast::<TimestampSecondType>()),
        TimeUnit::Millisecond => Arc::new(array.reinterpret_cast::<TimestampMillisecondType>()),
        TimeUnit::Microsecond => Arc::new(array.reinterpret_cast::<TimestampMicrosecondType>()),
        TimeUnit::Nanosecond => Arc::new(array.reinterpret_cast::<TimestampNanosecondType>()),
    };

    Ok(timestamp_array)
}

fn updated_nullbuffer(
    array_ref: ArrayRef,
    page_nulls: &NullBuffer,
    updated_nulls: &NullBuffer,
) -> TskvResult<ArrayRef> {
    if page_nulls.eq(updated_nulls) {
        return Ok(array_ref);
    }

    let data_type = array_ref.data_type();
    let data = array_ref.to_data();
    let nulls = updated_nulls.buffer().clone();

    let new_data = match data_type {
        DataType::UInt64 => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(UInt64Array::from(d)) as ArrayRef),
        DataType::Int64 => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(Int64Array::from(d)) as ArrayRef),
        DataType::Float64 => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(Float64Array::from(d)) as ArrayRef),
        DataType::Boolean => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(BooleanArray::from(d)) as ArrayRef),
        DataType::Utf8 => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(StringArray::from(d)) as ArrayRef),
        _ => {
            return Err(TskvError::UnsupportedDataType {
                dt: data_type.to_string(),
                location: location!(),
                backtrace: Backtrace::generate(),
            })
        }
    };
    Ok(new_data.unwrap())
}
