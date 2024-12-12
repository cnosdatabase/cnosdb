#[cfg(test)]
mod compact_test;
#[cfg(test)]
mod delta_compact_test;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use models::{SeriesId, SeriesKey};
use snafu::{OptionExt, ResultExt};
use trace::{info, trace};
use utils::BloomFilter;

use super::metrics::VnodeCompactionMetrics;
use crate::compaction::comapcting_block_meta_group::CompactingBlockMetaGroup;
use crate::compaction::compacting_block_meta::CompactingBlockMeta;
use crate::compaction::metrics::DurationMetricRecorder;
use crate::compaction::utils::filter_record_batch_by_time_range;
use crate::compaction::writer_wrapper::WriterWrapper;
use crate::compaction::CompactReq;
use crate::error::{ArrowSnafu, CommonSnafu, TskvResult};
use crate::tsfamily::version::{CompactMeta, VersionEdit};
use crate::tsm::chunk::Chunk;
use crate::tsm::page::Page;
use crate::tsm::reader::{decode_pages, decode_pages_buf, TsmReader};
use crate::tsm::ColumnGroupID;
use crate::ColumnFileId;

/// Temporary compacting data block.
/// - priority: When merging two (timestamp, value) pair with the same
/// timestamp from two data blocks, pair from data block with lower
/// priority will be discarded.
#[derive(Debug)]
pub enum CompactingBlock {
    Decoded {
        series_id: SeriesId,
        series_key: SeriesKey,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
    },
    Encoded {
        table_schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: SeriesKey,
        time_range: TimeRange,
        record_batch: Vec<Page>,
    },
    Raw {
        table_schema: TskvTableSchemaRef,
        meta: Arc<Chunk>,
        column_group_id: ColumnGroupID,
        raw: Vec<u8>,
    },
}

impl CompactingBlock {
    pub fn decoded(
        series_id: SeriesId,
        series_key: SeriesKey,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
    ) -> CompactingBlock {
        CompactingBlock::Decoded {
            series_id,
            series_key,
            table_schema,
            record_batch,
        }
    }

    pub fn encoded(
        table_schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: SeriesKey,
        time_range: TimeRange,
        record_batch: Vec<Page>,
    ) -> CompactingBlock {
        CompactingBlock::Encoded {
            table_schema,
            series_id,
            series_key,
            time_range,
            record_batch,
        }
    }

    pub fn raw(
        chunk: Arc<Chunk>,
        table_schema: TskvTableSchemaRef,
        column_group_id: ColumnGroupID,
        raw: Vec<u8>,
    ) -> CompactingBlock {
        CompactingBlock::Raw {
            meta: chunk,
            table_schema,
            column_group_id,
            raw,
        }
    }

    pub fn decode(self) -> TskvResult<RecordBatch> {
        match self {
            CompactingBlock::Decoded { record_batch, .. } => Ok(record_batch),
            CompactingBlock::Encoded {
                record_batch,
                table_schema,
                ..
            } => decode_pages(record_batch, table_schema.meta(), None),
            CompactingBlock::Raw {
                raw,
                meta,
                table_schema,
                column_group_id,
                ..
            } => decode_pages_buf(&raw, meta, column_group_id, table_schema),
        }
    }

    pub fn decode_opt(self, time_range: TimeRange) -> TskvResult<RecordBatch> {
        let record_batch = self.decode()?;
        filter_record_batch_by_time_range(record_batch, time_range).context(ArrowSnafu)
    }

    pub fn len(&self) -> usize {
        match self {
            CompactingBlock::Decoded { record_batch, .. } => record_batch.num_rows(),
            CompactingBlock::Encoded { record_batch, .. } => {
                record_batch[0].meta.num_values as usize
            }
            CompactingBlock::Raw {
                meta,
                column_group_id,
                ..
            } => {
                meta.column_group()[column_group_id].pages()[0]
                    .meta
                    .num_values as usize
            }
        }
    }
}

pub struct CompactingFile {
    tsm_reader_index: usize,
    tsm_reader: Arc<TsmReader>,
    series_idx: usize,
    series_ids: Vec<SeriesId>,
    out_time_ranges: Option<Arc<TimeRanges>>,
}

impl CompactingFile {
    fn new(
        tsm_reader_index: usize,
        tsm_reader: Arc<TsmReader>,
        out_time_ranges: Option<Arc<TimeRanges>>,
    ) -> Self {
        let mut series_ids = {
            let chunks = tsm_reader.chunk_group();
            chunks
                .iter()
                .flat_map(|(_, chunk)| {
                    chunk
                        .chunks()
                        .iter()
                        .map(|chunk_meta| chunk_meta.series_id())
                })
                .collect::<Vec<_>>()
        };
        series_ids.sort();

        Self {
            tsm_reader_index,
            tsm_reader,
            series_idx: 0,
            series_ids,
            out_time_ranges,
        }
    }

    fn next(&mut self) {
        self.series_idx += 1;
    }

    fn peek(&self) -> Option<SeriesId> {
        self.series_ids.get(self.series_idx).copied()
    }

    pub async fn get_record_batch(
        &mut self,
        block_meta: &CompactingBlockMeta,
    ) -> TskvResult<Option<RecordBatch>> {
        let column_group = block_meta.column_group()?;
        if let Some(ref time_ranges) = self.out_time_ranges {
            let mut has_data = false;
            let mut is_include = false;
            for time_range in time_ranges.time_ranges() {
                if column_group.time_range().overlaps(&time_range) {
                    has_data = true;
                    if time_range.includes(column_group.time_range()) {
                        is_include = true;
                    }
                    break;
                }
            }
            if !has_data {
                return Ok(None);
            }
            let mut record_batch = block_meta.get_record_batch().await?;
            if !is_include {
                record_batch =
                    filter_record_batch_by_time_range(record_batch, time_ranges.max_time_range())
                        .context(ArrowSnafu)?
            }
            return Ok(Some(record_batch));
        }
        Ok(Some(block_meta.get_record_batch().await?))
    }

    pub async fn get_raw_data(&mut self, block_meta: &CompactingBlockMeta) -> TskvResult<Vec<u8>> {
        let column_group = block_meta.column_group()?;
        if let Some(ref time_ranges) = self.out_time_ranges {
            let mut has_data = false;
            for time_range in time_ranges.time_ranges() {
                if column_group.time_range().overlaps(&time_range) {
                    has_data = true;
                    break;
                }
            }
            if !has_data {
                return Ok(vec![]);
            }
            let raw = block_meta.get_raw_data().await?;
            return Ok(raw);
        }
        block_meta.get_raw_data().await
    }

    pub fn has_tombstone(&self) -> bool {
        self.tsm_reader.has_tombstone()
    }
}

impl Eq for CompactingFile {}

impl PartialEq for CompactingFile {
    fn eq(&self, other: &Self) -> bool {
        self.tsm_reader.file_id() == other.tsm_reader.file_id() && self.peek() == other.peek()
    }
}

impl Ord for CompactingFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.peek(), other.peek()) {
            (Some(sid1), Some(sid2)) => sid1.cmp(&sid2),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for CompactingFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct CompactState {
    tsm_readers: Vec<Arc<TsmReader>>,
    /// The TimeRanges for delta files to partly compact with other files.
    out_time_ranges: Option<Arc<TimeRanges>>,

    compacting_files: Vec<CompactingFile>,
    // /// The time range of data to be merged of level-0 data blocks.
    // /// The level-0 data that out of the thime range will write back to level-0.
    // level_time_range: TimeRange,
    tmp_tsm_blk_meta_iters: Vec<(Arc<Chunk>, ColumnGroupID, usize)>,
    /// Index to mark `Peekable<BlockMetaIterator>` in witch `TsmReader`,
    /// tmp_tsm_blks[i] is in self.tsm_readers[ tmp_tsm_blk_tsm_reader_idx[i] ]
    tsm_reader_to_compacting_file_map: Vec<Option<usize>>,
}

impl CompactState {
    pub fn new(tsm_readers: Vec<Arc<TsmReader>>, out_time_range: TimeRange) -> Self {
        let out_time_ranges = if out_time_range.is_boundless() {
            None
        } else {
            Some(Arc::new(TimeRanges::new(vec![out_time_range])))
        };
        let tsm_readers_len = tsm_readers.len();
        let mut compacting_tsm_readers = Vec::with_capacity(tsm_readers_len);
        let mut compacting_files = Vec::with_capacity(tsm_readers_len);
        let mut compacting_tsm_file_idx = 0_usize;
        for tsm_reader in tsm_readers {
            let f = CompactingFile::new(
                compacting_tsm_file_idx,
                tsm_reader.clone(),
                out_time_ranges.clone(),
            );
            compacting_tsm_readers.push(tsm_reader);
            compacting_files.push(f);
            compacting_tsm_file_idx += 1;
        }
        let mut tsm_reader_to_compacting_file_map = vec![None::<_>; tsm_readers_len];
        for (i, f) in compacting_files.iter().enumerate() {
            tsm_reader_to_compacting_file_map[f.tsm_reader_index] = Some(i);
        }
        Self {
            tsm_readers: compacting_tsm_readers,
            out_time_ranges,

            compacting_files,
            tsm_reader_to_compacting_file_map,
            tmp_tsm_blk_meta_iters: Vec::with_capacity(compacting_tsm_file_idx),
        }
    }

    pub async fn next(
        &mut self,
        compacting_blk_meta_groups: &mut Vec<CompactingBlockMetaGroup>,
    ) -> TskvResult<Option<SeriesId>> {
        // For each tsm-file, get next index reader for current iteration field id
        if let Some(series_id) = self.next_series_id()? {
            compacting_blk_meta_groups.clear();
            // Get all of block_metas of this field id, and group these block_metas.
            self.fill_compacting_block_meta_groups(series_id, compacting_blk_meta_groups)?;
            Ok(Some(series_id))
        } else {
            Ok(None)
        }
    }
}

impl CompactState {
    fn next_series_id(&mut self) -> TskvResult<Option<SeriesId>> {
        self.tmp_tsm_blk_meta_iters.clear();
        let mut curr_sid: SeriesId;
        loop {
            if let Some(f) = self.compacting_files.first() {
                if let Some(sid) = f.peek() {
                    curr_sid = sid;
                } else {
                    return Ok(None);
                }
            } else {
                trace!("no file to select, mark finished");
                return Ok(None);
            }

            let mut loop_tsm_reader_idx;
            for f in self.compacting_files.iter_mut() {
                loop_tsm_reader_idx = f.tsm_reader_index;
                if self.tsm_reader_to_compacting_file_map[f.tsm_reader_index].is_none() {
                    continue;
                }
                let f_sid = match f.peek() {
                    Some(sid) => sid,
                    None => {
                        self.tsm_reader_to_compacting_file_map[loop_tsm_reader_idx] = None;
                        continue;
                    }
                };
                if curr_sid == f_sid {
                    let meta = f
                        .tsm_reader
                        .chunk()
                        .get(&f_sid)
                        .cloned()
                        .context(CommonSnafu {
                            reason: format!(
                                "series id {} not found in file {}",
                                f_sid,
                                f.tsm_reader.file_id()
                            ),
                        })?;
                    let column_groups_id = meta.column_group().keys().cloned().collect::<Vec<_>>();
                    column_groups_id.iter().for_each(|&column_group_id| {
                        let column_group = match meta.column_group().get(&column_group_id) {
                            Some(cg) => cg,
                            None => {
                                return;
                            }
                        };
                        if let Some(time) = self.out_time_ranges.as_ref() {
                            for time_range in time.time_ranges() {
                                if column_group.time_range().overlaps(&time_range) {
                                    self.tmp_tsm_blk_meta_iters.push((
                                        meta.clone(),
                                        column_group_id,
                                        loop_tsm_reader_idx,
                                    ));
                                    return;
                                }
                            }
                        } else {
                            self.tmp_tsm_blk_meta_iters.push((
                                meta.clone(),
                                column_group_id,
                                loop_tsm_reader_idx,
                            ));
                        }
                    });
                    f.next();
                } else {
                    break;
                }
            }

            self.compacting_files.sort();
            for (i, f) in self.compacting_files.iter().enumerate() {
                if self.tsm_reader_to_compacting_file_map[f.tsm_reader_index].is_some() {
                    self.tsm_reader_to_compacting_file_map[f.tsm_reader_index] = Some(i);
                }
            }

            if !self.tmp_tsm_blk_meta_iters.is_empty() {
                trace::trace!(
                    "selected {} blocks meta iterators",
                    self.tmp_tsm_blk_meta_iters.len()
                );
                break;
            } else {
                trace::trace!("iteration field_id {curr_sid} is finished, trying next field.");
                continue;
            }
        }
        Ok(Some(curr_sid))
    }

    /// Clear buffer vector and collect compacting `DataBlock`s into the buffer vector.
    fn fill_compacting_block_meta_groups(
        &mut self,
        seires_id: SeriesId,
        compacting_blk_meta_groups: &mut Vec<CompactingBlockMetaGroup>,
    ) -> TskvResult<()> {
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            return Ok(());
        }

        let mut blk_metas: Vec<CompactingBlockMeta> =
            Vec::with_capacity(self.tmp_tsm_blk_meta_iters.len());
        // Get all block_meta, and check if it's tsm file has a related tombstone file.
        for (chunk, columngroup_id, tsm_reader_idx) in self.tmp_tsm_blk_meta_iters.iter_mut() {
            if let Some(compacting_file_idx) =
                self.tsm_reader_to_compacting_file_map[*tsm_reader_idx]
            {
                blk_metas.push(CompactingBlockMeta::new(
                    *tsm_reader_idx,
                    compacting_file_idx,
                    self.tsm_readers[*tsm_reader_idx].clone(),
                    chunk.clone(),
                    *columngroup_id,
                ));
            }
        }
        if blk_metas.is_empty() {
            // Cannot load any data.
            return Ok(());
        }
        // Sort by field_id, min_ts and max_ts.
        blk_metas.sort();

        let mut blk_meta_groups: Vec<CompactingBlockMetaGroup> =
            Vec::with_capacity(blk_metas.len());
        for blk_meta in blk_metas {
            blk_meta_groups.push(CompactingBlockMetaGroup::new(seires_id, blk_meta)?);
        }
        // Compact blk_meta_groups.
        let mut i = 0;
        loop {
            let mut head_idx = i;
            // Find the first non-empty as head.
            for (off, bmg) in blk_meta_groups[i..].iter().enumerate() {
                if !bmg.is_empty() {
                    head_idx += off;
                    break;
                }
            }
            if head_idx >= blk_meta_groups.len() - 1 {
                // There no other blk_meta_group to merge with the last one.
                break;
            }
            let mut head = blk_meta_groups[head_idx].clone();
            i = head_idx + 1;
            for bmg in blk_meta_groups[i..].iter_mut() {
                if bmg.is_empty() {
                    continue;
                }
                if head.overlaps(bmg) {
                    head.append(bmg);
                }
            }
            blk_meta_groups[head_idx] = head;
        }

        for cbm_group in blk_meta_groups {
            if !cbm_group.is_empty() {
                compacting_blk_meta_groups.push(cbm_group);
            }
        }
        Ok(())
    }
}

pub async fn run_normal_compaction_job(
    request: CompactReq,
    metrics: VnodeCompactionMetrics,
) -> TskvResult<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    info!(
        "Compaction: Running compaction job on {}",
        request.compact_task
    );

    if request.files.is_empty() {
        // Nothing to compact
        return Ok(None);
    }
    let compact_task = request.compact_task;
    // ALl tsm-files that can be deleted after compaction.
    let mut tsm_file_metas_will_delete: Vec<CompactMeta> = Vec::new();

    // Buffers all tsm-files and it's indexes for this compaction
    let mut tsm_readers = Vec::new();
    for file in request.files.iter() {
        tsm_file_metas_will_delete.push(CompactMeta::from(file.as_ref()));
        let tsm_reader = request.version.get_tsm_reader(file.file_path()).await?;
        tsm_readers.push(tsm_reader);
    }

    let (mut version_edit, file_metas) =
        compact_files(request, tsm_readers, TimeRange::all(), metrics).await?;

    // Level 0 files that can be deleted after compaction.
    version_edit.del_files = tsm_file_metas_will_delete;

    info!("Compaction({compact_task}): Compact finished, version edits: {version_edit:?}");
    Ok(Some((version_edit, file_metas)))
}

pub async fn run_delta_compaction_job(
    request: CompactReq,
    metrics: VnodeCompactionMetrics,
) -> TskvResult<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    info!(
        "Compaction({}): Running compaction job on {request}",
        request.compact_task
    );

    let (delta_files, level_file) = request.split_delta_and_level_files();
    if delta_files.is_empty() {
        return Ok(None);
    }
    let compact_task = request.compact_task;
    let out_time_range = request.out_time_range;
    // Collect l0-files that can be deleted after compaction.
    let mut l0_file_metas_will_delete: Vec<CompactMeta> = Vec::new();
    // Collect l0-files that partly deleted after compaction.
    let mut l0_file_metas_will_partly_delete: Vec<CompactMeta> = Vec::new();
    // Open l0-files and tsm-file to run compaction.
    let mut tsm_readers = match &level_file {
        None => Vec::with_capacity(delta_files.len()),
        Some(f) => {
            let mut tsm_readers = Vec::with_capacity(1 + delta_files.len());
            tsm_readers.push(request.version.get_tsm_reader(f.file_path()).await?);
            tsm_readers
        }
    };

    for file in delta_files {
        let l0_file_reader = request.version.get_tsm_reader(file.file_path()).await?;
        let compacted_all_excluded_time_range = l0_file_reader
            .add_tombstone_and_compact_to_tmp(out_time_range)
            .await?;
        if compacted_all_excluded_time_range.includes(file.time_range()) {
            // The tombstone includes all of the l0-file, so delete it.
            l0_file_metas_will_delete.push(CompactMeta::from(file.as_ref()));
        } else {
            // The tombstone includes partly of the l0_file and the excluded range close to the edge.
            l0_file_metas_will_partly_delete.push(CompactMeta::new_del_file_part(
                0,
                file.file_id(),
                out_time_range.min_ts,
                out_time_range.max_ts,
            ));
        }
        tsm_readers.push(l0_file_reader);
    }

    let (mut version_edit, file_metas) =
        compact_files(request, tsm_readers, out_time_range, metrics).await?;

    // Level 0 files that can be deleted after compaction.
    version_edit.del_files = l0_file_metas_will_delete;
    if let Some(f) = level_file {
        // Lvel 1-4 file that can be deleted after compaction.
        version_edit.del_files.push(f.as_ref().into());
    }
    // Level 0 files that partly deleted after compaction.
    version_edit.partly_del_files = l0_file_metas_will_partly_delete;

    trace::info!("Compaction({compact_task}): Compact finished, version edits: {version_edit:?}");
    Ok(Some((version_edit, file_metas)))
}

async fn compact_files(
    request: CompactReq,
    tsm_readers: Vec<Arc<TsmReader>>,
    out_time_range: TimeRange,
    mut metrics: VnodeCompactionMetrics,
) -> TskvResult<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)> {
    let max_block_size = request.version.storage_opt().max_datablock_size as usize;
    let mut state = CompactState::new(tsm_readers, out_time_range);
    let mut writer_wrapper = WriterWrapper::new(&request).await?;

    let mut previous_merged_block = Option::<CompactingBlock>::None;
    let mut merging_blk_meta_groups = Vec::with_capacity(32);
    let mut curr_sid: Option<SeriesId> = None;

    metrics.begin();
    loop {
        let sid = match state.next(&mut merging_blk_meta_groups).await? {
            Some(sid) => sid,
            None => break,
        };
        for blk_meta_group in merging_blk_meta_groups.drain(..) {
            if let Some(c_sid) = curr_sid {
                if c_sid != sid {
                    // Iteration of next field id, write previous merged block.
                    if let Some(blk) = previous_merged_block.take() {
                        // Write the small previous merged block.
                        trace::trace!(
                            "write the previous compacting block (fid={curr_sid:?}): {:?}",
                            blk
                        );
                        metrics.write_begin();
                        writer_wrapper.write(blk).await?;
                        metrics.write_end()
                    }
                }
            }
            curr_sid = Some(sid);

            let mut merged_blks = blk_meta_group
                .merge_with_previous_block(
                    previous_merged_block.take(),
                    max_block_size,
                    &out_time_range,
                    &mut state.compacting_files,
                    &mut metrics,
                )
                .await?;
            if merged_blks.is_empty() {
                continue;
            }
            if merged_blks.len() == 1 && merged_blks[0].len() < max_block_size {
                // The only one data block too small, try to extend the next compacting blocks.
                previous_merged_block = Some(merged_blks.remove(0));
                continue;
            }
            let last_blk_idx = merged_blks.len() - 1;
            for (i, blk) in merged_blks.drain(..).enumerate() {
                if i == last_blk_idx && blk.len() < max_block_size {
                    // The last data block too small, try to extend to
                    // the next compacting blocks (current field id).
                    trace::trace!("compacting block (fid={sid}) {:?} is too small, try to merge with the next compacting blocks", blk);
                    previous_merged_block = Some(blk);
                    break;
                }
                trace::trace!("write compacting block(fid={sid}): {:?}", blk);
                metrics.write_begin();
                writer_wrapper.write(blk).await?;
                metrics.write_end();
            }
        }
    }
    if let Some(blk) = previous_merged_block {
        trace::trace!(
            "write the final compacting block(fid={curr_sid:?}): {:?}",
            blk
        );
        metrics.write_begin();
        writer_wrapper.write(blk).await?;
        metrics.write_end();
    }

    let (version_edit, file_metas) = writer_wrapper.close().await?;

    metrics.end(true);

    Ok((version_edit, file_metas))
}

#[cfg(test)]
pub mod test {
    use core::panic;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, UInt64Builder};
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampNanosecondArray};
    use models::codec::Encoding;
    use models::predicate::domain::TimeRange;
    use models::schema::tskv_table_schema::TskvTableSchemaRef;
    use models::{SeriesId, SeriesKey};
    use tokio::sync::RwLock;

    use crate::file_system::async_filesystem::LocalFileSystem;
    use crate::file_system::FileSystem;
    use crate::kv_option::Options;
    use crate::tsfamily::column_file::ColumnFile;
    use crate::tsfamily::version::VersionEdit;
    use crate::tsm::reader::TsmReader;
    use crate::tsm::writer::TsmWriter;
    use crate::{file_utils, LevelId};

    pub async fn write_data_blocks_to_column_file(
        dir: impl AsRef<Path>,
        data: Vec<HashMap<SeriesId, RecordBatch>>,
        table_schema: TskvTableSchemaRef,
        level_id: LevelId,
    ) -> (u64, Vec<Arc<ColumnFile>>) {
        if !LocalFileSystem::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut cfs = Vec::new();
        let mut file_seq = 0;
        for (i, d) in data.iter().enumerate() {
            file_seq = i as u64 + 1;
            let mut writer = TsmWriter::open(&dir, file_seq, 0, level_id == 0, Encoding::Null)
                .await
                .unwrap();
            for (sid, data_blks) in d.iter() {
                writer
                    .write_record_batch(
                        *sid,
                        SeriesKey::default(),
                        table_schema.clone(),
                        data_blks.clone(),
                    )
                    .await
                    .unwrap();
            }
            writer.finish().await.unwrap();
            let mut cf = ColumnFile::new(
                file_seq,
                level_id,
                TimeRange::new(writer.min_ts(), writer.max_ts()),
                writer.size(),
                writer.path(),
            );
            cf.set_series_id_filter(RwLock::new(Some(Arc::new(
                writer.series_bloom_filter().clone(),
            ))));
            cfs.push(Arc::new(cf));
        }
        (file_seq + 1, cfs)
    }

    pub async fn read_data_blocks_from_column_file(
        path: impl AsRef<Path>,
    ) -> HashMap<SeriesId, Vec<RecordBatch>> {
        let tsm_reader = TsmReader::open(&path).await.unwrap();
        let mut data = HashMap::new();
        for (sid, chunk) in tsm_reader.chunk() {
            let mut blks = vec![];
            for column_group_id in chunk.column_group().keys() {
                let blk = tsm_reader
                    .read_record_batch(*sid, *column_group_id)
                    .await
                    .unwrap();
                blks.push(blk)
            }
            data.insert(*sid, blks);
        }
        data
    }

    pub(crate) fn timestamp_column(data: Vec<i64>) -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from(data))
    }

    pub(crate) fn i64_column(data: Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(data))
    }

    pub(crate) fn i64_some_column(data: Vec<Option<i64>>) -> ArrayRef {
        let mut builder = Int64Builder::new();
        for datum in data {
            builder.append_option(datum);
        }
        Arc::new(builder.finish())
    }

    fn get_result_file_path(
        dir: impl AsRef<Path>,
        version_edit: VersionEdit,
        level_id: LevelId,
    ) -> PathBuf {
        if !version_edit.add_files.is_empty() {
            if let Some(f) = version_edit
                .add_files
                .into_iter()
                .find(|f| f.level == level_id)
            {
                if level_id == 0 {
                    return file_utils::make_delta_file(dir, f.file_id);
                }
                return file_utils::make_tsm_file(dir, f.file_id);
            }
            panic!("VersionEdit::add_files doesn't contain any file matches level-{level_id}.");
        }

        panic!("VersionEdit doesn't contain any add_files.");
    }

    /// Compare DataBlocks in path with the expected_Data using assert_eq.
    pub async fn check_column_file(
        dir: impl AsRef<Path>,
        version_edit: VersionEdit,
        expected_data: HashMap<SeriesId, Vec<RecordBatch>>,
        expected_data_level: LevelId,
    ) {
        let path = get_result_file_path(dir, version_edit, expected_data_level);
        let mut data = read_data_blocks_from_column_file(path).await;
        let mut data_series_ids = data.keys().copied().collect::<Vec<_>>();
        data_series_ids.sort_unstable();
        let mut expected_data_series_ids = expected_data.keys().copied().collect::<Vec<_>>();
        expected_data_series_ids.sort_unstable();
        assert_eq!(data_series_ids, expected_data_series_ids);

        for (k, v) in expected_data.into_iter() {
            let record_batches = data.remove(&k).unwrap();
            println!("v.len(): {}", v.len());
            println!("data_blks.len(): {}", record_batches.len());
            for (v, data_blc) in v.iter().zip(record_batches.iter()) {
                assert_eq!(v, data_blc);
            }
        }
    }

    pub fn create_options(base_dir: String, compact_trigger_file_num: u32) -> Arc<Options> {
        let mut config = config::tskv::get_config_for_test();
        config.storage.path = base_dir;
        config.storage.max_datablock_size = 1000;
        config.storage.compact_trigger_file_num = compact_trigger_file_num;
        let opt = Options::from(&config);
        Arc::new(opt)
    }

    pub(crate) fn generate_column_ts(min_ts: i64, max_ts: i64) -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from_iter_values(min_ts..=max_ts))
    }

    pub(crate) fn generate_column_i64(len: usize, none_range: Vec<(usize, usize)>) -> ArrayRef {
        let mut builder = Int64Builder::new();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                builder.append_null();
            } else {
                builder.append_value(i as i64);
            }
        }
        Arc::new(builder.finish())
    }

    pub(crate) fn generate_column_u64(len: usize, none_range: Vec<(usize, usize)>) -> ArrayRef {
        let mut builder = UInt64Builder::new();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                builder.append_null();
            } else {
                builder.append_value(i as u64);
            }
        }
        Arc::new(builder.finish())
    }

    pub(crate) fn generate_column_f64(len: usize, none_range: Vec<(usize, usize)>) -> ArrayRef {
        let mut builder = Float64Builder::new();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                builder.append_null();
            } else {
                builder.append_value(i as f64);
            }
        }
        Arc::new(builder.finish())
    }

    pub(crate) fn generate_column_bool(len: usize, none_range: Vec<(usize, usize)>) -> ArrayRef {
        let mut builder = BooleanBuilder::new();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                builder.append_null();
            } else {
                builder.append_value(true);
            }
        }
        Arc::new(builder.finish())
    }
}
