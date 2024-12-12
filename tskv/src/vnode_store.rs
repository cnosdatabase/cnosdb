use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use metrics::average::U64Average;
use models::meta_data::VnodeId;
use models::predicate::domain::{ResolvedPredicate, TimeRange, TimeRanges};
use models::utils::now_timestamp_secs;
use models::{ColumnId, SeriesId, SeriesKey};
use protos::kv_service::{raft_write_command, WritePointsResponse, *};
use replication::EngineMetrics;
use snafu::{OptionExt, ResultExt};
use tokio::sync::RwLock;
use trace::span_ext::SpanExt;
use trace::{debug, error, info, Span, SpanContext};
use utils::precision::Precision;

use crate::compaction::job::FlushJob;
use crate::compaction::FlushReq;
use crate::database::Database;
use crate::error::{IndexErrSnafu, InvalidParamSnafu, InvalidPointTableSnafu, TskvResult};
use crate::index::ts_index::TSIndex;
use crate::schema::error::{FieldNotFoundSnafu, TableNotFoundSnafu};
use crate::tsfamily::summary::Summary;
use crate::tsfamily::tseries_family::TseriesFamily;
use crate::{file_utils, TsKvContext, VnodeSnapshot};

#[derive(Clone)]
pub struct VnodeStorage {
    id: VnodeId,
    ctx: Arc<TsKvContext>,
    db: Arc<RwLock<Database>>,
    summary: Arc<RwLock<Summary>>,
    flush_job: Arc<FlushJob>,
    ts_index: Arc<RwLock<TSIndex>>,
    ts_family: Arc<RwLock<TseriesFamily>>,

    snapshots: Vec<VnodeSnapshot>,

    write_apply_duration: U64Average,
    write_build_group_duration: U64Average,
    write_put_points_duration: U64Average,
}

impl VnodeStorage {
    pub fn new(
        ctx: Arc<TsKvContext>,
        id: VnodeId,
        db: Arc<RwLock<Database>>,
        summary: Arc<RwLock<Summary>>,
        ts_index: Arc<RwLock<TSIndex>>,
        ts_family: Arc<RwLock<TseriesFamily>>,
    ) -> Self {
        let flush_job = FlushJob::new(ctx.clone(), summary.clone());
        Self {
            id,
            db,
            ctx,
            summary,
            flush_job,
            ts_index,
            ts_family,
            snapshots: vec![],
            write_apply_duration: U64Average::default(),
            write_build_group_duration: U64Average::default(),
            write_put_points_duration: U64Average::default(),
        }
    }

    pub fn ts_family(&self) -> Arc<RwLock<TseriesFamily>> {
        self.ts_family.clone()
    }

    pub fn db(&self) -> Arc<RwLock<Database>> {
        self.db.clone()
    }

    pub fn id(&self) -> VnodeId {
        self.id
    }

    pub async fn apply(
        &self,
        ctx: &replication::ApplyContext,
        command: raft_write_command::Command,
    ) -> TskvResult<Vec<u8>> {
        match command {
            raft_write_command::Command::WriteData(cmd) => {
                let precision = Precision::from(cmd.precision as u8);
                if let Err(err) = self.write(ctx, cmd.data, precision, None).await {
                    if ctx.apply_type == replication::APPLY_TYPE_WAL {
                        info!("recover: write points: {}", err);
                    } else {
                        return Err(err);
                    }
                }

                Ok(vec![])
            }

            raft_write_command::Command::DropTable(cmd) => {
                self.drop_table(&cmd.table).await?;
                Ok(vec![])
            }

            raft_write_command::Command::DropColumn(cmd) => {
                if let Err(err) = self.drop_table_column(&cmd.table, &cmd.column).await {
                    if ctx.apply_type == replication::APPLY_TYPE_WAL {
                        info!("recover: drop column: {}", err);
                    } else {
                        return Err(err);
                    }
                }
                Ok(vec![])
            }

            raft_write_command::Command::UpdateTags(cmd) => {
                self.update_tags_value(ctx, &cmd).await?;
                Ok(vec![])
            }

            raft_write_command::Command::DeleteFromTable(cmd) => {
                self.delete_from_table(&cmd).await?;
                Ok(vec![])
            }
        }
    }

    pub async fn get_snapshot(&mut self) -> TskvResult<Option<VnodeSnapshot>> {
        if let Some(snapshot) = self.snapshots.last_mut() {
            snapshot.active_time = now_timestamp_secs();

            info!("Snapshot: Get snapshot {}", snapshot);
            return Ok(Some(snapshot.clone()));
        }

        Ok(None)
    }

    pub async fn create_snapshot(&mut self) -> TskvResult<VnodeSnapshot> {
        debug!("Snapshot: create snapshot on vnode: {}", self.id);

        let (snapshot_version, snapshot_ve) = {
            let ts_family_w = self.ts_family.write().await;

            let version = ts_family_w.version();
            let ve = ts_family_w.build_version_edit();
            (version, ve)
        };

        let last_seq_no = snapshot_version.last_seq();
        let snapshot = VnodeSnapshot {
            last_seq_no,
            vnode_id: self.id,
            node_id: self.ctx.options.storage.node_id,
            version_edit: snapshot_ve,
            version: Some(snapshot_version),
            create_time: chrono::Local::now().format("%Y%m%d_%H%M%S_%3f").to_string(),
            active_time: 0,
        };
        info!("Snapshot: build snapshot: {}", snapshot);

        self.snapshots.retain(|x| {
            now_timestamp_secs() - x.active_time < self.ctx.options.storage.snapshot_holding_time
        });

        self.snapshots.push(snapshot.clone());

        Ok(snapshot)
    }

    /// Build a new Vnode from the VersionSnapshot, existing Vnode with the same VnodeId
    /// will be deleted.
    pub async fn apply_snapshot(
        &mut self,
        snapshot: VnodeSnapshot,
        shapshot_dir: &Path,
    ) -> TskvResult<()> {
        info!("Snapshot: apply snapshot {}", snapshot);

        // clear all snapshot
        self.snapshots = vec![];

        // delete already exist data
        let mut db_wlock = self.db.write().await;
        db_wlock.del_tsfamily_index(self.id);

        // reopen new summary file
        let summary_file = file_utils::make_tsfamily_summary_file(
            self.ctx
                .options
                .storage
                .ts_family_dir(&db_wlock.owner(), self.id),
        );

        let summary = Summary::new(self.id, self.ctx.clone(), summary_file).await?;
        *self.summary.write().await = summary;

        // apply data and reopen
        let mut version_edit = snapshot.version_edit.clone();
        version_edit.update_vnode_id(self.id);
        let ts_family = db_wlock
            .add_tsfamily(version_edit, shapshot_dir, self.summary.clone())
            .await?;

        let ts_index = db_wlock.rebuild_tsfamily_index(ts_family.clone()).await?;

        self.ts_index = ts_index;
        self.ts_family = ts_family;

        Ok(())
    }

    pub async fn flush(&self, block: bool, force: bool, compact: bool) -> TskvResult<()> {
        if force {
            let mut ts_family = self.ts_family.write().await;
            ts_family.switch_to_immutable();
        } else {
            let mut ts_family = self.ts_family.write().await;
            if !ts_family.check_to_flush().await {
                return Ok(());
            }
        }

        let owner = self.ts_family.read().await.owner();
        let request = FlushReq {
            tf_id: self.id,
            completion: false,
            owner: owner.to_string(),
            ts_index: self.ts_index.clone(),
            ts_family: self.ts_family.clone(),
            trigger_compact: compact,
            flush_metrics: Default::default(),
        };

        if block {
            FlushJob::run_block(self.flush_job.clone(), request).await
        } else {
            FlushJob::run_spawn(self.flush_job.clone(), request)
        }
    }

    pub async fn metrics(&self) -> EngineMetrics {
        let last_applied_id = self.ts_family.read().await.last_seq();
        let flushed_apply_id = self.ts_family.read().await.version().last_seq();
        let mut snapshot_apply_id = 0;
        if let Some(snapshot) = self.snapshots.last() {
            snapshot_apply_id = snapshot.last_seq_no;
        }

        EngineMetrics {
            last_applied_id,
            flushed_apply_id,
            snapshot_apply_id,
            write_apply_duration: self.write_apply_duration.average(),
            write_build_group_duration: self.write_build_group_duration.average(),
            write_put_points_duration: self.write_put_points_duration.average(),
        }
    }

    async fn write(
        &self,
        ctx: &replication::ApplyContext,
        points: Vec<u8>,
        precision: Precision,
        span_context: Option<&SpanContext>,
    ) -> TskvResult<WritePointsResponse> {
        let write_start = std::time::Instant::now();
        let span = Span::from_context("tskv engine write cache", span_context);
        let fb_points = flatbuffers::root::<protos::models::Points>(&points)
            .context(crate::error::InvalidFlatbufferSnafu)?;
        let tables = fb_points.tables().context(InvalidPointTableSnafu)?;

        let (mut recover_from_wal, mut strict_write) = (false, None);
        if ctx.apply_type == replication::APPLY_TYPE_WAL {
            (recover_from_wal, strict_write) = (true, Some(true));
        }

        let write_group = {
            let span = Span::enter_with_parent("build write group", &span);
            self.db
                .read()
                .await
                .build_write_group(
                    precision,
                    tables,
                    self.ts_index.clone(),
                    recover_from_wal,
                    strict_write,
                )
                .await
                .map_err(|err| {
                    span.error(err.to_string());
                    err
                })?
        };
        self.write_build_group_duration
            .add(write_start.elapsed().as_micros() as u64);

        let write_mem_start = std::time::Instant::now();
        let res = {
            let span = Span::enter_with_parent("put points", &span);
            match self
                .ts_family
                .read()
                .await
                .put_points(ctx.index, write_group)
            {
                Ok(points_number) => Ok(WritePointsResponse { points_number }),
                Err(err) => {
                    span.error(err.to_string());
                    Err(err)
                }
            }
        };
        self.write_put_points_duration
            .add(write_mem_start.elapsed().as_micros() as u64);
        self.write_apply_duration
            .add(write_start.elapsed().as_micros() as u64);

        // check to flush memecache to tsm files
        let _ = self.flush(false, false, true).await;

        res
    }

    async fn drop_table(&self, table: &str) -> TskvResult<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.
        let db_owner = self.db.read().await.owner();
        let schemas = self.db.read().await.get_schemas();
        if let Some(fields) = schemas.get_table_schema(table).await? {
            let column_ids: Vec<ColumnId> = fields.columns().iter().map(|f| f.id).collect();
            info!(
                "Drop table: deleting {} columns in table: {db_owner}.{table}",
                column_ids.len()
            );

            let series_ids = self
                .ts_index
                .read()
                .await
                .get_series_id_list(table, &[])
                .await
                .context(IndexErrSnafu)?;
            self.ts_family
                .write()
                .await
                .delete_series(&series_ids, &TimeRange::all());

            info!(
                "Drop table: vnode {} deleting {} fields in table: {db_owner}.{table}",
                self.id,
                column_ids.len() * series_ids.len()
            );

            let version = self.ts_family.read().await.super_version();
            version
                .add_tombstone(&series_ids, &column_ids, &TimeRange::all())
                .await?;

            info!(
                "Drop table: index {} deleting {} fields in table: {db_owner}.{table}",
                self.id,
                series_ids.len()
            );

            let mut index_w = self.ts_index.write().await;
            for sid in series_ids {
                index_w.del_series_info(sid).await.context(IndexErrSnafu)?;
            }
        }

        Ok(())
    }

    async fn drop_table_column(&self, table: &str, column_name: &str) -> TskvResult<()> {
        let db_name = self.db.read().await.db_name();
        let schema = self
            .db
            .read()
            .await
            .get_table_schema(table)
            .await?
            .context(TableNotFoundSnafu {
                database: db_name.to_string(),
                table: table.to_string(),
            })?;

        let column_id = schema
            .column(column_name)
            .context(FieldNotFoundSnafu {
                msg: format!("'{}'.'{}'.{}", db_name, table, column_name),
            })?
            .id;

        self.drop_table_columns(table, &[column_id]).await?;

        Ok(())
    }

    /// Update the value of the tag type columns of the specified table
    ///
    /// `new_tags` is the new tags, and the tag key must be included in all series
    ///
    /// # Parameters
    /// - `tenant` - The tenant name.
    /// - `database` - The database name.
    /// - `new_tags` - The tags and its new tag value.
    /// - `matched_series` - The series that need to be updated.
    /// - `dry_run` - Whether to only check if the `update_tags_value` is successful, if it is true, the update will not be performed.
    ///
    /// # Examples
    ///
    /// We have a table `tbl` as follows
    ///
    /// ```text
    /// +----+-----+-----+-----+
    /// | ts | tag1| tag2|field|
    /// +----+-----+-----+-----+
    /// | 1  | t1a | t2b | f1  |
    /// +----+-----+-----+-----+
    /// | 2  | t1a | t2c | f2  |
    /// +----+-----+-----+-----+
    /// | 3  | t1b | t2c | f3  |
    /// +----+-----+-----+-----+
    /// ```
    ///
    /// Execute the following update statement
    ///
    /// ```sql
    /// UPDATE tbl SET tag1 = 't1c' WHERE tag2 = 't2c';
    /// ```
    ///
    /// The `new_tags` is `[tag1 = 't1c']`, and the `matched_series` is `[(tag1 = 't1a', tag2 = 't2c'), (tag1 = 't1b', tag2 = 't2c')]`
    ///
    /// TODO Specify vnode id
    async fn update_tags_value(
        &self,
        ctx: &replication::ApplyContext,
        cmd: &UpdateTagsRequest,
    ) -> TskvResult<()> {
        let new_tags = cmd
            .new_tags
            .iter()
            .cloned()
            .map(
                |protos::kv_service::UpdateSetValue { key, value }| crate::UpdateSetValue {
                    key,
                    value,
                },
            )
            .collect::<Vec<_>>();

        let mut series = Vec::with_capacity(cmd.matched_series.len());
        for key in cmd.matched_series.iter() {
            let ss = SeriesKey::decode(key).map_err(|e| InvalidParamSnafu {
            reason: format!("Deserialize 'matched_series' of 'UpdateTagsRequest' failed, expected: SeriesKey, error msg: {e}"),
        }.build())?;
            series.push(ss);
        }

        // 准备数据
        // 获取待更新的 series key，更新后的 series key 及其对应的 series id
        let mut check_conflict = true;
        if ctx.apply_type == replication::APPLY_TYPE_WAL {
            check_conflict = false;
        }
        let (old_series_keys, new_series_keys, sids) = self
            .ts_index
            .read()
            .await
            .prepare_update_tags_value(&new_tags, &series, check_conflict)
            .await
            .context(IndexErrSnafu)?;

        if cmd.dry_run {
            return Ok(());
        }

        let series = sids
            .iter()
            .copied()
            .zip(new_series_keys.iter().cloned())
            .collect::<HashMap<_, _>>();

        // 更新索引
        if let Err(err) = self
            .ts_index
            .write()
            .await
            .update_series_key(old_series_keys, new_series_keys, sids, false)
            .await
        {
            error!("Update tags value tag of TSIndex({}): {}", self.id, err);

            return Err(crate::error::TskvError::IndexErr { source: err });
        }

        self.ts_family.read().await.update_tag_value(series).await?;

        Ok(())
    }

    async fn delete_from_table(&self, cmd: &DeleteFromTableRequest) -> TskvResult<()> {
        let predicate =
            bincode::deserialize::<ResolvedPredicate>(&cmd.predicate).map_err(|err| {
                InvalidParamSnafu {
                    reason: format!("Predicate of delete_from_table is invalid, error: {err}"),
                }
                .build()
            })?;

        let tag_domains = predicate.tags_filter();
        let series_ids = {
            let table_schema = match self.db.read().await.get_table_schema(&cmd.table).await? {
                None => return Ok(()),
                Some(schema) => schema,
            };

            self.ts_index
                .read()
                .await
                .get_series_ids_by_domains(table_schema.as_ref(), tag_domains)
                .await?
        };

        // 执行delete，删除缓存 & 写墓碑文件
        let time_ranges = predicate.time_ranges();
        self.delete(&cmd.table, &series_ids, &time_ranges).await
    }

    async fn drop_table_columns(&self, table: &str, column_ids: &[ColumnId]) -> TskvResult<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.
        let db_rlock = self.db.read().await;
        let db_owner = db_rlock.owner();
        let schemas = db_rlock.get_schemas();
        if let Some(fields) = schemas.get_table_schema(table).await? {
            let table_column_ids: HashSet<ColumnId> =
                fields.columns().iter().map(|f| f.id).collect();
            let mut to_drop_column_ids = Vec::with_capacity(column_ids.len());
            for cid in column_ids {
                if table_column_ids.contains(cid) {
                    to_drop_column_ids.push(*cid);
                }
            }

            let time_range = TimeRange::all();
            let series_ids = self
                .ts_index
                .read()
                .await
                .get_series_id_list(table, &[])
                .await
                .context(IndexErrSnafu)?;
            info!(
                "drop table column: vnode: {} deleting {} fields in table: {db_owner}.{table}",
                self.id,
                series_ids.len() * to_drop_column_ids.len()
            );

            self.ts_family
                .write()
                .await
                .drop_columns(&series_ids, &to_drop_column_ids);
            let version = self.ts_family.read().await.super_version();
            version
                .add_tombstone(&series_ids, &to_drop_column_ids, &time_range)
                .await?;
        }

        Ok(())
    }

    async fn delete(
        &self,
        table: &str,
        series_ids: &[SeriesId],
        time_ranges: &TimeRanges,
    ) -> TskvResult<()> {
        let vnode = self.ts_family.read().await;
        let db_name = self.db.read().await.db_name();
        vnode.delete_series_by_time_ranges(series_ids, time_ranges);

        let column_ids = self
            .db
            .read()
            .await
            .get_table_schema(table)
            .await?
            .context(TableNotFoundSnafu {
                database: db_name.to_string(),
                table: table.to_string(),
            })?
            .column_ids();

        let version = vnode.super_version();

        // Stop compaction when doing delete TODO

        for time_range in time_ranges.time_ranges() {
            version
                .add_tombstone(series_ids, &column_ids, &time_range)
                .await?;
        }

        Ok(())
    }

    pub fn get_summary(&self) -> Arc<RwLock<Summary>> {
        self.summary.clone()
    }

    pub fn get_ts_family(&self) -> Arc<RwLock<TseriesFamily>> {
        self.ts_family.clone()
    }

    pub async fn sync_index(&self) {
        let _ = self.ts_index.write().await.flush().await;
    }
}
