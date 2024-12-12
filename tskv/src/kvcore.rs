use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;
use memory_pool::MemoryPoolRef;
use meta::error::MetaError;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeId;
use models::predicate::domain::ColumnDomains;
use models::schema::database_schema::{make_owner, split_owner};
use models::{SeriesId, SeriesKey};
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::broadcast::{self, Sender as BroadcastSender};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::RwLock;
use trace::{debug, error, info, warn};

use crate::compaction::job::CompactJob;
use crate::compaction::metrics::{CompactionType, VnodeCompactionMetrics};
use crate::compaction::{self, check, pick_compaction, CompactTask};
use crate::database::Database;
use crate::error::{IndexErrSnafu, MetaSnafu, TskvResult};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::index::IndexResult;
use crate::kv_option::{Options, StorageOptions};
use crate::tsfamily::summary::{Summary, SummaryRequest};
use crate::tsfamily::super_version::SuperVersion;
use crate::version_set::{split_to_tsfamily, VersionSet};
use crate::vnode_store::VnodeStorage;
use crate::{file_utils, Engine, TsKvContext};

pub struct TsKv {
    ctx: Arc<TsKvContext>,
    version_set: Arc<RwLock<VersionSet>>,
    close_sender: BroadcastSender<Sender<()>>,
}

impl TsKv {
    pub async fn open(
        meta_manager: MetaRef,
        options: Options,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        metrics: Arc<MetricsRegister>,
    ) -> TskvResult<TsKv> {
        let options = Arc::new(options);
        let (compact_task_sender, compact_task_receiver) = mpsc::channel(1024);
        let ctx = Arc::new(TsKvContext {
            metrics,
            memory_pool,
            meta: meta_manager,
            compact_task_sender,
            options: options.clone(),
            runtime: runtime.clone(),
        });

        let old_summary = file_utils::make_summary_file(options.storage.summary_dir(), 0);
        if LocalFileSystem::try_exists(&old_summary) {
            split_to_tsfamily(ctx.clone(), old_summary).await.unwrap();
            std::fs::remove_dir_all(options.storage.summary_dir()).unwrap();
        }

        let version_set = Arc::new(RwLock::new(VersionSet::new(ctx.clone())));

        let compact_job = CompactJob::new(ctx.clone(), version_set.clone());
        compact_job.start_jobs(compact_task_receiver).await;
        Self::run_flush_cold_vnode_job(ctx.clone(), version_set.clone());

        let (close_sender, _close_receiver) = broadcast::channel(1);
        let core = Self {
            ctx,
            version_set,
            close_sender,
        };

        Ok(core)
    }

    fn run_flush_cold_vnode_job(ctx: Arc<TsKvContext>, version_set: Arc<RwLock<VersionSet>>) {
        let compact_trigger_cold_duration = ctx.options.storage.compact_trigger_cold_duration;
        let compact_trigger_file_num = ctx.options.storage.compact_trigger_file_num;
        let compact_task_sender = ctx.compact_task_sender.clone();
        if compact_trigger_cold_duration == Duration::ZERO {
            return;
        }

        ctx.runtime.spawn(async move {
            let mut cold_check_interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                cold_check_interval.tick().await;

                let vnodes = version_set.read().await.vnodes();
                for (tf_id, vnode) in vnodes.iter() {
                    let ts_family = vnode.ts_family();
                    let last_modified = ts_family.read().await.get_last_modified().await;
                    if let Some(last_modified) = last_modified {
                        if last_modified.elapsed() < compact_trigger_cold_duration {
                            continue;
                        }

                        if let Err(e) = vnode.flush(true, true, true).await {
                            trace::error!("flush code vnode {} faild: {:?}", tf_id, e);
                        }
                    }

                    let version = ts_family.read().await.super_version().version.clone();
                    let levels = version.levels_info();
                    let task = if levels[0].files.len() as u32 >= compact_trigger_file_num {
                        CompactTask::Delta(*tf_id)
                    } else {
                        CompactTask::Normal(*tf_id)
                    };

                    if let Err(e) = compact_task_sender.send(task).await {
                        warn!("Failed to send compact task: {task}: {e}");
                    }
                }
            }
        });
    }

    async fn sync_indexs(&self) -> IndexResult<()> {
        let vs_guard = self.version_set.read().await;
        for (_, vnode_storage) in vs_guard.vnodes().iter() {
            vnode_storage.sync_index().await;
        }
        Ok(())
    }

    pub async fn get_db(&self, tenant: &str, database: &str) -> Option<Arc<RwLock<Database>>> {
        self.version_set.read().await.get_db(tenant, database)
    }

    pub fn get_ctx(&self) -> Arc<TsKvContext> {
        self.ctx.clone()
    }

    pub(crate) async fn get_db_or_else_create(
        &self,
        tenant: &str,
        db_name: &str,
    ) -> TskvResult<Arc<RwLock<Database>>> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, db_name) {
            return Ok(db);
        }

        let db_schema = self
            .ctx
            .meta
            .tenant_meta(tenant)
            .await
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })
            .context(MetaSnafu)?
            .get_db_schema(db_name)
            .context(MetaSnafu)?
            .ok_or(MetaError::DatabaseNotFound {
                database: db_name.to_string(),
            })
            .context(MetaSnafu)?;

        let db = self.version_set.write().await.create_db(db_schema).await?;
        Ok(db)
    }
}

#[async_trait::async_trait]
impl Engine for TsKv {
    async fn open_tsfamily(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
    ) -> TskvResult<VnodeStorage> {
        if let Some(vnode) = self.version_set.read().await.get_vnode(vnode_id) {
            return Ok(vnode.clone());
        }

        let database = self.get_db_or_else_create(tenant, db_name).await?;

        let mut vs_lock = self.version_set.write().await;
        if let Some(vnode) = vs_lock.get_vnode(vnode_id) {
            return Ok(vnode.clone());
        }

        let owner = make_owner(tenant, db_name);
        let dir = self.ctx.options.storage.ts_family_dir(&owner, vnode_id);
        let summary_file = file_utils::make_tsfamily_summary_file(dir);
        let summary = Summary::new(vnode_id, self.ctx.clone(), summary_file).await?;
        let summary = Arc::new(RwLock::new(summary));
        let ts_family = database
            .write()
            .await
            .get_tsfamily_or_create(vnode_id, summary.clone())
            .await?;
        let ts_index = database
            .write()
            .await
            .get_tsindex_or_create(vnode_id)
            .await?;

        let vnode = VnodeStorage::new(
            self.ctx.clone(),
            vnode_id,
            database,
            summary,
            ts_index,
            ts_family,
        );

        vs_lock.add_vnode(vnode_id, vnode.clone());

        Ok(vnode)
    }

    async fn remove_tsfamily(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> TskvResult<()> {
        self.version_set.write().await.remove_vnode(vnode_id);
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            db.write().await.del_tsfamily_index(vnode_id);
        }

        Ok(())
    }

    async fn flush_tsfamily(
        &self,
        _tenant: &str,
        _database: &str,
        vnode_id: VnodeId,
        trigger_compact: bool,
    ) -> TskvResult<()> {
        let vnode_opt = self.version_set.read().await.get_vnode(vnode_id).cloned();
        if let Some(vnode) = vnode_opt {
            vnode.flush(true, true, trigger_compact).await?;
        }

        Ok(())
    }

    async fn get_series_id_by_filter(
        &self,
        tenant: &str,
        database: &str,
        tab: &str,
        vnode_id: VnodeId,
        filter: &ColumnDomains<String>,
    ) -> TskvResult<Vec<SeriesId>> {
        let (schema, ts_index) = match self.version_set.read().await.get_db(tenant, database) {
            Some(db) => {
                let db = db.read().await;
                let schema = match db.get_table_schema(tab).await? {
                    None => return Ok(vec![]),
                    Some(schema) => schema,
                };
                let ts_index = match db.get_ts_index(vnode_id) {
                    Some(ts_index) => ts_index,
                    None => return Ok(vec![]),
                };
                (schema, ts_index)
            }
            None => return Ok(vec![]),
        };

        let res = ts_index
            .read()
            .await
            .get_series_ids_by_domains(&schema, filter)
            .await?;

        Ok(res)
    }

    async fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        _table: &str,
        vnode_id: VnodeId,
        series_id: &[SeriesId],
    ) -> TskvResult<Vec<SeriesKey>> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            Ok(db
                .read()
                .await
                .get_series_key(vnode_id, series_id)
                .await
                .context(IndexErrSnafu)?)
        } else {
            Ok(vec![])
        }
    }

    async fn get_db_version(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> TskvResult<Option<Arc<SuperVersion>>> {
        if let Some(vnode) = self.version_set.read().await.get_vnode(vnode_id) {
            Ok(Some(vnode.ts_family().read().await.super_version()))
        } else {
            debug!(
                "not found ts_family {} in {}.{}",
                vnode_id, tenant, database
            );
            Ok(None)
        }
    }

    fn get_storage_options(&self) -> Arc<StorageOptions> {
        self.ctx.options.storage.clone()
    }

    async fn compact(&self, vnode_ids: Vec<VnodeId>) -> TskvResult<()> {
        for vnode_id in vnode_ids {
            if let Some(vnode) = self.version_set.read().await.get_vnode(vnode_id) {
                let ts_family = vnode.ts_family().clone();

                // TODO: stop current and prevent next flush and compaction.
                if !ts_family.read().await.can_compaction() {
                    warn!("forbidden compaction on moving vnode {}", vnode_id);
                    return Ok(());
                }

                let owner = ts_family.read().await.owner();
                let (tenant, db_name) = split_owner(&owner);

                if let Err(e) = self.flush_tsfamily(tenant, db_name, vnode_id, true).await {
                    error!("Failed to flush vnode {}: {:?}", vnode_id, e);
                }

                let version = ts_family.read().await.version();
                if let Some(mut req) = pick_compaction(CompactTask::Manual(vnode_id), version).await
                {
                    req.set_file_id(vnode.get_summary().read().await.file_id());
                    let vnode_compaction_metrics = VnodeCompactionMetrics::new(
                        &self.ctx.metrics,
                        self.ctx.options.storage.node_id,
                        vnode_id,
                        CompactionType::Manual,
                        self.ctx.options.storage.collect_compaction_metrics,
                    );
                    match compaction::run_compaction_job(req, vnode_compaction_metrics).await {
                        Ok(Some((version_edit, file_metas))) => {
                            let request = SummaryRequest {
                                version_edit,
                                mem_caches: None,
                                file_metas: Some(file_metas),
                                ts_family: ts_family.clone(),
                            };
                            let summary = vnode.get_summary();
                            summary.write().await.apply_version_edit(&request).await?;
                        }
                        Ok(None) => {
                            info!("There is nothing to compact.");
                        }
                        Err(e) => {
                            error!("Compaction job failed: {:?}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_vnode_hash_tree(&self, vnode_id: VnodeId) -> TskvResult<RecordBatch> {
        let vnode_opt = self.version_set.read().await.get_vnode(vnode_id).cloned();
        if let Some(vnode) = vnode_opt {
            let ts_family = vnode.get_ts_family();
            let owner = ts_family.read().await.owner();
            let (tenant, db_name) = split_owner(&owner);
            self.flush_tsfamily(tenant, db_name, vnode_id, false)
                .await?;

            return check::vnode_checksum(ts_family).await;
        }

        Ok(RecordBatch::new_empty(check::vnode_table_checksum_schema()))
    }

    async fn close(&self) {
        let (tx, mut rx) = mpsc::channel(1);
        if let Err(e) = self.close_sender.send(tx) {
            error!("Failed to broadcast close signal: {:?}", e);
        }
        while let Some(_x) = rx.recv().await {
            continue;
        }
        self.sync_indexs()
            .await
            .expect("Tskv Index haven't been sync.");

        info!("TsKv closed");
    }
}

impl std::fmt::Debug for TsKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tskv engine type")
    }
}
