use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cache::ShardedAsyncCache;
use models::schema::database_schema::DatabaseSchema;
use parking_lot::RwLock as SyncRwLock;
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::sync::RwLock;
use utils::id_generator::IDGenerator;
use utils::BloomFilter;

use super::tseries_family::TseriesFamily;
use super::version::{CompactMeta, VnodeAction};
use crate::error::{IOSnafu, RecordFileIOSnafu, TskvError, TskvResult};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::mem_cache::memcache::MemCache;
use crate::record_file::{Reader, RecordDataType, RecordDataVersion, Writer};
use crate::tsfamily::level_info::LevelInfo;
use crate::tsfamily::version::Version;
use crate::{file_utils, ColumnFileId, TsKvContext, VersionEdit, VnodeId};

const SUMMARY_BUFFER_SIZE: usize = 1024 * 1024;

pub struct Summary {
    ctx: Arc<TsKvContext>,
    tsf_id: VnodeId,
    writer: Writer,
    file_id: IDGenerator,
}

impl Summary {
    // create a new summary file
    pub async fn new(
        tsf_id: VnodeId,
        ctx: Arc<TsKvContext>,
        summary_file: impl AsRef<std::path::Path>,
    ) -> TskvResult<Self> {
        let mut writer = Writer::open(summary_file, SUMMARY_BUFFER_SIZE).await?;
        writer.sync().await?;

        Ok(Self {
            ctx,
            tsf_id,
            writer,
            file_id: IDGenerator::new(1),
        })
    }

    /// Recover Version from summary file
    pub async fn recover(&self, db_schema: &DatabaseSchema) -> TskvResult<Option<Version>> {
        let summary_file = file_utils::make_tsfamily_summary_file(
            self.ctx
                .options
                .storage
                .ts_family_dir(&db_schema.owner(), self.tsf_id),
        );
        let mut reader = Box::new(Reader::open(summary_file).await?);

        let mut tsf_edits = vec![];
        loop {
            let res = reader.read_record().await;
            match res {
                Ok(result) => {
                    let ed = VersionEdit::decode(&result.data)?;
                    if ed.act_tsf == VnodeAction::Add {
                        tsf_edits = vec![ed];
                    } else if ed.act_tsf == VnodeAction::Delete {
                        tsf_edits.clear();
                    } else {
                        tsf_edits.push(ed);
                    }
                }
                Err(TskvError::Eof) => break,
                Err(TskvError::RecordFileHashCheckFailed { .. }) => continue,
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if tsf_edits.is_empty() {
            return Ok(None);
        }

        let mut max_seq_no = 0;
        let mut max_file_id = 0_u64;
        let mut max_level_ts = i64::MIN;
        let mut files: HashMap<u64, CompactMeta> = HashMap::new();
        for e in tsf_edits {
            max_seq_no = max(max_seq_no, e.seq_no);
            max_level_ts = max(max_level_ts, e.max_level_ts);
            max_file_id = max(max_file_id, e.file_id);
            for m in e.del_files {
                files.remove(&m.file_id);
            }
            for m in e.add_files {
                files.insert(m.file_id, m);
            }
        }
        self.file_id.set(max_file_id + 1);

        // Recover levels_info according to `CompactMeta`s;
        let owner = Arc::new(db_schema.owner());
        let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(
            db_schema.config.max_cache_readers() as usize,
        ));
        let weak_tsm_reader_cache = Arc::downgrade(&tsm_reader_cache);
        let mut levels =
            LevelInfo::init_levels(owner.clone(), self.tsf_id, self.ctx.options.storage.clone());
        for meta in files.into_values() {
            levels[meta.level as usize].push_compact_meta(
                &meta,
                RwLock::new(None),
                weak_tsm_reader_cache.clone(),
            );
        }

        Ok(Some(Version::new(
            self.tsf_id,
            owner,
            self.ctx.options.storage.clone(),
            max_seq_no,
            levels,
            max_level_ts,
            tsm_reader_cache,
        )))
    }

    /// Write VersionEdits into summary file, generate and then apply new Versions for TseriesFamilies.
    pub async fn apply_version_edit(&mut self, request: &SummaryRequest) -> TskvResult<()> {
        trace::info!("Write version edit to summary {}.", self.tsf_id);
        self.write_record(&request.version_edit).await?;

        // Generate a new Version and then apply it.
        trace::info!("Applying new version {}.", self.tsf_id);
        let mut file_metas = HashMap::new();
        if let Some(tmp) = &request.file_metas {
            file_metas = tmp.clone();
        }

        let mut partly_deleted_file_paths: Vec<PathBuf> = Vec::new();
        let mut partly_deleted_files: HashSet<ColumnFileId> = HashSet::new();

        for compact_meta in request.version_edit.partly_del_files.iter() {
            partly_deleted_file_paths.push(compact_meta.file_path(
                &self.ctx.options.storage,
                request.ts_family.read().await.owner().clone().as_str(),
                request.ts_family.read().await.tf_id(),
            ));
            partly_deleted_files.insert(compact_meta.file_id);
        }

        let new_version = request
            .ts_family
            .read()
            .await
            .version()
            .copy_apply_version_edits(request.version_edit.clone(), &mut file_metas);
        let new_version = Arc::new(new_version);

        let _ = install_tombstones_for_tsm_readers(
            self.ctx.runtime.clone(),
            partly_deleted_file_paths.clone(),
            new_version.clone(),
        )
        .await;

        // Mark all partly deleted files as not-compacting after ombstones are replaced with compact_tmp.
        new_version
            .unmark_compacting_files(&partly_deleted_files)
            .await;

        request
            .ts_family
            .write()
            .await
            .new_version(new_version, request.mem_caches.as_ref());
        trace::info!("Applied new version for ts_family {}.", self.tsf_id);

        if self.writer.file_size() < self.ctx.options.storage.max_summary_size {
            return Ok(());
        }

        let ve = request.ts_family.read().await.build_version_edit();
        self.rewrite_summary_file(&ve).await?;

        Ok(())
    }

    async fn rewrite_summary_file(&mut self, ve: &VersionEdit) -> TskvResult<()> {
        let summary_file = &self.writer.path().clone();
        let tmp_file = file_utils::make_summary_file_tmp(
            self.ctx
                .options
                .storage
                .ts_family_dir(&ve.tsf_name, self.tsf_id),
        );
        LocalFileSystem::remove_if_exists(&tmp_file)
            .map_err(|e| TskvError::FileSystemError { source: e })?;

        self.writer = Writer::open(&tmp_file, SUMMARY_BUFFER_SIZE).await?;
        self.write_record(ve).await?;

        trace::info!("Rename summary file {:?} -> {:?}", tmp_file, summary_file,);
        std::fs::rename(tmp_file, summary_file).context(IOSnafu)?;

        self.writer = Writer::open(summary_file, SUMMARY_BUFFER_SIZE)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn write_record(&mut self, ve: &VersionEdit) -> TskvResult<()> {
        let buf = ve.encode()?;
        let _ = self
            .writer
            .write_record(
                RecordDataVersion::V1.into(),
                RecordDataType::Summary.into(),
                &[&buf],
            )
            .await?;

        self.writer.sync().await
    }

    pub fn file_id(&self) -> IDGenerator {
        self.file_id.clone()
    }

    pub fn next_file_id(&self) -> u64 {
        self.file_id.next_id()
    }
}

/// Replace tombstones with compact_tmp in parallel.
async fn install_tombstones_for_tsm_readers(
    runtime: Arc<Runtime>,
    partly_deleted_file_paths: Vec<PathBuf>,
    version: Arc<Version>,
) -> TskvResult<()> {
    let partly_deleted_files_num = partly_deleted_file_paths.len();
    trace::debug!(
        "Installing tombstones after delta-compaction of {partly_deleted_files_num} tomb files."
    );
    let mut replace_tombstone_compact_tmp_tasks =
        Vec::with_capacity(partly_deleted_file_paths.len());
    for tsm_path in partly_deleted_file_paths {
        let cache_inner = version.clone();
        let jh: tokio::task::JoinHandle<std::result::Result<(), TskvError>> = runtime.spawn(async move {
            let path_display = tsm_path.display();
            let path = path_display.to_string();
            match cache_inner.get_tsm_reader(&path).await {
                Ok(tsm_reader) => {
                    tsm_reader.replace_tombstone_with_compact_tmp().await
                }
                Err(e) => {
                    trace::error!(
                        "Failed to get tsm_reader for file '{path_display}' when trying to replace tombstones generated in delta-compaction: {e}",
                    );
                    Ok(())
                }
            }
        });
        replace_tombstone_compact_tmp_tasks.push(jh);
    }
    let mut results = Vec::with_capacity(replace_tombstone_compact_tmp_tasks.len());
    for jh in replace_tombstone_compact_tmp_tasks {
        match jh.await {
            Ok(Ok(_)) => {
                results.push(Ok(()));
            }
            Ok(Err(e)) => {
                // TODO: This delta-compaction should be failed.
                trace::error!("Failed to replace tombstone with compact_tmp: {e}");
                results.push(Err(e));
            }
            Err(e) => {
                trace::error!("Maybe panicked replacing tombstone with compact_tmp: {e}");
                results.push(Err(RecordFileIOSnafu {
                    reason: "".to_string(),
                }
                .build()));
            }
        }
    }
    trace::debug!(
        "Installed tombstones after delta-compaction of {partly_deleted_files_num} tomb files."
    );

    Ok(())
}

pub async fn print_summary_statistics(path: impl AsRef<Path>) {
    let mut reader = Reader::open(&path).await.unwrap();
    println!("============================================================");
    let mut i = 0_usize;
    loop {
        match reader.read_record().await {
            Ok(record) => {
                let ve = VersionEdit::decode(&record.data).unwrap();
                println!("VersionEdit #{}, vnode_id: {}", i, ve.tsf_id);
                println!("------------------------------------------------------------");
                i += 1;
                if ve.act_tsf == VnodeAction::Add {
                    println!("  Add ts_family: {}", ve.tsf_id);
                    println!("------------------------------------------------------------");
                }
                if ve.act_tsf == VnodeAction::Delete {
                    println!("  Delete ts_family: {}", ve.tsf_id);
                    println!("------------------------------------------------------------");
                }

                println!("  Presist sequence: {}", ve.seq_no);
                println!("------------------------------------------------------------");

                if ve.add_files.is_empty() && ve.del_files.is_empty() {
                    println!("  Add file: None. Delete file: None.");
                }
                if !ve.add_files.is_empty() {
                    let mut buffer = String::new();
                    ve.add_files.iter().for_each(|f| {
                        buffer.push_str(
                            format!("{} (level: {}, {} B), ", f.file_id, f.level, f.file_size)
                                .as_str(),
                        )
                    });
                    if !buffer.is_empty() {
                        buffer.truncate(buffer.len() - 2);
                    }
                    println!("  Add file:[ {} ]", buffer);
                }
                if !ve.del_files.is_empty() {
                    let mut buffer = String::new();
                    ve.del_files.iter().for_each(|f| {
                        buffer.push_str(format!("{} (level: {}), ", f.file_id, f.level).as_str())
                    });
                    if !buffer.is_empty() {
                        buffer.truncate(buffer.len() - 2);
                    }
                    println!("  Delete file:[ {} ]", buffer);
                }
            }
            Err(TskvError::Eof) => break,
            Err(TskvError::RecordFileHashCheckFailed { .. }) => continue,
            Err(err) => panic!("Errors when read summary file: {}", err),
        }
        println!("============================================================");
    }
}

pub struct SummaryRequest {
    pub version_edit: VersionEdit,
    pub ts_family: Arc<RwLock<TseriesFamily>>,
    pub mem_caches: Option<Vec<Arc<SyncRwLock<MemCache>>>>,
    pub file_metas: Option<HashMap<ColumnFileId, Arc<BloomFilter>>>,
}

pub struct SummaryTask {
    pub request: SummaryRequest,
    pub call_back: OneShotSender<TskvResult<()>>,
}

impl SummaryTask {
    pub fn new(
        ts_family: Arc<RwLock<TseriesFamily>>,
        version_edit: VersionEdit,
        file_metas: Option<HashMap<ColumnFileId, Arc<BloomFilter>>>,
        mem_caches: Option<Vec<Arc<SyncRwLock<MemCache>>>>,
        call_back: OneShotSender<TskvResult<()>>,
    ) -> Self {
        Self {
            call_back,
            request: SummaryRequest {
                version_edit,
                ts_family,
                mem_caches,
                file_metas,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use config::tskv::MetaConfig;
    use memory_pool::GreedyMemoryPool;
    use meta::model::meta_admin::AdminMeta;
    use metrics::metric_register::MetricsRegister;
    use models::schema::database_schema::{
        make_owner, DatabaseConfig, DatabaseOptions, DatabaseSchema,
    };
    use models::schema::tenant::TenantOptions;
    use sysinfo::{ProcessRefreshKind, RefreshKind, System};

    use crate::kv_option::Options;
    use crate::tsfamily::summary::{Summary, SummaryRequest};
    use crate::tsfamily::version::{CompactMeta, VersionEdit};
    use crate::{file_utils, Engine, TsKv, VnodeId};

    #[test]
    fn test_version_edit() {
        let mut ve = VersionEdit::default();

        let add_file_100 = CompactMeta {
            file_id: 100,
            ..Default::default()
        };
        ve.add_file(add_file_100, 100_000_000);

        let del_file_101 = CompactMeta {
            file_id: 101,
            ..Default::default()
        };
        ve.del_files.push(del_file_101);

        let ve_buf = ve.encode().unwrap();
        let ve2 = VersionEdit::decode(&ve_buf).unwrap();
        assert_eq!(ve2, ve);

        let ves = vec![ve, ve2];
        let ves_buf = VersionEdit::encode_vec(&ves).unwrap();
        let ves_2 = VersionEdit::decode_vec(&ves_buf).unwrap();
        assert_eq!(ves, ves_2);
    }

    fn new_tskv(base_dir: String) -> TsKv {
        let mut config = config::tskv::get_config_for_test();
        config.storage.path = base_dir;

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(4)
                .build()
                .unwrap(),
        );

        let options = Options::from(&config);
        let runtime_clone = runtime.clone();
        let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));

        runtime_clone.block_on(async {
            let meta_manager =
                AdminMeta::new(config.clone(), Arc::new(MetricsRegister::default())).await;
            meta_manager.add_data_node().await.unwrap();
            let _ = meta_manager
                .create_tenant("cnosdb".to_string(), TenantOptions::default())
                .await;

            TsKv::open(
                meta_manager.clone(),
                options,
                runtime.clone(),
                memory_pool.clone(),
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap()
        })
    }

    fn summary_file(tskv: &TsKv, tenant: &str, db_name: &str, vnode_id: VnodeId) -> PathBuf {
        let owner = make_owner(tenant, db_name);
        file_utils::make_tsfamily_summary_file(
            tskv.get_ctx()
                .options
                .storage
                .ts_family_dir(&owner, vnode_id),
        )
    }

    pub fn kill_process(process_name: &str) {
        println!("- Killing processes {process_name}...");
        let system = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new()),
        );
        for (pid, process) in system.processes() {
            if process.name() == process_name {
                match process.kill_with(sysinfo::Signal::Kill) {
                    Some(true) => println!("- Killed process {pid} ('{}')", process.name()),
                    Some(false) => {
                        println!("- Failed killing process {pid} ('{}')", process.name())
                    }
                    None => println!("- Kill with signal 'Kill' isn't supported on this platform"),
                }
            }
        }
    }

    #[tokio::test]
    async fn test_summary_recover_all() {
        let temp_dir = tempfile::Builder::new().prefix("meta").tempdir().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let cluster_name = "cluster_001".to_string();
        let size = 1024 * 1024 * 1024;
        meta::service::single::start_singe_meta_server(
            path,
            cluster_name,
            &MetaConfig::default(),
            size,
        )
        .await;
        let join_handle = tokio::spawn(async {
            let _ = tokio::task::spawn_blocking(|| {
                test_summary_recover();
                test_summary_write_recover();
            })
            .await;
        });
        join_handle.await.unwrap();
        kill_process("cnosdb-meta");
        temp_dir.close().unwrap();
    }

    fn test_summary_recover() {
        println!("async run test_summary_recover.");
        let base_dir = "/tmp/test/summary/test_summary_recover";
        let _ = std::fs::remove_dir_all(base_dir);
        std::fs::create_dir_all(base_dir).unwrap();
        let tskv = new_tskv(base_dir.to_string());

        let runtime = tskv.get_ctx().runtime.clone();
        runtime.block_on(async move {
            let client = tskv.get_ctx().meta.tenant_meta("cnosdb").await.unwrap();
            let schema = DatabaseSchema::new(
                "cnosdb",
                "hello",
                DatabaseOptions::default(),
                Arc::new(DatabaseConfig::default()),
            );
            client.create_db(schema).await.unwrap();

            tskv.open_tsfamily("cnosdb", "hello", 100)
                .await
                .expect("add tsfamily successfully");

            let summary_file = summary_file(&tskv, "cnosdb", "hello", 100);
            let summary = Summary::new(100, tskv.get_ctx(), summary_file)
                .await
                .unwrap();
            let database = tskv.get_db("cnosdb", "hello").await.unwrap();
            let db_schema = database.read().await.get_schema().await.unwrap();
            let version = summary.recover(&db_schema).await.unwrap().unwrap();
            assert_eq!(version.owner().as_str(), "cnosdb.hello");
            assert_eq!(version.tf_id(), 100);
        });
    }

    fn test_summary_write_recover() {
        println!("async run test_recover_summary_with_roll.");
        let base_dir = "/tmp/test/summary/test_recover_summary_with_roll";
        let _ = std::fs::remove_dir_all(base_dir);
        std::fs::create_dir_all(base_dir).unwrap();
        let tskv = new_tskv(base_dir.to_string());

        // Create database, add some vnodes and delete some vnodes..
        const VNODE_ID: u32 = 10;
        let tenant = "cnosdb";
        let database = "test_recover_summary_with_roll";
        let runtime = tskv.get_ctx().runtime.clone();
        runtime.block_on(async {
            let client = tskv.get_ctx().meta.tenant_meta("cnosdb").await.unwrap();
            let schema = DatabaseSchema::new(
                tenant,
                database,
                DatabaseOptions::default(),
                Arc::new(DatabaseConfig::default()),
            );
            client.create_db(schema).await.unwrap();

            let vnode = tskv
                .open_tsfamily(tenant, database, VNODE_ID)
                .await
                .expect("add tsfamily successfully");

            let owner = make_owner(tenant, database);
            let mut edit = VersionEdit::new_update_vnode(VNODE_ID, owner, 100);
            let meta = CompactMeta {
                file_id: 15,
                is_delta: false,
                file_size: 100,
                level: 1,
                min_ts: 1,
                max_ts: 1,
                tsf_id: VNODE_ID,
            };
            edit.add_file(meta, 1);

            let request = SummaryRequest {
                version_edit: edit,
                ts_family: vnode.ts_family(),
                mem_caches: None,
                file_metas: None,
            };

            let summary_file = summary_file(&tskv, tenant, database, VNODE_ID);
            let mut summary = Summary::new(VNODE_ID, tskv.get_ctx(), &summary_file)
                .await
                .unwrap();
            summary.apply_version_edit(&request).await.unwrap();

            let database = tskv.get_db("cnosdb", database).await.unwrap();
            let db_schema = database.read().await.get_schema().await.unwrap();
            let recover_version = summary.recover(&db_schema).await.unwrap().unwrap();

            assert_eq!(summary.next_file_id(), 16);
            assert_eq!(recover_version.last_seq(), 100);
            assert_eq!(recover_version.levels_info()[1].tsf_id, VNODE_ID);
            assert!(!recover_version.levels_info()[1].files[0].is_delta());
            assert_eq!(recover_version.levels_info()[1].files[0].file_id(), 15);
            assert_eq!(recover_version.levels_info()[1].files[0].size(), 100);

            let tsf_version = vnode.ts_family().read().await.version();
            assert_eq!(tsf_version.last_seq(), 100);
            assert_eq!(tsf_version.levels_info()[1].tsf_id, VNODE_ID);
            assert!(!tsf_version.levels_info()[1].files[0].is_delta());
            assert_eq!(tsf_version.levels_info()[1].files[0].file_id(), 15);
            assert_eq!(tsf_version.levels_info()[1].files[0].size(), 100);

            //---------------rewrite--------------
            let ve = vnode.ts_family().read().await.build_version_edit();
            summary.rewrite_summary_file(&ve).await.unwrap();

            let summary = Summary::new(VNODE_ID, tskv.get_ctx(), &summary_file)
                .await
                .unwrap();
            let recover_version = summary.recover(&db_schema).await.unwrap().unwrap();
            assert_eq!(summary.next_file_id(), 16);
            assert_eq!(recover_version.last_seq(), 100);
            assert_eq!(recover_version.levels_info()[1].tsf_id, VNODE_ID);
            assert!(!recover_version.levels_info()[1].files[0].is_delta());
            assert_eq!(recover_version.levels_info()[1].files[0].file_id(), 15);
            assert_eq!(recover_version.levels_info()[1].files[0].size(), 100);
        });
    }
}
