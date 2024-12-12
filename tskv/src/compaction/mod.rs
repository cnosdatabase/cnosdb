pub mod check;
mod comapcting_block_meta_group;
mod compact;
mod compacting_block_meta;
mod flush;
pub mod job;
pub mod metrics;
mod picker;
mod utils;
mod writer_wrapper;

use std::collections::HashMap;
use std::sync::Arc;

use ::utils::id_generator::IDGenerator;
use ::utils::BloomFilter;
#[cfg(test)]
pub use compact::test::create_options;
pub use compact::*;
use metrics::FlushMetrics;
use models::predicate::domain::TimeRange;
pub use picker::*;
use tokio::sync::RwLock;

use crate::compaction::metrics::VnodeCompactionMetrics;
use crate::index::ts_index::TSIndex;
use crate::tsfamily::column_file::ColumnFile;
use crate::tsfamily::tseries_family::TseriesFamily;
use crate::tsfamily::version::Version;
use crate::{ColumnFileId, LevelId, TskvResult, VersionEdit, VnodeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactTask {
    /// Compact the files in the in_level into the out_level.
    Normal(VnodeId),
    /// Compact the files in level-0 to larger files.
    Delta(VnodeId),
    /// Triggers compaction manually.
    Manual(VnodeId),
}

impl CompactTask {
    pub fn vnode_id(&self) -> VnodeId {
        match self {
            CompactTask::Normal(vnode_id) => *vnode_id,
            CompactTask::Delta(vnode_id) => *vnode_id,
            CompactTask::Manual(vnode_id) => *vnode_id,
        }
    }

    fn priority(&self) -> usize {
        match self {
            CompactTask::Manual(_) => 0,
            CompactTask::Delta(_) => 1,
            CompactTask::Normal(_) => 2,
        }
    }
}

impl Ord for CompactTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority().cmp(&other.priority())
    }
}

impl PartialOrd for CompactTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for CompactTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactTask::Normal(vnode_id) => write!(f, "Normal({})", vnode_id),
            CompactTask::Delta(vnode_id) => write!(f, "Delta({})", vnode_id),
            CompactTask::Manual(vnode_id) => write!(f, "Manual({})", vnode_id),
        }
    }
}

pub struct CompactReq {
    compact_task: CompactTask,

    file_id: IDGenerator,
    version: Arc<Version>,
    files: Vec<Arc<ColumnFile>>,
    in_level: LevelId,
    out_level: LevelId,
    out_time_range: TimeRange,
}

impl CompactReq {
    /// Split the `files` into delta files and an optional level-1~4 file. Only for delta compaction.
    pub fn split_delta_and_level_files(&self) -> (Vec<Arc<ColumnFile>>, Option<Arc<ColumnFile>>) {
        debug_assert!(self.in_level == 0);
        let (mut delta_files, mut level_files) = (vec![], Option::None);
        for f in self.files.iter() {
            if f.is_delta() {
                delta_files.push(f.clone());
            } else if level_files.is_none() {
                level_files = Some(f.clone());
            }
        }
        (delta_files, level_files)
    }

    pub fn set_file_id(&mut self, file_id: IDGenerator) {
        self.file_id = file_id;
    }
}

impl std::fmt::Display for CompactReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_database: {}, ts_family: {}, in_level: {}, out_level: {:?}, out_time_range: {}, files: [",
            self.version.owner(),
            self.version.tf_id(),
            self.in_level,
            self.out_level,
            self.out_time_range,
        )?;
        if !self.files.is_empty() {
            write!(
                f,
                "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                self.files[0].level(),
                self.files[0].file_id(),
                self.files[0].time_range().min_ts,
                self.files[0].time_range().max_ts
            )?;
            for file in self.files.iter().skip(1) {
                write!(
                    f,
                    ", {{ Level-{}, file_id: {}, time_range: {}-{} }}",
                    file.level(),
                    file.file_id(),
                    file.time_range().min_ts,
                    file.time_range().max_ts
                )?;
            }
        }
        write!(f, "]")
    }
}

pub async fn run_compaction_job(
    request: CompactReq,
    metrics: VnodeCompactionMetrics,
) -> TskvResult<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    if request.in_level == 0 {
        run_delta_compaction_job(request, metrics).await
    } else {
        run_normal_compaction_job(request, metrics).await
    }
}

#[derive(Clone)]
pub struct FlushReq {
    pub tf_id: VnodeId,
    pub owner: String,
    pub completion: bool,
    pub trigger_compact: bool,
    pub ts_index: Arc<RwLock<TSIndex>>,
    pub ts_family: Arc<RwLock<TseriesFamily>>,

    pub flush_metrics: Arc<RwLock<FlushMetrics>>,
}

impl std::fmt::Display for FlushReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlushReq owner: {}, on vnode: {}",
            self.owner, self.tf_id,
        )
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use cache::ShardedAsyncCache;
    use models::codec::Encoding;
    use models::predicate::domain::TimeRange;
    use utils::id_generator::IDGenerator;

    use crate::compaction::compact::test::create_options;
    use crate::compaction::{CompactReq, CompactTask};
    use crate::file_utils::{make_delta_file, make_tsm_file};
    use crate::kv_option::StorageOptions;
    use crate::tsfamily::column_file::ColumnFile;
    use crate::tsfamily::level_info::LevelInfo;
    use crate::tsfamily::version::Version;
    use crate::tsm::tombstone::{TsmTombstoneCache, TOMBSTONE_BUFFER_SIZE};
    use crate::tsm::writer::TsmWriter;
    use crate::tsm::TOMBSTONE_FILE_SUFFIX;
    use crate::{record_file, ColumnFileId};

    /// The sketch of a version of a vnode.
    #[derive(Debug)]
    pub struct VersionSketch {
        pub id: u32,
        pub dir: PathBuf,
        pub tenant_database: Arc<String>,
        pub levels: [LevelSketch; 5],
        pub tombstone_map: HashMap<u64, TimeRange>,
        pub max_level_ts: i64,
    }

    impl VersionSketch {
        pub fn new<P: AsRef<Path>>(dir: P, tenant_database: Arc<String>, vnode_id: u32) -> Self {
            let levels = [
                LevelSketch(0, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(1, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(2, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(3, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(4, (i64::MAX, i64::MIN), vec![]),
            ];
            Self {
                id: vnode_id,
                dir: dir.as_ref().to_path_buf(),
                tenant_database,
                levels,
                tombstone_map: HashMap::new(),
                max_level_ts: i64::MIN,
            }
        }

        pub fn add(mut self, level: usize, file: FileSketch) -> Self {
            self.max_level_ts = self.max_level_ts.max(file.1 .1);
            let level_sketch = &mut self.levels[level];
            level_sketch.1 .0 = level_sketch.1 .0.min(file.1 .0);
            level_sketch.1 .1 = level_sketch.1 .1.max(file.1 .1);
            level_sketch.2.push(file);
            self
        }

        pub fn add_t(
            mut self,
            level: usize,
            file: FileSketch,
            tomb_all_excluded: (i64, i64),
        ) -> Self {
            self.tombstone_map.insert(file.0, tomb_all_excluded.into());
            self = self.add(level, file);
            self
        }

        async fn to_column_files(
            &self,
            storage_opt: &StorageOptions,
            files: &mut Vec<Arc<ColumnFile>>,
            mut filter: impl FnMut(&LevelSketch, &FileSketch) -> bool,
        ) {
            for level_sketch in self.levels.iter() {
                let level_dir = if level_sketch.0 == 0 {
                    storage_opt.delta_dir(self.tenant_database.as_str(), self.id)
                } else {
                    storage_opt.tsm_dir(self.tenant_database.as_str(), self.id)
                };
                for file_sketch in level_sketch.2.iter() {
                    if filter(level_sketch, file_sketch) {
                        let file = file_sketch.to_column_file(&level_dir, level_sketch.0).await;
                        files.push(Arc::new(file));
                    }
                }
            }
        }

        pub async fn to_version(&self, storage_opt: Arc<StorageOptions>) -> Version {
            let mut level_infos =
                LevelInfo::init_levels(self.tenant_database.clone(), self.id, storage_opt.clone());
            for (level, level_sketch) in self.levels.iter().enumerate() {
                let level_dir = if level == 0 {
                    storage_opt.delta_dir(self.tenant_database.as_str(), self.id)
                } else {
                    storage_opt.tsm_dir(self.tenant_database.as_str(), self.id)
                };
                level_sketch
                    .to_level_info(&mut level_infos[level], &level_dir, level as u32)
                    .await;
            }

            Version::new(
                self.id,
                self.tenant_database.clone(),
                storage_opt,
                1,
                level_infos,
                self.max_level_ts,
                Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
            )
        }

        pub async fn to_version_with_tsm(&self, storage_opt: Arc<StorageOptions>) -> Version {
            let version = self.to_version(storage_opt).await;
            self.make_tsm_files(&version).await;
            version
        }

        async fn make_tsm_files(&self, version: &Version) {
            let tsm_dir = version
                .storage_opt()
                .tsm_dir(self.tenant_database.as_str(), self.id);
            let delta_dir = version
                .storage_opt()
                .delta_dir(self.tenant_database.as_str(), self.id);
            let _ = std::fs::remove_dir_all(&tsm_dir);
            let _ = std::fs::remove_dir_all(&delta_dir);

            for level_sketch in self.levels.iter() {
                for file_sketch in level_sketch.2.iter() {
                    let tsm_path = if level_sketch.0 == 0 {
                        make_delta_file(&delta_dir, file_sketch.0)
                    } else {
                        make_tsm_file(&tsm_dir, file_sketch.0)
                    };
                    let mut writer = TsmWriter::open(
                        &tsm_dir,
                        file_sketch.0,
                        0,
                        level_sketch.0 == 0,
                        Encoding::Zstd,
                    )
                    .await
                    .unwrap();
                    writer.finish().await.unwrap();

                    if let Some(tr) = self.tombstone_map.get(&file_sketch.0) {
                        let tombstone_path = tsm_path.with_extension(TOMBSTONE_FILE_SUFFIX);
                        let tomb = TsmTombstoneCache::with_all_excluded(*tr);
                        let mut writer =
                            record_file::Writer::open(tombstone_path, TOMBSTONE_BUFFER_SIZE)
                                .await
                                .unwrap();
                        tomb.save_to(&mut writer).await.unwrap();
                        writer.close().await.unwrap();
                    }
                }
            }
        }
    }

    /// The sketch of a level, contains a tuple of
    /// `level, (min_ts, max_ts), files`.
    #[derive(Debug, Clone)]
    pub struct LevelSketch(pub u32, pub (i64, i64), pub Vec<FileSketch>);

    impl LevelSketch {
        pub async fn to_level_info(
            &self,
            level_info: &mut LevelInfo,
            level_dir: impl AsRef<Path>,
            level: u32,
        ) {
            let mut level_cur_size = 0_u64;
            let mut files = Vec::with_capacity(self.2.len());
            for file_sketch in self.2.iter() {
                level_cur_size += file_sketch.2;
                let file = file_sketch.to_column_file(&level_dir, level).await;
                files.push(Arc::new(file));
            }
            level_info.files = files;
            level_info.cur_size = level_cur_size;
            level_info.time_range = self.1.into();
        }
    }

    /// The sketch of column file, contains `a tuple of
    /// file_id, (min_ts, max_ts), size, being_compact`.
    #[derive(Debug, Clone)]
    pub struct FileSketch(pub u64, pub (i64, i64), pub u64, pub bool);

    impl FileSketch {
        async fn to_column_file(&self, file_dir: impl AsRef<Path>, level: u32) -> ColumnFile {
            let path = if level == 0 {
                make_delta_file(file_dir, self.0)
            } else {
                make_tsm_file(file_dir, self.0)
            };
            let col = ColumnFile::new(self.0, level, self.1.into(), self.2, path);
            if self.3 {
                col.mark_compacting().await;
            }
            col
        }
    }

    #[tokio::test]
    async fn test_generate_version() {
        let dir = "/tmp/test/compaction/test_generate_version";
        let storage_opt = create_options(dir.to_string(), 1).storage.clone();
        let vnode_sketch = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(6, (1, 10), 50, false))
            .add(0, FileSketch(7, (790, 800), 50, false))
            .add(1, FileSketch(1, (701, 800), 100, false))
            .add(1, FileSketch(2, (601, 700), 100, false))
            .add(2, FileSketch(3, (501, 600), 200, false))
            .add(3, FileSketch(4, (301, 500), 300, false))
            .add(4, FileSketch(5, (1, 300), 400, false));
        assert_eq!(vnode_sketch.id, 1);
        assert_eq!(vnode_sketch.tenant_database.as_str(), "dba");
        assert_eq!(vnode_sketch.max_level_ts, 800);
        let levels_sketch = vnode_sketch.levels.clone();
        assert_eq!(levels_sketch[0].0, 0);
        assert_eq!(levels_sketch[0].1, (1, 800));
        assert_eq!(levels_sketch[0].2.len(), 2);
        assert_eq!(levels_sketch[1].0, 1);
        assert_eq!(levels_sketch[1].1, (601, 800));
        assert_eq!(levels_sketch[1].2.len(), 2);
        assert_eq!(levels_sketch[2].0, 2);
        assert_eq!(levels_sketch[2].1, (501, 600));
        assert_eq!(levels_sketch[2].2.len(), 1);
        assert_eq!(levels_sketch[3].0, 3);
        assert_eq!(levels_sketch[3].1, (301, 500));
        assert_eq!(levels_sketch[3].2.len(), 1);
        assert_eq!(levels_sketch[4].0, 4);
        assert_eq!(levels_sketch[4].1, (1, 300));
        assert_eq!(levels_sketch[4].2.len(), 1);

        let version = vnode_sketch.to_version(storage_opt.clone()).await;
        assert_eq!(version.tf_id(), 1);
        assert_eq!(version.owner().as_str(), "dba");
        assert_eq!(version.levels_info().len(), levels_sketch.len());
        let tsm_dir = storage_opt.tsm_dir("dba", 1);
        let delta_dir = storage_opt.delta_dir("dba", 1);
        for (version_level, level_sketch) in version.levels_info().iter().zip(levels_sketch.iter())
        {
            assert_eq!(version_level.owner.as_str(), "dba");
            assert_eq!(version_level.tsf_id, 1);
            assert_eq!(version_level.level, level_sketch.0);
            assert_eq!(
                version_level.cur_size,
                level_sketch.2.iter().map(|f| f.2).sum::<u64>()
            );
            assert_eq!(version_level.time_range, level_sketch.1.into());
            assert_eq!(version_level.files.len(), level_sketch.2.len());
            for (version_file, file_sketch) in version_level.files.iter().zip(level_sketch.2.iter())
            {
                assert_eq!(version_file.file_id(), file_sketch.0);
                assert_eq!(version_file.level(), level_sketch.0);
                assert_eq!(version_file.time_range(), &(file_sketch.1.into()));
                assert_eq!(version_file.size(), file_sketch.2);
                assert_eq!(version_file.is_compacting().await, file_sketch.3);
                if level_sketch.0 == 0 {
                    assert_eq!(
                        version_file.file_path(),
                        &make_delta_file(&delta_dir, file_sketch.0)
                    );
                } else {
                    assert_eq!(
                        version_file.file_path(),
                        &make_tsm_file(&tsm_dir, file_sketch.0)
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_compact_req_methods_delta_compaction() {
        // This case doesn't need directory to exist.
        let dir = "/tmp/test/compaction/test_compact_req_methods_delta_compaction";
        let opt = create_options(dir.to_string(), 1);

        {
            // Merge delta files to level-1.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (20, 30), 10, false))
                .add(0, FileSketch(2, (30, 40), 10, false))
                .add(1, FileSketch(3, (10, 20), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let files = version.levels_info()[0].files.clone();
            let req = CompactReq {
                file_id: IDGenerator::new(1),
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files,
                in_level: 0,
                out_level: 1,
                out_time_range: (1, 20).into(),
            };

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }

        {
            // Merge delta files to an empty level: level-3.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (1, 9), 10, false))
                .add(0, FileSketch(2, (1, 9), 10, false))
                .add(2, FileSketch(3, (10, 20), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let files = version.levels_info()[0].files.clone();
            let req = CompactReq {
                file_id: IDGenerator::new(1),
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files,
                in_level: 0,
                out_level: 3,
                out_time_range: (1, 9).into(),
            };

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }

        {
            // Merge delta files to level-2 but level-3 is empty.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (9, 20), 10, false))
                .add(0, FileSketch(2, (9, 20), 10, false))
                .add(2, FileSketch(3, (1, 10), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let files = version.levels_info()[0].files.clone();
            let req = CompactReq {
                file_id: IDGenerator::new(1),
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files,
                in_level: 0,
                out_level: 2,
                out_time_range: (11, 20).into(),
            };

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }

        {
            // Merge delta files with level files.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (1, 20), 10, false))
                .add(0, FileSketch(2, (1, 20), 10, false))
                .add(2, FileSketch(3, (1, 10), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                file_id: IDGenerator::new(1),
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 0,
                out_level: 2,
                out_time_range: (1, 10).into(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |_, _| true)
                .await;

            let (mut delta_files_exp, mut level_files_exp) = (vec![], vec![]);
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            version_sketch
                .to_column_files(&opt.storage, &mut level_files_exp, |l, _| l.0 != 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert_eq!(
                level_file.map(|f| f.file_id()),
                Some(level_files_exp[0].file_id()),
            );
        }

        {
            // Merge delta files to level files, not existed level files.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (11, 15), 10, false))
                .add(0, FileSketch(2, (16, 20), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                file_id: IDGenerator::new(1),
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 0,
                out_level: 1,
                out_time_range: (11, 20).into(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |_, _| true)
                .await;

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }

        {
            // Merge delta files to level files, target_level has no level files.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (11, 15), 10, false))
                .add(0, FileSketch(2, (16, 20), 10, false))
                .add(2, FileSketch(3, (1, 5), 5, false))
                .add(2, FileSketch(4, (6, 10), 5, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                file_id: IDGenerator::new(1),
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 0,
                out_level: 1,
                out_time_range: TimeRange::all(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |l, _| l.0 == 0)
                .await;

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }
    }

    #[tokio::test]
    async fn test_compact_req_methods_normal_compaction() {
        // This case doesn't need directory to exist.
        let dir = "/tmp/test/compaction/test_compact_req_methods_normal_compaction";
        let opt = create_options(dir.to_string(), 1);

        {
            // Merge level files to next level.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(1, FileSketch(1, (11, 15), 10, false))
                .add(1, FileSketch(2, (16, 20), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                file_id: IDGenerator::new(1),
                compact_task: CompactTask::Normal(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 1,
                out_level: 2,
                out_time_range: TimeRange::all(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |_, _| true)
                .await;
        }

        {
            // Merge level files to next level.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(1, FileSketch(1, (11, 15), 10, false))
                .add(1, FileSketch(2, (16, 20), 10, false))
                .add(2, FileSketch(3, (1, 5), 5, false))
                .add(2, FileSketch(4, (6, 10), 5, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                file_id: IDGenerator::new(1),
                compact_task: CompactTask::Normal(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 1,
                out_level: 2,
                out_time_range: TimeRange::all(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |l, _| l.0 == 1)
                .await;
            assert_eq!(req.out_time_range, TimeRange::all());
        }
    }
}
