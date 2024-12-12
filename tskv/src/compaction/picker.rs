use std::fmt::Debug;
use std::sync::Arc;
use std::u64;

use models::predicate::domain::TimeRange;
use tokio::sync::RwLockWriteGuard;
use trace::{debug, error, info};
use utils::id_generator::IDGenerator;

use super::CompactTask;
use crate::compaction::CompactReq;
use crate::tsfamily::column_file::ColumnFile;
use crate::tsfamily::level_info::LevelInfo;
use crate::tsfamily::version::Version;
use crate::tsm::tombstone::TsmTombstoneCache;
use crate::{LevelId, TskvResult};

pub async fn pick_compaction(
    compact_task: CompactTask,
    version: Arc<Version>,
) -> Option<CompactReq> {
    match &compact_task {
        CompactTask::Normal(_) => {
            LevelCompactionPicker
                .pick_compaction(compact_task, version)
                .await
        }
        CompactTask::Delta(_) => {
            DeltaCompactionPicker::new()
                .pick_compaction(compact_task, version)
                .await
        }
        CompactTask::Manual(_) => {
            DeltaCompactionPicker::new()
                .pick_compaction(compact_task, version)
                .await
        }
    }
}

/// Compaction picker for picking a level from level-1 to level-4, and then
/// pick inner files of the level.
#[derive(Debug)]
struct LevelCompactionPicker;

impl LevelCompactionPicker {
    async fn pick_compaction(
        &self,
        compact_task: CompactTask,
        version: Arc<Version>,
    ) -> Option<CompactReq> {
        // 1. Get TseriesFamily's newest **version**(`Arc<Version>`)
        // 2. Get all level's score, pick LevelInfo with the max score.
        // 3. Get files(`Vec<Arc<ColumnFile>>`) from the picked level, sorted by min_ts(ascending)
        //    and size(ascending), pick ColumnFile until picking_files_size reaches
        //    max_compact_size, remove away the last picked files with overlapped time_range.
        // 4. (Deprecated and skipped): Pick files from level-0.
        // 5. Build CompactReq using **version**, picked level and picked files.

        debug!("Picker(level): version: [ {:?} ]", version.levels_info());

        let storage_opt = version.storage_opt();
        let level_infos = version.levels_info();

        // Pick a level to compact
        let level_start: &LevelInfo;
        let in_level;
        let out_level;
        if let Some((start_lvl, out_lvl)) = self.pick_level(level_infos).await {
            level_start = &level_infos[start_lvl as usize];
            in_level = start_lvl;
            out_level = out_lvl;
        } else {
            debug!("Picker(level): picked no level");
            return None;
        }

        // Pick selected level files.
        if level_start.files.is_empty() {
            return None;
        }

        let mut files = level_start.files.clone();
        files.sort_by(Self::compare_column_file);
        let picking_files: Vec<Arc<ColumnFile>> =
            Self::pick_files(files, storage_opt.max_compact_size).await;
        debug!("Picker(level): Picked files: [ {:?} ]", &picking_files);
        if picking_files.is_empty() {
            return None;
        }

        // Run compaction and send them to the next level, even if picked only 1 file,.
        Some(CompactReq {
            compact_task,
            version,
            files: picking_files,
            in_level,
            out_level,
            out_time_range: TimeRange::all(),
            file_id: IDGenerator::new(u64::MAX),
        })
    }

    /// Weight of file number of a level to be picked.
    fn level_weight_file_num(level: LevelId) -> f64 {
        match level {
            0 => 1.0,
            1 => 0.8,
            2 => 0.4,
            3 => 0.2,
            4 => 0.1,
            _ => 0.0,
        }
    }

    fn compare_column_file(a: &Arc<ColumnFile>, b: &Arc<ColumnFile>) -> std::cmp::Ordering {
        match a.time_range().min_ts.cmp(&b.time_range().min_ts) {
            std::cmp::Ordering::Equal => a.size().cmp(&b.size()),
            ord => ord,
        }
    }

    async fn pick_level(&self, levels: &[LevelInfo]) -> Option<(LevelId, LevelId)> {
        // - Level max_size (level closer to max_size
        //     has more possibility to run compact)
        //   - (level.max_size - level.cur_size) as numerator
        // - Level running compactions (level running compaction
        //     has less possibility to run compact)
        //   - compacting_files as deniminator
        // - Level weight (higher level that file is too large
        //     has less possibility to run compact)
        //   - level_weight as numerator
        // - Level file_count (after all, level has more files
        //     has more possibility to run compact)
        //   - level.files.len() as numerator

        if levels.is_empty() {
            return None;
        }

        // Level score context: Vec<(level, level_size, compacting_files in level, level_weight, level_score)>
        let mut level_scores: Vec<(LevelId, u64, usize, f64, f64)> =
            Vec::with_capacity(levels.len());
        for lvl in levels.iter() {
            // Ignore level 0 (delta files)
            if lvl.level == 0 || lvl.cur_size == 0 || lvl.files.len() <= 1 {
                continue;
            }
            let mut compacting_files = 0_usize;
            for file in lvl.files.iter() {
                if file.is_compacting().await {
                    compacting_files += 1;
                }
            }

            // let level_file_num_weight = (lvl.files.len() - compacting_files) as f64
            //     * Self::level_weight_file_num(lvl.level);
            // let level_remaining_size_weight = lvl.max_size.checked_sub(lvl.cur_size).unwrap_or(1)
            //     as f64
            //     * Self::level_weight_remaining_size(lvl.level);
            // let level_score = 10e6 * (level_file_num_weight / level_remaining_size_weight);

            let level_score: f64 = (lvl.files.len() - compacting_files) as f64
                * Self::level_weight_file_num(lvl.level);

            level_scores.push((lvl.level, lvl.cur_size, compacting_files, 0.0, level_score));
        }

        if level_scores.is_empty() {
            return None;
        }
        level_scores.sort_by(|(_, _, _, _, score_a), (_, _, _, _, score_b)| {
            score_a.partial_cmp(score_b).expect("a NaN score").reverse()
        });

        debug!(
            "Picker(level), level scores: [ {} ]",
            level_scores
                .iter()
                .map(|lc| format!("{{ Level-{}: {} }}", lc.0, lc.4))
                .collect::<Vec<String>>()
                .join(", ")
        );

        level_scores.first().cloned().map(|(level, _, _, _, _)| {
            if level == 4 {
                (level, level)
            } else {
                (level, level + 1)
            }
        })
    }

    async fn pick_files(
        src_files: Vec<Arc<ColumnFile>>,
        max_compact_size: u64,
    ) -> Vec<Arc<ColumnFile>> {
        let mut dst_files = Vec::with_capacity(src_files.len());

        let mut picking_file_size = 0_u64;
        for file in src_files.iter() {
            if !file.mark_compacting().await {
                continue;
            }
            picking_file_size += file.size();
            dst_files.push(file.clone());

            if picking_file_size >= max_compact_size {
                // Picked file size >= max_compact_size, try break picking files.
                break;
            }
        }

        dst_files
    }
}

/// For the given time_range of a file, return recommended level to compact in.
/// If it returns 0, it means that there are no files overlapped with the given
/// time range in levels.
fn advise_out_level(time_range: &TimeRange, levels: &[LevelInfo; 5]) -> LevelId {
    if time_range.min_ts > levels[1].time_range.max_ts || levels[1].time_range.is_none() {
        // If lv-1 is (+∞，-∞), compact to lv-1.
        // If the range is newer than level-1, return 0 to tell the caller that
        // there is no other level overlapped with the range.
        return 0;
    }
    if time_range.min_ts > levels[1].time_range.min_ts {
        // If the range is overlapped with level-1, compact with level-1 files.
        return 1;
    }
    if (time_range.max_ts <= levels[1].time_range.min_ts
        && time_range.min_ts >= levels[3].time_range.max_ts)
        || levels[2].time_range.is_none()
    {
        return 2;
    }
    if (time_range.max_ts <= levels[2].time_range.min_ts
        && time_range.min_ts >= levels[4].time_range.max_ts)
        || levels[3].time_range.is_none()
    {
        return 3;
    }
    if time_range.max_ts <= levels[3].time_range.min_ts || levels[4].time_range.is_none() {
        return 4;
    }

    // The range overlapped with level-4 and other levels, empirically compact to lower(1~4) level.
    for lv in levels[1..].iter() {
        if lv.time_range.overlaps(time_range) {
            return lv.level;
        }
    }

    4
}

#[derive(Debug)]
struct DeltaCompactionPicker {
    timestamp: i64,
}

// todo: get file timerange after remove tombstone file
impl DeltaCompactionPicker {
    fn new() -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    async fn delta_file_first_remained_time_range(
        &self,
        file: &ColumnFile,
    ) -> TskvResult<TimeRange> {
        let tomb_path = file.tombstone_path();
        let tomb_trs = match TsmTombstoneCache::load(tomb_path).await {
            Ok(Some(tomb_cache)) => tomb_cache.all_excluded().clone(),
            Ok(None) => return Ok(*file.time_range()),
            Err(e) => {
                error!(
                    "Picker(delta) [{}]: failed to load tombstone file '{}': {e}",
                    self.timestamp,
                    file.file_path().display()
                );
                return Err(e);
            }
        };
        debug!(
            "Picker(delta): file: {}, all_excluded time ranges: [ {tomb_trs} ]",
            file.file_id(),
        );
        match file.time_range().exclude_time_ranges(&tomb_trs) {
            Some(trs) => {
                if let Some(tr) = trs.time_ranges().next() {
                    Ok(tr)
                } else {
                    Ok(TimeRange::none())
                }
            }
            None => Ok(TimeRange::none()),
        }
    }

    async fn pick_compaction(
        &self,
        compact_task: CompactTask,
        version: Arc<Version>,
    ) -> Option<CompactReq> {
        debug!(
            "Picker(delta) [{}]: version: [ {:?} ]",
            self.timestamp,
            version.levels_info()
        );
        if version.levels_info()[0].files.len()
            < version.storage_opt().compact_trigger_file_num as usize
        {
            info!(
                "Picker(delta) [{}]: picked nothing, level_0 files({}) < {}",
                self.timestamp,
                version.levels_info()[0].files.len(),
                version.storage_opt().compact_trigger_file_num
            );
            return None;
        }

        let lv0 = &version.levels_info()[0];
        let lv14 = &version.levels_info()[1..];
        let mut picked_time_range = TimeRange::none();
        let mut picked_l0_compacting_wlocks = Vec::new();
        let mut picked_l0_files = vec![];
        let mut not_picked_l0_file: Option<(
            Arc<ColumnFile>,        // l0_file
            LevelId,                // adviced_out_level
            TimeRange,              // l0_file_remained_tr
            RwLockWriteGuard<bool>, // l0_file_compacting
        )> = Option::None;
        for l0_file in lv0.files.iter() {
            let mut l0_file_compacting = l0_file.write_lock_compacting().await;
            debug!(
                "Picker(delta) [{}]: Level-0 file {} compacting: {}",
                self.timestamp,
                l0_file.file_id(),
                *l0_file_compacting
            );
            if *l0_file_compacting {
                continue;
            }

            let l0_file_remained_tr_first =
                match self.delta_file_first_remained_time_range(l0_file).await {
                    Ok(trs) => trs,
                    Err(_) => {
                        continue;
                    }
                };

            let advised_out_level =
                advise_out_level(&l0_file_remained_tr_first, version.levels_info());
            if picked_time_range.is_none() {
                // First cycle to pick l0_file
                picked_time_range = l0_file_remained_tr_first;
            }
            // For well ordered lv0-files, merged to level 1
            if 0 == advised_out_level {
                *l0_file_compacting = true;
                picked_l0_compacting_wlocks.push(l0_file_compacting);
                picked_l0_files.push(l0_file.clone());
                picked_time_range.merge(&l0_file_remained_tr_first);
                if picked_l0_files.len() >= version.storage_opt().compact_trigger_file_num as usize
                {
                    info!(
                        "Picker(delta) [{}]: picked level_0 files({:?}) to level: 1",
                        self.timestamp,
                        &picked_l0_files
                            .iter()
                            .map(|file| file.file_id())
                            .collect::<Vec<_>>()
                    );
                    return Some(CompactReq {
                        compact_task,
                        version: version.clone(),
                        files: picked_l0_files,
                        in_level: 0,
                        out_level: 1,
                        out_time_range: picked_time_range,
                        file_id: IDGenerator::new(u64::MAX),
                    });
                }
                continue;
            }
            // Release the previous picked l0_files.
            for mut wlock in std::mem::take(&mut picked_l0_compacting_wlocks) {
                *wlock = false;
            }
            picked_l0_files.clear();
            picked_time_range = TimeRange::none();

            if (advised_out_level as usize) < version.levels_info().len() {
                if not_picked_l0_file.is_none() {
                    not_picked_l0_file = Some((
                        l0_file.clone(),
                        advised_out_level,
                        l0_file_remained_tr_first,
                        l0_file_compacting,
                    ));
                } else {
                    trace::debug!(
                        "Picker(delta) [{}]: lv0 file overlapped, scan next",
                        self.timestamp
                    );
                    // break;
                }
            } else {
                trace::error!(
                    "Picker(delta) [{}]: advised_out_level({advised_out_level}) is out of range",
                    self.timestamp,
                );
            }
        }

        // Release the previous picked l0_files.
        for mut wlock in picked_l0_compacting_wlocks {
            *wlock = false;
        }

        if let Some((
            l0_file,
            advised_out_level,
            l0_file_remained_tr_first,
            mut l0_file_compacting,
        )) = not_picked_l0_file
        {
            // Find the first file in level1-4 that overlaps with lv0-file
            for lv in lv14 {
                if lv.time_range.overlaps(&l0_file_remained_tr_first) {
                    for lv_file in lv.files.iter() {
                        let mut lv_file_compacting = lv_file.write_lock_compacting().await;
                        if *lv_file_compacting {
                            continue;
                        }
                        if lv_file.time_range().overlaps(&l0_file_remained_tr_first) {
                            *lv_file_compacting = true;
                            *l0_file_compacting = true;
                            info!("Picker(delta) [{}]: picked two level files: level_0 file({l0_file}), level file: {lv_file} to level: {}", self.timestamp, lv.level());
                            return Some(CompactReq {
                                compact_task,
                                version: version.clone(),
                                files: vec![l0_file.clone(), lv_file.clone()],
                                in_level: 0,
                                out_level: lv.level(),
                                // One delta-file and one level-file, the out_time_range is
                                // the time range of the level-file.
                                out_time_range: *lv_file.time_range(),
                                file_id: IDGenerator::new(u64::MAX),
                            });
                        }
                    }
                    // No file in the out-level overlaps with lv0-file, compact lv0-file to advised out-level.
                    if let Some(out_time_range) =
                        l0_file_remained_tr_first.intersect(&lv.time_range)
                    {
                        return Some(CompactReq {
                            compact_task,
                            version: version.clone(),
                            files: vec![l0_file.clone()],
                            in_level: 0,
                            out_level: lv.level(),
                            out_time_range,
                            file_id: IDGenerator::new(u64::MAX),
                        });
                    }
                }
            }

            // No file in level1-4 overlaps with lv0-file, compact lv0-file to advised out-level.
            if advised_out_level > 1 {
                *l0_file_compacting = true;
                info!("Picker(delta) [{}]: picked level_0 file: {l0_file} to level: {advised_out_level}", self.timestamp,);
                return Some(CompactReq {
                    compact_task,
                    version: version.clone(),
                    files: vec![l0_file.clone()],
                    in_level: 0,
                    out_level: advised_out_level,
                    out_time_range: l0_file_remained_tr_first,
                    file_id: IDGenerator::new(u64::MAX),
                });
            }
        }

        info!(
            "Picker(delta) [{}]: picked nothing, level_0 files({:?})",
            self.timestamp,
            &picked_l0_files
                .iter()
                .map(|file| file.file_id())
                .collect::<Vec<_>>()
        );
        None
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use models::predicate::domain::TimeRange;

    use super::advise_out_level;
    use crate::compaction::picker::{DeltaCompactionPicker, LevelCompactionPicker};
    use crate::compaction::test::{FileSketch, VersionSketch};
    use crate::compaction::{create_options, CompactTask};

    #[tokio::test]
    async fn test_pick_normal_compaction() {
        let dir = "/tmp/test/pick/normal_compaction";
        let _ = std::fs::remove_dir_all(dir);
        // Some files in Level 1 will be picked and compact to Level 2.
        let opt = create_options(dir.to_string(), 1);

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(11, (1, 1000), 1000, false))
            .add(0, FileSketch(12, (33010, 34000), 1000, false))
            .add(1, FileSketch(7, (34001, 35000), 1000, false))
            .add(1, FileSketch(8, (35001, 36000), 1000, false))
            .add(1, FileSketch(9, (34501, 35500), 1000, true))
            .add(1, FileSketch(10, (35001, 36000), 1000, true))
            .add(2, FileSketch(5, (30001, 32000), 1000, false))
            .add(2, FileSketch(6, (32001, 34000), 1000, false))
            .add(3, FileSketch(3, (20001, 25000), 1000, false))
            .add(3, FileSketch(4, (25001, 30000), 1000, false))
            .add(4, FileSketch(1, (1, 10000), 1000, false))
            .add(4, FileSketch(2, (10001, 20000), 1000, false))
            .to_version(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Normal(0);
        let compact_req = LevelCompactionPicker
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        assert_eq!(compact_req.files.len(), 2);
        assert_eq!(compact_req.out_level, 2);
    }

    /// Test picker for delta compaction that all delta files could be merged into level-1.
    #[tokio::test]
    async fn test_pick_delta_compaction_with_tsm_1() {
        let dir = "/tmp/test/pick/delta_compaction_with_tsm_1";
        let _ = std::fs::remove_dir_all(dir);
        let opt = create_options(dir.to_string(), 4);

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(11, (901, 1000), 100, false)) // adviced_level: 0
            .add_t(0, FileSketch(12, (1, 600), 100, false), (401, 500)) // remained (1, 400), adviced_level: 3
            .add(0, FileSketch(13, (902, 950), 100, false)) // adviced_level: 0
            .add(0, FileSketch(14, (951, 980), 10, false)) // adviced_level: 0
            .add(0, FileSketch(15, (961, 990), 10, false)) // adviced_level: 0
            .add(0, FileSketch(16, (100, 600), 10, false)) // remained (100, 600), adviced_level: 2
            .add(0, FileSketch(17, (961, 990), 10, false)) // adviced_level: 0
            .add(0, FileSketch(18, (902, 950), 100, false)) // adviced_level: 0
            .add(0, FileSketch(19, (951, 980), 10, false)) // adviced_level: 0
            .add(0, FileSketch(20, (961, 990), 10, false)) // adviced_level: 0
            // lv1, (601, 800)
            .add(1, FileSketch(7, (601, 650), 100, false))
            .add(1, FileSketch(8, (651, 700), 100, false))
            .add(1, FileSketch(9, (701, 750), 100, false))
            .add(1, FileSketch(10, (751, 800), 100, false))
            // lv2, (401, 600)
            .add(2, FileSketch(5, (401, 500), 200, false))
            .add(2, FileSketch(6, (501, 600), 200, false))
            // lv3, (201, 400)
            .add(3, FileSketch(3, (201, 300), 300, false))
            .add(3, FileSketch(4, (301, 400), 300, false))
            // lv4, (1, 200)
            .add(4, FileSketch(1, (1, 100), 400, false))
            .add(4, FileSketch(2, (101, 200), 400, false))
            .to_version_with_tsm(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Delta(0);
        let compact_req = DeltaCompactionPicker::new()
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        let (lv0_files, lv14_file) = compact_req.split_delta_and_level_files();
        assert!(lv14_file.is_none());
        assert_eq!(lv0_files.len(), 4);
        assert_eq!(lv0_files[0].file_id(), 17);
        assert_eq!(lv0_files[1].file_id(), 18);
        assert_eq!(lv0_files[2].file_id(), 19);
        assert_eq!(lv0_files[3].file_id(), 20);
        assert_eq!(compact_req.out_level, 1);
        assert_eq!(compact_req.out_time_range, (902, 990).into());
    }

    /// Test picker for delta compaction that tsm files overlaps with some delta files.
    #[tokio::test]
    async fn test_pick_delta_compaction_with_tsm_2() {
        let dir = "/tmp/test/pick/delta_compaction_with_tsm_2";
        let _ = std::fs::remove_dir_all(dir);
        let opt = create_options(dir.to_string(), 1);

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            // remained (1, 400), adviced_level: 3
            .add_t(0, FileSketch(11, (1, 600), 100, false), (401, 500))
            // remained (100, 600), adviced_level: 2
            .add(0, FileSketch(12, (100, 600), 10, false))
            // remained (301, 400), adviced_level: 3
            .add_t(0, FileSketch(13, (301, 500), 10, false), (401, 500))
            // remained (1, 500), adviced_level: 2
            .add(0, FileSketch(14, (1, 500), 10, false))
            // lv1, (601, 800)
            .add(1, FileSketch(7, (601, 650), 100, false))
            .add(1, FileSketch(8, (651, 700), 100, false))
            .add(1, FileSketch(9, (701, 750), 100, false))
            .add(1, FileSketch(10, (751, 800), 100, false))
            // lv2, (401, 600)
            .add(2, FileSketch(5, (401, 500), 200, false))
            .add(2, FileSketch(6, (501, 600), 200, false))
            // lv3, (201, 400)
            .add(3, FileSketch(3, (201, 300), 300, false))
            .add(3, FileSketch(4, (301, 400), 300, false))
            // lv4, (1, 200)
            .add(4, FileSketch(1, (1, 100), 400, false))
            .add(4, FileSketch(2, (101, 200), 400, false))
            .to_version_with_tsm(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Delta(0);
        let compact_req = DeltaCompactionPicker::new()
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        let (lv0_files, lv14_file) = compact_req.split_delta_and_level_files();
        assert!(lv14_file.is_some());
        assert_eq!(lv14_file.unwrap().file_id(), 3);
        assert_eq!(lv0_files.len(), 1);
        assert_eq!(lv0_files[0].file_id(), 11);
        assert_eq!(compact_req.out_level, 3);
        assert_eq!(compact_req.out_time_range, (201, 300).into());
    }

    /// Test picker for delta compaction that there are only level-0 and level-1 but
    /// one delta files is earlier than level-1.
    #[tokio::test]
    async fn test_pick_delta_compaction_with_tsm_3() {
        let dir = "/tmp/test/pick/delta_compaction_with_tsm_3";
        let _ = std::fs::remove_dir_all(dir);
        let opt = create_options(dir.to_string(), 1);

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            // remained (1, 400), adviced_level: 2
            .add_t(0, FileSketch(11, (1, 600), 100, false), (401, 500))
            .add(0, FileSketch(12, (100, 600), 10, false))
            .add(1, FileSketch(7, (601, 650), 100, false))
            .add(1, FileSketch(8, (651, 700), 100, false))
            .add(1, FileSketch(9, (701, 750), 100, false))
            .add(1, FileSketch(10, (751, 800), 100, false))
            .to_version_with_tsm(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Delta(0);
        let compact_req = DeltaCompactionPicker::new()
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        let (lv0_files, lv14_file) = compact_req.split_delta_and_level_files();
        assert!(lv14_file.is_none());
        assert_eq!(lv0_files.len(), 1);
        assert_eq!(lv0_files[0].file_id(), 11);
        assert_eq!(compact_req.out_level, 2);
        assert_eq!(compact_req.out_time_range, (1, 400).into());
    }

    #[tokio::test]
    async fn test_advise_out_level() {
        {
            let dir = "/tmp/test/pick/test_adviced_out_level/1";
            let _ = std::fs::remove_dir_all(dir);
            let opt = create_options(dir.to_string(), 1);

            let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
                .add(1, FileSketch(1, (701, 800), 100, false)) // Level#1, 701~800
                .add(2, FileSketch(2, (401, 600), 200, false)) // Level#2, 401~600
                .add(3, FileSketch(3, (201, 400), 300, false)) // level#3, 201, 400
                .add(4, FileSketch(4, (1, 200), 400, false)) // level#4, 1~200
                .to_version_with_tsm(opt.storage.clone())
                .await;
            let levels = version.levels_info();

            assert_eq!(advise_out_level(&TimeRange::new(801, 802), levels), 0);
            assert_eq!(advise_out_level(&TimeRange::new(800, 801), levels), 1);
            assert_eq!(advise_out_level(&TimeRange::new(700, 701), levels), 2);
            assert_eq!(advise_out_level(&TimeRange::new(601, 602), levels), 2);
            assert_eq!(advise_out_level(&TimeRange::new(-1, 0), levels), 4);
        }
        {
            let dir = "/tmp/test/pick/test_adviced_out_level/2";
            let _ = std::fs::remove_dir_all(dir);
            let opt = create_options(dir.to_string(), 1);

            let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
                .add(1, FileSketch(1, (701, 800), 100, false)) // Level#1, 701~800
                .to_version_with_tsm(opt.storage.clone())
                .await;
            let levels = version.levels_info();

            assert_eq!(advise_out_level(&TimeRange::new(801, 802), levels), 0);
            assert_eq!(advise_out_level(&TimeRange::new(800, 801), levels), 1);
            assert_eq!(advise_out_level(&TimeRange::new(700, 701), levels), 2);
            assert_eq!(advise_out_level(&TimeRange::new(601, 602), levels), 2);
            assert_eq!(advise_out_level(&TimeRange::new(-1, 0), levels), 2);
        }
        {
            let dir = "/tmp/test/pick/test_adviced_out_level/3";
            let _ = std::fs::remove_dir_all(dir);
            let opt = create_options(dir.to_string(), 1);

            let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
                .to_version_with_tsm(opt.storage.clone())
                .await;
            let levels = version.levels_info();

            assert_eq!(advise_out_level(&TimeRange::new(801, 802), levels), 0);
        }
    }

    /// Test picker for delta compaction that tsm files doesn't overlap with any delta file.
    #[tokio::test]
    async fn test_pick_delta_compaction_without_tsm_1() {
        let dir = "/tmp/test/pick/delta_compaction_without_tsm_1";
        let _ = std::fs::remove_dir_all(dir);
        let opt = create_options(dir.to_string(), 1);

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(9, (601, 650), 10, false)) // Not overlaps with any lv1-4 files, but time_range between lv-1 and lv-3, merge to level_2.
            .add(0, FileSketch(10, (651, 700), 10, false)) // Only merge one lv-0 file in one time.
            .add(1, FileSketch(7, (701, 750), 100, false)) // Level#1, 701~800
            .add(1, FileSketch(8, (751, 800), 100, false))
            .add(2, FileSketch(5, (401, 500), 200, false)) // Level#2, 401~600
            .add(2, FileSketch(6, (501, 600), 200, false))
            .add(3, FileSketch(3, (201, 300), 300, false)) // level#3, 201, 400
            .add(3, FileSketch(4, (301, 400), 300, false))
            .add(4, FileSketch(1, (1, 100), 400, false)) // level#4, 1~200
            .add(4, FileSketch(2, (101, 200), 400, false))
            .to_version_with_tsm(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Delta(0);
        let compact_req = DeltaCompactionPicker::new()
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        let (lv0_files, lv14_file) = compact_req.split_delta_and_level_files();
        assert!(lv14_file.is_none());
        assert_eq!(lv0_files.len(), 1);
        assert_eq!(lv0_files[0].file_id(), 9);
        assert_eq!(compact_req.out_level, 2);
        assert_eq!(compact_req.out_time_range, (601, 650).into());
    }

    /// Test picker for delta compaction that tsm files doesn't overlap with any delta file.
    #[tokio::test]
    async fn test_pick_delta_compaction_without_tsm_2() {
        let dir = "/tmp/test/pick/delta_compaction_without_tsm_2";
        let _ = std::fs::remove_dir_all(dir);
        let opt = create_options(dir.to_string(), 1);

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(9, (-100, -1), 10, false)) // Not overlaps with any lv1-4 files, but time_range before lv-4, merge to level_4.
            .add(0, FileSketch(10, (-100, -1), 10, false))
            .add(1, FileSketch(7, (701, 750), 100, false)) // Level#1, 701~800
            .add(1, FileSketch(8, (751, 800), 100, false))
            .add(2, FileSketch(5, (401, 500), 200, false)) // Level#2, 401~600
            .add(2, FileSketch(6, (501, 600), 200, false))
            .add(3, FileSketch(3, (201, 300), 300, false)) // level#3, 201, 400
            .add(3, FileSketch(4, (301, 400), 300, false))
            .add(4, FileSketch(1, (1, 100), 400, false)) // level#4, 1~200
            .add(4, FileSketch(2, (101, 200), 400, false))
            .to_version_with_tsm(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Delta(0);
        let compact_req = DeltaCompactionPicker::new()
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        let (lv0_files, lv14_file) = compact_req.split_delta_and_level_files();
        assert!(lv14_file.is_none());
        assert_eq!(lv0_files.len(), 1);
        assert_eq!(lv0_files[0].file_id(), 9);
        assert_eq!(compact_req.out_level, 4);
        assert_eq!(compact_req.out_time_range, (-100, -1).into());
    }
}
