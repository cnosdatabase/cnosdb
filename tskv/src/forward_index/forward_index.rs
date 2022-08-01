use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use config::GLOBAL_CONFIG;
use models::{FieldId, FieldInfo, SeriesId, SeriesInfo, ValueType};
use num_traits::ToPrimitive;
use trace::error;

use super::*;
use crate::record_file::{self, Record, RecordFileError, RecordFileResult};

const VERSION: u8 = 1;

struct InMemSeriesInfo {
    id: SeriesId,
    pos: usize,
    field_infos: Vec<InMemFieldInfo>,
}

impl InMemSeriesInfo {
    fn with_series_info(series_info: &SeriesInfo, pos: usize) -> Self {
        Self {
            id: series_info.series_id(),
            pos,
            field_infos: series_info.field_infos().iter().map(|f| f.into()).collect(),
        }
    }
}

struct InMemFieldInfo {
    id: FieldId,
    value_type: ValueType,
}

impl From<&FieldInfo> for InMemFieldInfo {
    fn from(field_info: &FieldInfo) -> Self {
        Self {
            id: field_info.field_id(),
            value_type: field_info.value_type(),
        }
    }
}

pub struct ForwardIndex {
    series_info_set: HashMap<SeriesId, InMemSeriesInfo>,
    record_writer: record_file::Writer,
    record_reader: record_file::Reader,
    file_path: PathBuf,
}

#[repr(u8)]
#[derive(Copy, Clone)]
enum ForwardIndexAction {
    Unknown = 0_u8,
    AddSeriesInfo = 1_u8,
    DelSeriesInfo = 2_u8,
}

impl ForwardIndexAction {
    fn u8_number(&self) -> u8 {
        *self as u8
    }
}

impl From<u8> for ForwardIndexAction {
    fn from(act: u8) -> Self {
        match act {
            0_u8 => ForwardIndexAction::Unknown,
            1_u8 => ForwardIndexAction::AddSeriesInfo,
            2_u8 => ForwardIndexAction::DelSeriesInfo,
            _ => ForwardIndexAction::Unknown,
        }
    }
}

impl ForwardIndex {
    pub fn new(path: &String) -> ForwardIndex {
        ForwardIndex {
            series_info_set: HashMap::new(),
            record_writer: record_file::Writer::new(Path::new(&path)).unwrap(),
            record_reader: record_file::Reader::new(Path::new(&path)).unwrap(),
            file_path: Path::new(&path).to_path_buf(),
        }
    }

    pub async fn add_series_info_if_not_exists(
        &mut self,
        mut series_info: SeriesInfo,
    ) -> ForwardIndexResult<()> {
        // Generate series id
        series_info.finish();
        match self.series_info_set.entry(series_info.series_id()) {
            // an series_info in memory.
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                // Already a series_info here.
                let mut mem_series_info = entry.get_mut();
                for field_info in series_info.field_infos().iter() {
                    let mut flag = false;
                    // 1. Check if specified field_id exists.
                    // 2. Check if all field with same field_id has the same value_type.
                    for old_field_info in &mem_series_info.field_infos {
                        if field_info.field_id() == old_field_info.id {
                            if field_info.value_type() == old_field_info.value_type {
                                flag = true;
                                break;
                            } else {
                                return Err(ForwardIndexError::FieldType);
                            }
                        }
                    }

                    // If specified field_id does not exist, then insert the field_info into
                    // series_info
                    if !flag {
                        // Read a series_info from the ForwardIndex file.
                        let record = self
                            .record_reader
                            .read_one(match mem_series_info.pos.to_usize() {
                                None => {
                                    error!("failed convert series info pos to usize");
                                    0
                                }
                                Some(v) => v,
                            })
                            .await
                            .map_err(|err| ForwardIndexError::ReadFile { source: err })?;
                        if record.data_type != ForwardIndexAction::AddSeriesInfo.u8_number() {
                            return Err(ForwardIndexError::Action);
                        }
                        if record.data_version != VERSION {
                            return Err(ForwardIndexError::Version);
                        }
                        let mut origin_series_info = SeriesInfo::decode(&record.data);
                        origin_series_info.push_field_info(field_info.clone());

                        // Write series_info at the end of ForwardIndex file.
                        let pos = self
                            .record_writer
                            .write_record(
                                VERSION,
                                ForwardIndexAction::AddSeriesInfo.u8_number(),
                                &origin_series_info.encode(),
                            )
                            .await
                            .map_err(|err| ForwardIndexError::WriteFile { source: err })?;
                        mem_series_info.pos = pos as usize;

                        // Put field_info in memory
                        let mem_field_info = InMemFieldInfo::from(field_info);
                        mem_series_info.field_infos.push(mem_field_info);
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                // None series_info here
                let data = series_info.encode();
                // Write series_info at the end of ForwardIndex file.
                let pos = self
                    .record_writer
                    .write_record(
                        VERSION,
                        ForwardIndexAction::AddSeriesInfo.u8_number(),
                        &data,
                    )
                    .await
                    .map_err(|err| ForwardIndexError::WriteFile { source: err })?;
                entry.insert(InMemSeriesInfo::with_series_info(
                    &series_info,
                    pos as usize,
                ));
            }
        }

        Ok(())
    }

    pub async fn close(&mut self) -> ForwardIndexResult<()> {
        self.record_writer
            .close()
            .await
            .map_err(|err| ForwardIndexError::CloseFile { source: err })
    }

    pub fn get_series_info_list(&self, sids: &[SeriesId]) -> Vec<SeriesInfo> {
        todo!()
    }

    pub async fn del_series_info(&mut self, series_id: SeriesId) -> ForwardIndexResult<()> {
        match self.series_info_set.entry(series_id) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                self.record_writer
                    .write_record(
                        VERSION,
                        ForwardIndexAction::DelSeriesInfo.u8_number(),
                        series_id.to_le_bytes().as_ref(),
                    )
                    .await
                    .map_err(|err| ForwardIndexError::WriteFile { source: err })?;
                entry.remove();
                Ok(())
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                // TODO Err()?
                Ok(())
            }
        }
    }

    pub async fn load_cache_file(&mut self) -> RecordFileResult<()> {
        let mut record_reader = record_file::Reader::new(&self.file_path).unwrap();
        while let Ok(record) = record_reader.read_record().await {
            self.handle_record(record).await;
        }
        Ok(())
    }

    async fn handle_record(&mut self, record: Record) {
        if record.data_version != VERSION {
            // TODO warning log
            return;
        }

        match ForwardIndexAction::from(record.data_type) {
            ForwardIndexAction::AddSeriesInfo => {
                let series_info = SeriesInfo::decode(&record.data);
                let series_info = InMemSeriesInfo::with_series_info(&series_info, 0);
                self.series_info_set.insert(series_info.id, series_info);
            }

            ForwardIndexAction::DelSeriesInfo => {
                if record.data.len() != 8 {
                    // TODO warning log
                    return;
                }
                let bytes = match record.data.try_into() {
                    Ok(v) => v,
                    Err(_) => {
                        error!("failed handle record in case data try into");
                        return;
                    }
                };
                let series_id = u64::from_le_bytes(bytes);
                self.series_info_set.remove(&series_id);
            }
            ForwardIndexAction::Unknown => {
                // TODO warning log
            }
        };
    }
}

impl From<&str> for ForwardIndex {
    fn from(path: &str) -> Self {
        ForwardIndex::new(&path.to_string())
    }
}

#[derive(Clone)]
pub struct ForwardIndexConfig {
    pub path: String,
}

impl Default for ForwardIndexConfig {
    fn default() -> Self {
        ForwardIndexConfig {
            path: GLOBAL_CONFIG.forward_index_path.clone(),
        }
    }
}
