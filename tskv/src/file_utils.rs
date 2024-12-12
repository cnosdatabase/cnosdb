use std::path::{Path, PathBuf};

use lazy_static::lazy_static;
use regex::Regex;
use snafu::ResultExt;
use tokio::fs;

use crate::error::{IOSnafu, InvalidFileNameSnafu};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::TskvResult;

lazy_static! {
    static ref SUMMARY_FILE_NAME_PATTERN: Regex = Regex::new(r"summary-\d{6}").unwrap();
    static ref WAL_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.wal").unwrap();
    static ref TSM_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.tsm").unwrap();
    static ref HINTEDOFF_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.hh").unwrap();
    static ref INDEX_BINLOG_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.binlog").unwrap();
}

/// Make a path for summary file by it's directory and id.
pub fn make_summary_file(dir: impl AsRef<Path>, number: u64) -> PathBuf {
    let p = format!("summary-{:06}", number);
    dir.as_ref().join(p)
}

/// Make a path for summary temporary file by it's directory.
pub fn make_summary_file_tmp(dir: impl AsRef<Path>) -> PathBuf {
    let p = "summary.tmp".to_string();
    dir.as_ref().join(p)
}

/// Make a path for summary file by it's directory and id.
pub fn make_tsfamily_summary_file(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().join("summary")
}

/// Check a summary file's name.
pub fn check_summary_file_name(file_name: &str) -> bool {
    SUMMARY_FILE_NAME_PATTERN.is_match(file_name)
}

/// Rename a file, from old path to new path.
pub async fn rename(old_name: impl AsRef<Path>, new_name: impl AsRef<Path>) -> TskvResult<()> {
    fs::create_dir_all(new_name.as_ref().parent().unwrap())
        .await
        .context(IOSnafu)?;
    fs::rename(old_name, new_name).await.context(IOSnafu)
}

/// Get id from a summary file's name.
pub fn get_summary_file_id(file_name: &str) -> TskvResult<u64> {
    if !check_summary_file_name(file_name) {
        return Err(InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "summary file name does not contain an id".to_string(),
        }
        .build());
    }
    let (_, file_number) = file_name.split_at(8);
    file_number.parse::<u64>().map_err(|_| {
        InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "summary file name contains an invalid id".to_string(),
        }
        .build()
    })
}

/// Make a path for index binlog file by it's directory and id.
pub fn make_index_binlog_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.binlog", sequence);
    dir.as_ref().join(p)
}

/// Check a index binlog file's name.
pub fn check_index_binlog_file_name(file_name: &str) -> bool {
    INDEX_BINLOG_FILE_NAME_PATTERN.is_match(file_name)
}

/// Get id from a index binlog file's name.
pub fn get_index_binlog_file_id(file_name: &str) -> TskvResult<u64> {
    if !check_index_binlog_file_name(file_name) {
        return Err(InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "index binlog file name does not contain an id".to_string(),
        }
        .build());
    }
    let file_number = &file_name[1..7];
    file_number.parse::<u64>().map_err(|_| {
        InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "index binlog file name contains an invalid id".to_string(),
        }
        .build()
    })
}

/// Make a path for WAL (write ahead log) file by it's directory and id.
pub fn make_wal_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.wal", sequence);
    dir.as_ref().join(p)
}

/// Check a WAL file's name.
pub fn check_wal_file_name(file_name: &str) -> bool {
    WAL_FILE_NAME_PATTERN.is_match(file_name)
}

/// Get id from a WAL file's name.
pub fn get_wal_file_id(file_name: &str) -> TskvResult<u64> {
    if !check_wal_file_name(file_name) {
        return Err(InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "wal file name does not contain an id".to_string(),
        }
        .build());
    }
    let file_number = &file_name[1..7];
    file_number.parse::<u64>().map_err(|_| {
        InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "wal file name contains an invalid id".to_string(),
        }
        .build()
    })
}

pub fn make_tsm_file_name(sequence: u64) -> String {
    format!("_{:06}.tsm", sequence)
}

/// Make a path for TSM file by it's directory and id.
pub fn make_tsm_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    dir.as_ref().join(make_tsm_file_name(sequence))
}

/// Get id from a TSM file's name.
pub fn get_tsm_file_id_by_path(tsm_path: impl AsRef<Path>) -> TskvResult<u64> {
    let path = tsm_path.as_ref();
    let file_name = path
        .file_name()
        .expect("path must not be '..'")
        .to_str()
        .expect("file name must be UTF-8 string");
    if file_name.len() == 1 {
        return Err(InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "tsm file name contains an invalid id".to_string(),
        }
        .build());
    }
    let start = file_name.find('_').unwrap_or(0_usize) + 1;
    let end = file_name.find('.').unwrap_or(file_name.len());
    let file_number = &file_name[start..end];
    file_number.parse::<u64>().map_err(|_| {
        InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "tsm file name contains an invalid id".to_string(),
        }
        .build()
    })
}

pub fn make_tsm_tombstone_file_name(sequence: u64) -> String {
    format!("_{:06}.tombstone", sequence)
}

/// Make a path for TSM tombstone file by it's directory and id.
pub fn make_tsm_tombstone_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    dir.as_ref().join(make_tsm_tombstone_file_name(sequence))
}

pub fn make_delta_file_name(sequence: u64) -> String {
    format!("_{:06}.delta", sequence)
}

/// Make a path for TSM delta file by it's directory and id.
pub fn make_delta_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    dir.as_ref().join(make_delta_file_name(sequence))
}

/// Get the file's path that has the maximum id of files in a directory.
pub fn get_max_sequence_file_name<F>(
    dir: impl AsRef<Path>,
    get_sequence: F,
) -> Option<(PathBuf, u64)>
where
    F: Fn(&str) -> TskvResult<u64>,
{
    let segments = LocalFileSystem::list_file_names(dir);
    if segments.is_empty() {
        return None;
    }

    let mut max_id = 0;
    let mut max_index = 0;
    let mut is_found = false;
    for (i, file_name) in segments.iter().enumerate() {
        match get_sequence(file_name) {
            Ok(id) => {
                is_found = true;
                if max_id < id {
                    max_id = id;
                    max_index = i;
                }
            }
            Err(_) => continue,
        }
    }

    if !is_found {
        return None;
    }

    let max_file_name = segments.get(max_index).unwrap();
    Some((PathBuf::from(max_file_name), max_id))
}

/* -------------------------------------------------------------------------------------- */

pub fn make_file_name(id: u64, suffix: &str) -> String {
    format!("_{:06}.{}", id, suffix)
}

pub fn make_file_path(dir: impl AsRef<Path>, id: u64, suffix: &str) -> PathBuf {
    dir.as_ref().join(make_file_name(id, suffix))
}

pub fn get_file_id_range(dir: impl AsRef<Path>, suffix: &str) -> Option<(u64, u64)> {
    let file_names = LocalFileSystem::list_file_names(dir);
    if file_names.is_empty() {
        return None;
    }

    let pattern = Regex::new(&(r"_\d{6}\.".to_string() + suffix)).unwrap();
    let get_file_id = |file_name: &str| -> TskvResult<u64> {
        if !pattern.is_match(file_name) {
            return Err(InvalidFileNameSnafu {
                file_name: file_name.to_string(),
                message: "index binlog file name does not contain an id".to_string(),
            }
            .build());
        }

        let file_number = &file_name[1..7];
        file_number.parse::<u64>().map_err(|_| {
            InvalidFileNameSnafu {
                file_name: file_name.to_string(),
                message: "index binlog file name contains an invalid id".to_string(),
            }
            .build()
        })
    };

    let mut max_id = 0;
    let mut min_id = u64::MAX;
    let mut is_found = false;
    for file_name in file_names.iter() {
        if let Ok(id) = get_file_id(file_name) {
            is_found = true;
            if max_id < id {
                max_id = id;
            }

            if min_id > id {
                min_id = id;
            }
        }
    }

    if !is_found {
        return None;
    }

    Some((min_id, max_id))
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::{check_summary_file_name, make_summary_file};
    use crate::file_utils::{
        check_wal_file_name, get_summary_file_id, get_wal_file_id, make_wal_file,
    };

    #[test]
    fn test_get_file_id() {
        let summary_file_name = "summary-000123";
        let summary_file_id = get_summary_file_id(summary_file_name).unwrap();
        dbg!(summary_file_id);
        assert_eq!(summary_file_id, 123);

        let wal_file_name = "_000123.wal";
        let wal_file_id = get_wal_file_id(wal_file_name).unwrap();
        dbg!(wal_file_id);
        assert_eq!(wal_file_id, 123);
    }

    #[test]
    fn test_make_file() {
        let path = PathBuf::from("/tmp/test".to_string());
        {
            let summary_file_path = make_summary_file(&path, 0);
            let summary_file_name = summary_file_path.file_name().unwrap().to_str().unwrap();
            assert!(check_summary_file_name(summary_file_name));
            let summary_file_id = get_summary_file_id(summary_file_name).unwrap();
            assert_eq!(summary_file_id, 0);
        }
        {
            let wal_file_path = make_wal_file(&path, 0);
            let wal_file_name = wal_file_path.file_name().unwrap().to_str().unwrap();
            assert!(check_wal_file_name(wal_file_name));
            let wal_file_id = get_wal_file_id(wal_file_name).unwrap();
            assert_eq!(wal_file_id, 0);
        }
    }
}
