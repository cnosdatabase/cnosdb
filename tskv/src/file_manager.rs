use std::{
    borrow::BorrowMut,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    thread::JoinHandle,
};

use futures::channel::oneshot::{self, Sender};
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use regex::Regex;
use snafu::{ResultExt, Snafu};

use crate::{
    direct_io::{self, make_io_task, run_io_task, AsyncContext, File, IoTask, TaskType},
    Error, Result,
};

lazy_static! {
    static ref WAL_PATTERN: Regex = Regex::new("_.*\\.wal").unwrap();
    static ref SUMARRY_PATTERN: Regex = Regex::new("_.*\\.summary").unwrap();
}

pub struct FileManager {
    file_system: Arc<direct_io::FileSystem>,
    async_rt: Arc<AsyncContext>,
    thread_pool: Mutex<Vec<thread::JoinHandle<()>>>,
}

pub fn get_file_manager() -> &'static FileManager {
    static INSTANCE: OnceCell<FileManager> = OnceCell::new();
    INSTANCE.get_or_init(FileManager::new)
}

impl FileManager {
    pub fn new() -> Self {
        let fs_options = direct_io::Options::default();
        let thread_num = fs_options.get_thread_num();
        let rt = Arc::new(AsyncContext::new(fs_options.get_thread_num()));
        let mut pool = Vec::new();
        for i in 0..thread_num {
            let mrt = rt.clone();
            let h = thread::Builder::new().name("AsyncIOThread_".to_string() + &i.to_string())
                                          .spawn(move || run_io_task(mrt, i))
                                          .unwrap();
            pool.push(h);
        }

        Self { file_system: Arc::new(direct_io::FileSystem::new(&fs_options)),
               async_rt: rt,
               thread_pool: Mutex::new(pool) }
    }

    pub fn open_file_with(&self,
                          path: impl AsRef<Path>,
                          options: &fs::OpenOptions)
                          -> Result<direct_io::File> {
        self.file_system
            .open_with(path, options)
            .map_err(|err| Error::UnableToOpenFile { source: err })
    }

    pub fn open_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        self.file_system.open(path).map_err(|err| Error::UnableToOpenFile { source: err })
    }

    pub fn create_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        self.file_system.create(path).map_err(|err| Error::UnableToOpenFile { source: err })
    }

    pub fn open_create_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        if try_exists(path.as_ref()) { self.open_file(path) } else { self.create_file(path) }
    }

    pub async fn sync_all(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system.sync_all(sync).map_err(|err| Error::UnableToSyncFile { source: err })
    }

    pub async fn sync_data(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system.sync_data(sync).map_err(|err| Error::UnableToSyncFile { source: err })
    }

    pub async fn write_at(&self, file: Arc<direct_io::File>, pos: u64, buf: &mut [u8]) {
        let (cb, rx) = oneshot::channel::<crate::error::Result<usize>>();
        let task = make_io_task(TaskType::FrontWrite, buf.as_mut_ptr(), buf.len(), pos, file, cb);

        self.put_io_task(task).unwrap();

        self.async_rt.try_wakeup();

        let ret = rx.await.unwrap();
    }

    pub async fn read_at(&self, file: direct_io::File, pos: u64, size: u64) {}

    pub fn put_io_task(&self, task: IoTask) -> Result<()> {
        if self.async_rt.is_closed() {
            return Err(Error::Cancel);
        }
        if task.is_pri_high() {
            let _ = self.async_rt.high_op_queue.push(task);
        } else if task.task_type == TaskType::BackRead {
            let _ = self.async_rt.read_queue.push(task);
        } else if task.task_type == TaskType::BackWrite {
            let _ = self.async_rt.write_queue.push(task);
        }
        Ok(())
    }
}

pub fn list_file_names(dir: impl AsRef<Path>) -> Vec<String> {
    let mut list = Vec::new();

    for file_name in walkdir::WalkDir::new(dir).min_depth(1)
                                               .max_depth(1)
                                               .into_iter()
                                               .filter_map(|e| {
                                                   let dir_entry = match e {
                                                       Ok(dir_entry)
                                                           if dir_entry.file_type().is_file() =>
                                                       {
                                                           dir_entry
                                                       },
                                                       _ | Err(_) => {
                                                           return None;
                                                       },
                                                   };
                                                   dir_entry.file_name()
                                                            .to_str()
                                                            .map(|file_name| file_name.to_string())
                                               })
    {
        list.push(file_name);
    }

    list
}

/// Case `std::fs::try_exists` is unstable, so copied the same logic to here
pub fn try_exists(path: impl AsRef<Path>) -> bool {
    match std::fs::metadata(path) {
        Ok(_) => true,
        Err(_) => false,
    }
}

pub fn make_wal_file_name(path: &str, sequence: u64) -> PathBuf {
    let p = format!("{}/_{:05}.wal", path, sequence);
    PathBuf::from(p)
}

pub fn get_summary_file(path: &str, file_no: u64) -> PathBuf {
    let p = format!("{}/_{:06}.summary", path, file_no);
    PathBuf::from(p)
}

pub fn get_max_sequence_file_name(dir: impl AsRef<Path>) -> Option<(PathBuf, u64)> {
    let segments = list_file_names(dir);
    if segments.is_empty() {
        return None;
    }
    let mut max_id = 1;
    let mut max_index = 0;
    for (i, file_name) in segments.iter().enumerate() {
        match get_id_by_file_name(file_name) {
            Ok(id) => {
                if max_id < id {
                    max_id = id;
                    max_index = i;
                }
            },
            Err(_) => continue,
        }
    }
    let max_file_name = segments.get(max_index).unwrap();
    Some((PathBuf::from(max_file_name), max_id))
}

pub fn get_id_by_file_name(file_name: &String) -> Result<u64> {
    if !WAL_PATTERN.is_match(file_name) {
        return Err(Error::InvalidFileName { file_name: file_name.clone() });
    }
    let parts: Vec<&str> = file_name.split('.').collect();
    if parts.len() != 2 {
        Err(Error::InvalidFileName { file_name: file_name.clone() })
    } else {
        parts.first()
             .unwrap()
             .split_at(1)
             .1
             .parse::<u64>()
             .map_err(|err| Error::InvalidFileName { file_name: file_name.clone() })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use futures::channel::oneshot;
    use tokio::runtime::Builder;

    use super::FileManager;
    use crate::{
        direct_io::{make_io_task, FileSync, TaskType},
        file_manager,
    };

    #[test]
    fn test_get_instance() {
        let file_manager_1 = file_manager::get_file_manager();
        println!("0x{:X}", file_manager_1 as *const FileManager as usize);
        let file_manager_2 = file_manager::get_file_manager();
        println!("0x{:X}", file_manager_2 as *const FileManager as usize);
        assert_eq!(file_manager_1 as *const FileManager as usize,
                   file_manager_2 as *const FileManager as usize);

        let file_manager_3 = FileManager::new();
        println!("0x{:X}", &file_manager_3 as *const FileManager as usize);
        assert_ne!(file_manager_1 as *const FileManager as usize,
                   &file_manager_3 as *const FileManager as usize);
    }

    #[tokio::test]
    async fn test_io_task() {
        let file_manager = file_manager::get_file_manager();

        let mut buf = vec![1_u8; 1024];
        let file = file_manager.create_file("./a.hex").unwrap();

        let (cb, rx) = oneshot::channel::<crate::error::Result<usize>>();
        let task =
            make_io_task(TaskType::FrontWrite, buf.as_mut_ptr(), buf.len(), 0, Arc::new(file), cb);

        file_manager.put_io_task(task).unwrap();

        file_manager.async_rt.try_wakeup();

        let ret = rx.await.unwrap();
        file_manager.sync_all(FileSync::Hard).await.unwrap();
        ret.unwrap();
    }
    #[test]
    fn test_file() {
        let file_manager = file_manager::get_file_manager();
        let rt = Builder::new_current_thread()
                                              // let rt = Builder::new_multi_thread()
                                              .enable_all()
                                              .build()
                                              .unwrap();
        rt.block_on(async move {
              let mut buf = vec![1_u8; 1024];
              let file = Arc::new(file_manager.create_file("./test_lyt.log").unwrap());

              file_manager.write_at(file.clone(), 0, &mut buf[..]).await;
          });
    }
}
