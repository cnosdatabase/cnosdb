use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use models::schema::database_schema::{make_owner, DatabaseSchema};
use tokio::sync::RwLock;

use crate::database::Database;
use crate::error::TskvResult;
use crate::record_file::Reader;
use crate::tsfamily::summary::Summary;
use crate::tsfamily::version::{CompactMeta, VersionEdit, VnodeAction};
use crate::vnode_store::VnodeStorage;
use crate::{file_utils, TsKvContext, TskvError, VnodeId};

pub struct VersionSet {
    ctx: Arc<TsKvContext>,
    dbs: HashMap<String, Arc<RwLock<Database>>>,
    vnodes: HashMap<VnodeId, VnodeStorage>,
}

impl VersionSet {
    pub fn new(ctx: Arc<TsKvContext>) -> Self {
        Self {
            ctx,
            dbs: HashMap::new(),
            vnodes: HashMap::new(),
        }
    }

    pub async fn create_db(&mut self, schema: DatabaseSchema) -> TskvResult<Arc<RwLock<Database>>> {
        let db = self
            .dbs
            .entry(schema.owner())
            .or_insert(Arc::new(RwLock::new(
                Database::new(self.ctx.clone(), schema).await?,
            )))
            .clone();
        Ok(db)
    }

    pub fn get_db(&self, tenant_name: &str, db_name: &str) -> Option<Arc<RwLock<Database>>> {
        let owner_name = make_owner(tenant_name, db_name);
        if let Some(v) = self.dbs.get(&owner_name) {
            return Some(v.clone());
        }

        None
    }

    #[allow(dead_code)]
    pub fn delete_db(&mut self, tenant: &str, database: &str) -> Option<Arc<RwLock<Database>>> {
        let owner = make_owner(tenant, database);
        self.dbs.remove(&owner)
    }

    pub fn add_vnode(&mut self, id: VnodeId, vnode: VnodeStorage) {
        self.vnodes.insert(id, vnode);
    }

    pub fn remove_vnode(&mut self, id: VnodeId) {
        self.vnodes.remove(&id);
    }

    pub fn get_vnode(&self, id: VnodeId) -> Option<&VnodeStorage> {
        self.vnodes.get(&id)
    }

    pub fn vnodes(&self) -> HashMap<VnodeId, VnodeStorage> {
        self.vnodes.clone()
    }
}

// -----------------------------*******************---------------------------------//
pub async fn split_to_tsfamily(
    ctx: Arc<TsKvContext>,
    summary_file: impl AsRef<Path>,
) -> TskvResult<()> {
    let mut reader = Box::new(Reader::open(&summary_file).await.unwrap());
    let mut tsf_edits_map: HashMap<VnodeId, Vec<VersionEdit>> = HashMap::new();
    loop {
        let res = reader.read_record().await;
        match res {
            Ok(result) => {
                let ed = VersionEdit::decode(&result.data)?;
                if ed.act_tsf == VnodeAction::Add {
                    tsf_edits_map.insert(ed.tsf_id, vec![ed]);
                } else if ed.act_tsf == VnodeAction::Delete {
                    tsf_edits_map.remove(&ed.tsf_id);
                } else if let Some(data) = tsf_edits_map.get_mut(&ed.tsf_id) {
                    data.push(ed);
                }
            }
            Err(TskvError::Eof) => break,
            Err(TskvError::RecordFileHashCheckFailed { .. }) => continue,
            Err(e) => {
                return Err(e);
            }
        }
    }

    for (tsf_id, edits) in tsf_edits_map {
        if edits.is_empty() {
            continue;
        }

        let owner = Arc::new(edits[0].tsf_name.clone());

        let mut max_seq_no = 0;
        let mut max_file_id = 1_u64;
        let mut max_level_ts = i64::MIN;
        let mut files: HashMap<u64, CompactMeta> = HashMap::new();
        for e in edits {
            max_seq_no = std::cmp::max(max_seq_no, e.seq_no);
            max_level_ts = std::cmp::max(max_level_ts, e.max_level_ts);
            max_file_id = std::cmp::max(max_file_id, e.file_id);
            for m in e.del_files {
                files.remove(&m.file_id);
            }
            for m in e.add_files {
                files.insert(m.file_id, m);
            }
        }

        let mut ve = VersionEdit::new_add_vnode(tsf_id, owner.to_string(), max_seq_no);
        ve.file_id = max_file_id;
        ve.max_level_ts = max_level_ts;
        ve.add_files = files.into_values().collect();

        let dir = ctx.options.storage.ts_family_dir(&owner, tsf_id);
        let tmp_file = file_utils::make_tsfamily_summary_file(dir);
        let mut summary = Summary::new(tsf_id, ctx.clone(), &tmp_file).await?;
        summary.write_record(&ve).await?;
        trace::info!("split summary {:?} to {:?}", ve, tmp_file);
    }

    Ok(())
}
