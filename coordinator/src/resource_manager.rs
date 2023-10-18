use std::collections::HashMap;
use std::sync::Arc;

use meta::model::meta_admin::AdminMeta;
use models::meta_data::ReplicationSet;
use models::oid::Identifier;
use models::schema::{ResourceInfo, ResourceOperator, ResourceStatus, TableSchema};
use models::utils::now_timestamp_nanos;
use protos::kv_service::admin_command_request::Command::{self, DropDb, DropTab, UpdateTags};
use protos::kv_service::{
    AddColumnRequest, AdminCommandRequest, AlterColumnRequest, DropColumnRequest, DropDbRequest,
    DropTableRequest, RenameColumnRequest, UpdateSetValue, UpdateTagsRequest,
};
use tracing::{debug, error, info};

use crate::errors::*;
use crate::{Coordinator, VnodeManagerCmdType};

#[derive(Clone)]
pub struct ResourceManager {}

impl ResourceManager {
    pub async fn change_and_write(
        admin_meta: Arc<AdminMeta>,
        mut resourceinfo: ResourceInfo,
        dest_status: ResourceStatus,
        comment: &str,
    ) -> CoordinatorResult<bool> {
        resourceinfo.set_status(dest_status);
        resourceinfo.set_comment(comment);
        admin_meta
            .write_resourceinfo(resourceinfo.get_names(), resourceinfo.clone())
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;
        Ok(true)
    }

    pub async fn check_and_run(coord: Arc<dyn Coordinator>) -> CoordinatorResult<bool> {
        let (node_id, is_lock) = coord
            .meta_manager()
            .read_resourceinfos_mark()
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;
        if !is_lock {
            // lock resourceinfos
            coord
                .meta_manager()
                .write_resourceinfos_mark(coord.node_id(), true)
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;

            let mut is_need_loop = false;
            loop {
                let resourceinfos = coord
                    .meta_manager()
                    .read_resourceinfos(&[])
                    .await
                    .map_err(|err| CoordinatorError::Meta { source: err })?;
                let now = now_timestamp_nanos();
                for mut resourceinfo in resourceinfos {
                    if (*resourceinfo.get_status() == ResourceStatus::Schedule
                        && resourceinfo.get_time() <= now)
                        || *resourceinfo.get_status() == ResourceStatus::Failed
                    {
                        resourceinfo.increase_try_count();

                        resourceinfo.set_status(ResourceStatus::Executing);
                        if let Err(meta_err) = coord
                            .meta_manager()
                            .write_resourceinfo(resourceinfo.get_names(), resourceinfo.clone())
                            .await
                        {
                            error!("{}", meta_err.to_string());
                            is_need_loop = true;
                            continue;
                        }
                        if let Err(coord_err) =
                            ResourceManager::do_operator(coord.clone(), resourceinfo).await
                        {
                            error!("{}", coord_err.to_string());
                            is_need_loop = true;
                        }
                    }
                }
                if !is_need_loop {
                    // unlock resourceinfos
                    coord
                        .meta_manager()
                        .write_resourceinfos_mark(coord.node_id(), false)
                        .await
                        .map_err(|err| CoordinatorError::Meta { source: err })?;
                    return Ok(true);
                }
            }
        } else {
            info!("resource is executing by {}", node_id);
            Ok(false)
        }
    }

    async fn do_operator(
        coord: Arc<dyn Coordinator>,
        resourceinfo: ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let operator_result = match resourceinfo.get_operator() {
            ResourceOperator::DropTenant(tenant_name) => {
                ResourceManager::drop_tenant(coord.clone(), tenant_name).await
            }
            ResourceOperator::DropDatabase(tenant_name, db_name) => {
                ResourceManager::drop_database(coord.clone(), tenant_name, db_name).await
            }
            ResourceOperator::DropTable(tenant_name, db_name, table_name) => {
                ResourceManager::drop_table(coord.clone(), tenant_name, db_name, table_name).await
            }
            ResourceOperator::AddColumn(tenant_name, ..)
            | ResourceOperator::DropColumn(tenant_name, ..)
            | ResourceOperator::AlterColumn(tenant_name, ..)
            | ResourceOperator::RenameTagName(tenant_name, ..) => {
                ResourceManager::alter_table(coord.clone(), &resourceinfo, tenant_name).await
            }
            ResourceOperator::UpdateTagValue(
                tenant_name,
                db_name,
                update_set_value,
                series_keys,
                replica_set,
            ) => {
                ResourceManager::update_tag_value(
                    coord.clone(),
                    tenant_name,
                    db_name,
                    update_set_value,
                    series_keys,
                    replica_set,
                )
                .await
            }
        };

        let mut status_comment = (ResourceStatus::Successed, String::default());
        if let Err(coord_err) = &operator_result {
            status_comment.0 = ResourceStatus::Failed;
            status_comment.1 = coord_err.to_string();
        }
        ResourceManager::change_and_write(
            coord.meta_manager(),
            resourceinfo.clone(),
            status_comment.0,
            &status_comment.1,
        )
        .await?;

        operator_result
    }

    async fn drop_tenant(
        coord: Arc<dyn Coordinator>,
        tenant_name: &str,
    ) -> CoordinatorResult<bool> {
        let tenant =
            coord
                .tenant_meta(tenant_name)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant_name.to_string(),
                })?;

        // drop role in the tenant
        let all_roles = tenant
            .custom_roles()
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;
        for role in all_roles {
            tenant
                .drop_custom_role(role.name())
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;
        }

        // drop database in the tenant
        let all_dbs = tenant
            .list_databases()
            .map_err(|err| CoordinatorError::Meta { source: err })?;
        for db_name in all_dbs {
            tenant
                .drop_db(&db_name)
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;
        }

        // drop tenant metadata
        coord
            .meta_manager()
            .drop_tenant(tenant_name)
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;

        Ok(true)
    }

    async fn drop_database(
        coord: Arc<dyn Coordinator>,
        tenant_name: &str,
        db_name: &str,
    ) -> CoordinatorResult<bool> {
        let tenant =
            coord
                .tenant_meta(tenant_name)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant_name.to_string(),
                })?;

        let req = AdminCommandRequest {
            tenant: tenant_name.to_string(),
            command: Some(DropDb(DropDbRequest {
                db: db_name.to_string(),
            })),
        };

        if coord.using_raft_replication() {
            let buckets = tenant.get_db_info(db_name)?.map_or(vec![], |v| v.buckets);
            for bucket in buckets {
                for replica in bucket.shard_group {
                    let cmd_type = VnodeManagerCmdType::DestoryRaftGroup(replica.id);
                    coord.vnode_manager(tenant_name, cmd_type).await?;
                }
            }
        } else {
            coord.broadcast_command(req).await?;
        }
        debug!("Drop database {} of tenant {}", db_name, tenant_name);

        tenant
            .drop_db(db_name)
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;

        Ok(true)
    }

    async fn drop_table(
        coord: Arc<dyn Coordinator>,
        tenant_name: &str,
        db_name: &str,
        table_name: &str,
    ) -> CoordinatorResult<bool> {
        let tenant =
            coord
                .tenant_meta(tenant_name)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant_name.to_string(),
                })?;

        info!("Drop table {}/{}/{}", tenant_name, db_name, table_name);
        let req = AdminCommandRequest {
            tenant: tenant_name.to_string(),
            command: Some(DropTab(DropTableRequest {
                db: db_name.to_string(),
                table: table_name.to_string(),
            })),
        };
        coord.broadcast_command(req).await?;

        tenant
            .drop_table(db_name, table_name)
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;

        Ok(true)
    }

    async fn alter_table(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
        tenant_name: &str,
    ) -> CoordinatorResult<bool> {
        let tenant =
            coord
                .tenant_meta(tenant_name)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant_name.to_string(),
                })?;

        let req = match resourceinfo.get_operator() {
            ResourceOperator::AddColumn(_, table_schema, table_column) => Some((
                table_schema,
                AdminCommandRequest {
                    tenant: tenant_name.to_string(),
                    command: Some(Command::AddColumn(AddColumnRequest {
                        db: table_schema.db.to_owned(),
                        table: table_schema.name.to_string(),
                        column: table_column.encode()?,
                    })),
                },
            )),
            ResourceOperator::DropColumn(_, drop_column_name, table_schema) => Some((
                table_schema,
                AdminCommandRequest {
                    tenant: tenant_name.to_string(),
                    command: Some(Command::DropColumn(DropColumnRequest {
                        db: table_schema.db.to_owned(),
                        table: table_schema.name.to_string(),
                        column: drop_column_name.clone(),
                    })),
                },
            )),
            ResourceOperator::AlterColumn(_, alter_column_name, table_schema, table_column) => {
                Some((
                    table_schema,
                    AdminCommandRequest {
                        tenant: tenant_name.to_string(),
                        command: Some(Command::AlterColumn(AlterColumnRequest {
                            db: table_schema.db.to_owned(),
                            table: table_schema.name.to_string(),
                            name: alter_column_name.clone(),
                            column: table_column.encode()?,
                        })),
                    },
                ))
            }
            ResourceOperator::RenameTagName(_, old_column_name, new_column_name, table_schema) => {
                Some((
                    table_schema,
                    AdminCommandRequest {
                        tenant: tenant_name.to_string(),
                        command: Some(Command::RenameColumn(RenameColumnRequest {
                            db: table_schema.db.to_owned(),
                            table: table_schema.name.to_string(),
                            old_name: old_column_name.clone(),
                            new_name: new_column_name.clone(),
                            dry_run: false,
                        })),
                    },
                ))
            }
            _ => None,
        };

        if let Some((schema, req)) = req {
            coord.broadcast_command(req).await?;

            tenant
                .update_table(&TableSchema::TsKvTableSchema(Arc::new(schema.clone())))
                .await?;
        }

        Ok(true)
    }

    async fn update_tag_value(
        coord: Arc<dyn Coordinator>,
        tenant_name: &str,
        db_name: &str,
        update_set_value: &[(Vec<u8>, Option<Vec<u8>>)],
        series_keys: &[Vec<u8>],
        replica_set: &[ReplicationSet],
    ) -> CoordinatorResult<bool> {
        let new_tags_vec: Vec<UpdateSetValue> = update_set_value
            .iter()
            .map(|e| UpdateSetValue {
                key: e.0.clone(),
                value: e.1.clone(),
            })
            .collect();
        let update_tags_request = UpdateTagsRequest {
            db: db_name.to_string(),
            new_tags: new_tags_vec,
            matched_series: series_keys.to_vec(),
            dry_run: false,
        };

        let req = AdminCommandRequest {
            tenant: tenant_name.to_string(),
            command: Some(UpdateTags(update_tags_request)),
        };

        coord
            .broadcast_command_by_vnode(req, replica_set.to_vec())
            .await?;

        Ok(true)
    }

    pub async fn add_resource_task(
        coord: Arc<dyn Coordinator>,
        mut resourceinfo: ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let resourceinfos = coord.meta_manager().read_resourceinfos(&[]).await?;
        let mut resourceinfos_map: HashMap<String, ResourceInfo> = HashMap::default();
        for resourceinfo in resourceinfos {
            resourceinfos_map.insert(resourceinfo.get_names().join("/"), resourceinfo);
        }

        let name = resourceinfo.get_names().join("/");
        if !resourceinfos_map.contains_key(&name)
            || *resourceinfos_map[&name].get_status() == ResourceStatus::Successed
            || *resourceinfos_map[&name].get_status() == ResourceStatus::Cancel
            || *resourceinfos_map[&name].get_status() == ResourceStatus::Fatal
        {
            if *resourceinfo.get_status() == ResourceStatus::Executing {
                resourceinfo.increase_try_count();
            }

            // write to meta
            coord
                .meta_manager()
                .write_resourceinfo(resourceinfo.get_names(), resourceinfo.clone())
                .await?;

            if *resourceinfo.get_status() == ResourceStatus::Executing {
                // execute right now
                ResourceManager::do_operator(coord, resourceinfo).await?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
