use actix_web::web::Data;
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptionsBuilder};
use models::oid::Identifier;
use models::schema::TenantOptionsBuilder;
use tracing::error;

use crate::store::command::{CommonResp, ReadCommand};
use crate::{MetaApp, WriteCommand};

pub mod api;
pub mod connection;
pub mod raft_api;

pub async fn init_meta(app: &Data<MetaApp>) {
    // init user
    let user_opt_res = UserOptionsBuilder::default()
        .must_change_password(true)
        .comment("system admin")
        .build();
    let user_opt = if user_opt_res.is_err() {
        error!(
            "failed init admin user {}, exit init meta",
            app.meta_init.admin_user
        );
        return;
    } else {
        user_opt_res.unwrap()
    };
    let req = WriteCommand::CreateUser(
        app.meta_init.cluster_name.clone(),
        app.meta_init.admin_user.clone(),
        user_opt,
        true,
    );
    if app.raft.client_write(req).await.is_err() {
        error!(
            "failed init admin user {}, exit init meta",
            app.meta_init.admin_user
        );
        return;
    }

    // init tenant
    let tenant_opt = TenantOptionsBuilder::default()
        .comment("system tenant")
        .build()
        .expect("failed to init system tenant.");
    let req = WriteCommand::CreateTenant(
        app.meta_init.cluster_name.clone(),
        app.meta_init.system_tenant.clone(),
        tenant_opt,
    );
    if app.raft.client_write(req).await.is_err() {
        error!(
            "failed init system tenant {}, exit init meta",
            app.meta_init.system_tenant
        );
        return;
    }

    // init role
    let req = ReadCommand::User(
        app.meta_init.cluster_name.clone(),
        app.meta_init.admin_user.to_string(),
    );
    let sm_r = app.store.state_machine.read().await;
    let user_resp =
        serde_json::from_str::<CommonResp<Option<UserDesc>>>(&sm_r.process_read_command(&req));
    drop(sm_r);
    let user = if user_resp.is_err() {
        error!(
            "failed get admin user {}, exit init meta",
            app.meta_init.admin_user
        );
        return;
    } else {
        user_resp.unwrap()
    };
    if let CommonResp::Ok(Some(user_desc)) = user {
        let role = TenantRoleIdentifier::System(SystemTenantRole::Owner);
        let req = WriteCommand::AddMemberToTenant(
            app.meta_init.cluster_name.clone(),
            *user_desc.id(),
            role,
            app.meta_init.system_tenant.to_string(),
        );
        if app.raft.client_write(req).await.is_err() {
            error!(
                "failed add admin user {} to system tenant {}, exist init meta",
                app.meta_init.admin_user, app.meta_init.system_tenant
            );
            return;
        }
    }

    // init database
    let req = WriteCommand::Set {
        key: format!(
            "/{}/tenants/{}/dbs/{}",
            app.meta_init.cluster_name, app.meta_init.system_tenant, app.meta_init.default_database
        ),
        value: format!(
            "{{\"tenant\":\"{}\",\"database\":\"{}\",\"config\":{{\"ttl\":null,\"shard_num\":null,\"vnode_duration\":null,\"replica\":null,\"precision\":null}}}}",
            app.meta_init.system_tenant, app.meta_init.default_database
        ),
    };
    if app.raft.client_write(req).await.is_err() {
        error!(
            "failed create default database {}, exist init meta",
            app.meta_init.default_database
        );
    }
}
