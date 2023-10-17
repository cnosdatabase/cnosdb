use async_trait::async_trait;
use meta::error::MetaError;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateTenant;
use spi::Result;
use trace::debug;

use crate::execution::ddl::DDLDefinitionTask;

pub struct CreateTenantTask {
    stmt: CreateTenant,
}

impl CreateTenantTask {
    pub fn new(stmt: CreateTenant) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateTenantTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let CreateTenant {
            ref name,
            ref if_not_exists,
            ref options,
        } = self.stmt;

        // 元数据接口查询tenant是否存在
        let tenant = query_state_machine.meta.tenant_meta(name).await;

        match (if_not_exists, tenant) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetaError::TenantAlreadyExists {
                tenant: name.clone(),
            })?,
            // does not exist, create
            (_, None) => {
                // 创建tenant
                // name: String
                // options: TenantOptions
                debug!("Create tenant {} with options [{}]", name, options);
                query_state_machine
                    .meta
                    .create_tenant(name.to_string(), options.clone())
                    .await?;
                Ok(Output::Nil(()))
            }
        }
    }
}
