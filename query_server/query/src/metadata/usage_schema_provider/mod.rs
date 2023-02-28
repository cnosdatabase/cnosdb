use std::collections::HashMap;
use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::datasource::{provider_as_source, TableProvider, ViewTable};
use datafusion::logical_expr::{binary_expr, col, LogicalPlanBuilder, Operator};
use datafusion::prelude::lit;
use meta::error::MetaError;
use meta::MetaClientRef;
use models::auth::user::User;
use models::oid::Identifier;
use models::schema::DEFAULT_CATALOG;
use spi::Result;
pub use vnode_disk_storage::USAGE_SCHEMA_VNODE_DISK_STORAGE;

use crate::data_source::table_provider::tskv::ClusterTable;
use crate::metadata::usage_schema_provider::data_in::DataIn;
use crate::metadata::usage_schema_provider::data_out::DataOut;
use crate::metadata::usage_schema_provider::queries::Queries;
use crate::metadata::usage_schema_provider::vnode_disk_storage::VnodeDiskStorage;
use crate::metadata::usage_schema_provider::writes::Writes;

mod data_in;
mod data_out;
mod queries;
mod vnode_disk_storage;
mod writes;

pub const USAGE_SCHEMA: &str = "usage_schema";

pub struct UsageSchemaProvider {
    tenant_meta: MetaClientRef,
    table_factories: HashMap<String, BoxUsageSchemaTableFactory>,
}

impl UsageSchemaProvider {
    pub fn new(default_meta_client: MetaClientRef) -> Self {
        let mut provider = Self {
            tenant_meta: default_meta_client,
            table_factories: Default::default(),
        };
        provider.register_table_factory(Box::new(VnodeDiskStorage {}));
        provider.register_table_factory(Box::new(DataIn {}));
        provider.register_table_factory(Box::new(DataOut {}));
        provider.register_table_factory(Box::new(Queries {}));
        provider.register_table_factory(Box::new(Writes {}));
        provider
    }

    fn register_table_factory(&mut self, factory: BoxUsageSchemaTableFactory) {
        let _ = self
            .table_factories
            .insert(factory.table_name().to_ascii_lowercase(), factory);
    }

    pub fn name(&self) -> &str {
        USAGE_SCHEMA
    }

    pub fn table(
        &self,
        name: &str,
        user: &User,
        coord: CoordinatorRef,
        meta: MetaClientRef,
    ) -> Result<Arc<dyn TableProvider>> {
        let usage_schema_table = self
            .table_factories
            .get(name)
            .ok_or_else(|| MetaError::TableNotFound { table: name.into() })?;
        usage_schema_table.create(user, coord.clone(), meta.clone(), self.tenant_meta.clone())
    }
}

type BoxUsageSchemaTableFactory = Box<dyn UsageSchemaTableFactory + Send + Sync>;

pub trait UsageSchemaTableFactory {
    fn table_name(&self) -> &str;
    fn create(
        &self,
        user: &User,
        coord: CoordinatorRef,
        meta: MetaClientRef,
        default_catalog: MetaClientRef,
    ) -> Result<Arc<dyn TableProvider>>;
}

pub fn create_usage_schema_view_table(
    user: &User,
    coord: CoordinatorRef,
    meta: MetaClientRef,
    view_table_name: &str,
    default_catalog_meta_client: MetaClientRef,
) -> spi::Result<Arc<dyn TableProvider>> {
    let tskv = default_catalog_meta_client
        .get_tskv_table_schema(USAGE_SCHEMA, view_table_name)?
        .ok_or_else(|| MetaError::TableNotFound {
            table: view_table_name.into(),
        })?;
    let cluster_table = Arc::new(ClusterTable::new(coord.clone(), tskv));
    if user.desc().is_admin() && meta.tenant_name().eq(DEFAULT_CATALOG) {
        return Ok(cluster_table);
    }
    let cluster_table = provider_as_source(cluster_table);
    let builder = LogicalPlanBuilder::scan(view_table_name, cluster_table, None)?.filter(
        binary_expr(col("tenant"), Operator::Eq, lit(meta.tenant().name())),
    )?;

    let builder_copy = builder.clone();
    let cols = builder_copy
        .schema()
        .fields()
        .iter()
        .filter(|f| f.name().ne("tenant"))
        .map(|f| col(f.name()));

    let logical_plan = builder.project(cols)?.build()?;
    Ok(Arc::new(ViewTable::try_new(logical_plan, None)?))
}
