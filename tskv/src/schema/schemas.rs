use std::borrow::Cow;

use async_recursion::async_recursion;
use meta::error::{MetaError, TenantNotFoundSnafu};
use meta::model::{MetaClientRef, MetaRef};
use models::codec::Encoding;
use models::schema::database_schema::DatabaseSchema;
use models::schema::table_schema::TableSchema;
use models::schema::tskv_table_schema::{
    ColumnType, TableColumn, TskvTableSchema, TskvTableSchemaRef,
};
use snafu::OptionExt;

use crate::database::FbSchema;
use crate::schema::error::{ColumnTypeSnafu, SchemaResult};

#[derive(Debug)]
pub struct DBschemas {
    meta: MetaRef,
    db_schema: DatabaseSchema,
    tenant_name: String,
    database_name: String,
}

impl DBschemas {
    pub async fn new(db_schema: DatabaseSchema, meta: MetaRef) -> SchemaResult<Self> {
        let client =
            meta.tenant_meta(db_schema.tenant_name())
                .await
                .context(TenantNotFoundSnafu {
                    tenant: db_schema.tenant_name().to_string(),
                })?;

        if client.get_db_schema(db_schema.database_name())?.is_none() {
            client.create_db(db_schema.clone()).await?;
        }

        Ok(Self {
            meta,
            db_schema: db_schema.clone(),
            tenant_name: db_schema.tenant_name().to_string(),
            database_name: db_schema.database_name().to_string(),
        })
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant_name
    }

    pub fn db_schema(&self) -> &DatabaseSchema {
        &self.db_schema
    }

    async fn tenant_meta(&self) -> SchemaResult<MetaClientRef> {
        let client =
            self.meta
                .tenant_meta(self.tenant_name())
                .await
                .context(TenantNotFoundSnafu {
                    tenant: self.tenant_name().to_string(),
                })?;

        Ok(client)
    }

    #[async_recursion]
    async fn check_field_type_or_else_add_impl<'a>(
        &self,
        fb_schema: &'a FbSchema<'a>,
        get_schema_from_meta: bool,
    ) -> SchemaResult<TskvTableSchemaRef> {
        let mut schema_changed = false;
        let mut new_schema = false;
        if get_schema_from_meta {
            self.get_table_schema_by_meta(fb_schema.table).await?;
        } else {
            self.get_table_schema(fb_schema.table).await?;
        }
        let opt_schema = self.get_table_schema(fb_schema.table).await?;
        let mut schema = match opt_schema.as_ref() {
            Some(schema) => Cow::Borrowed(schema.as_ref()),
            None => {
                let mut schema = TskvTableSchema::new(
                    self.tenant_name().into(),
                    self.database_name().into(),
                    fb_schema.table.to_string(),
                    vec![],
                );
                let db_schema = self.db_schema_by_meta().await?;
                let precision = db_schema.config.precision();
                schema.add_column(TableColumn::new_time_column(
                    schema.next_column_id(),
                    (*precision).into(),
                ));
                new_schema = true;
                Cow::Owned(schema)
            }
        };

        for tag_name in &fb_schema.tag_names {
            match schema.column(tag_name) {
                Some(column) => {
                    if !column.column_type.is_tag() {
                        return Err(ColumnTypeSnafu {
                            column: tag_name.to_string(),
                            found: ColumnType::Tag,
                            expected: column.column_type.clone(),
                        }
                        .build());
                    }
                }
                None => {
                    let next_column_id = schema.next_column_id();
                    schema.to_mut().add_column(TableColumn::new_tag_column(
                        next_column_id,
                        tag_name.to_string(),
                    ));
                    schema_changed = true;
                }
            };
        }

        for (field_name, field_type) in fb_schema
            .field_names
            .iter()
            .zip(fb_schema.field_types.iter())
        {
            match schema.column(field_name) {
                Some(column) => {
                    let column_type = ColumnType::from_proto_field_type(*field_type);
                    if !column.column_type.matches_type(&column_type) {
                        return Err(ColumnTypeSnafu {
                            column: field_name.to_string(),
                            found: column_type,
                            expected: column.column_type.clone(),
                        }
                        .build());
                    }
                }
                None => {
                    let next_column_id = schema.next_column_id();
                    schema.to_mut().add_column(TableColumn::new(
                        next_column_id,
                        field_name.to_string(),
                        ColumnType::from_proto_field_type(*field_type),
                        Encoding::Default,
                    ));
                    schema_changed = true;
                }
            }
        }

        let client = self.tenant_meta().await?;
        //schema changed store it
        let schema = if new_schema {
            schema.to_mut().schema_version = 0;
            let schema: TskvTableSchemaRef = schema.into_owned().into();
            let res = client
                .create_table(&TableSchema::TsKvTableSchema(schema.clone()))
                .await;
            if let Err(MetaError::TableAlreadyExists { .. }) = res.as_ref() {
                self.check_field_type_or_else_add_impl(fb_schema, true)
                    .await?;
            } else {
                res?;
            }
            schema
        } else if schema_changed {
            schema.to_mut().schema_version += 1;
            let schema: TskvTableSchemaRef = schema.into_owned().into();
            let res = client
                .update_table(&TableSchema::TsKvTableSchema(schema.clone()))
                .await;
            if let Err(MetaError::UpdateTableConflict { .. }) = &res {
                self.check_field_type_or_else_add_impl(fb_schema, true)
                    .await?;
            } else {
                res?;
            }
            schema
        } else {
            // Safety
            // if opt_schema is none then create new schema
            unsafe { opt_schema.unwrap_unchecked() }
        };
        Ok(schema)
    }

    pub async fn check_field_type_or_else_add(
        &self,
        fb_schema: &FbSchema<'_>,
    ) -> SchemaResult<TskvTableSchemaRef> {
        //load schema first from cache
        self.check_field_type_or_else_add_impl(fb_schema, false)
            .await
    }

    pub async fn get_table_schema(&self, tab: &str) -> SchemaResult<Option<TskvTableSchemaRef>> {
        let schema = self
            .tenant_meta()
            .await?
            .get_tskv_table_schema(&self.database_name, tab)?;

        Ok(schema)
    }

    pub async fn get_table_schema_by_meta(
        &self,
        tab: &str,
    ) -> SchemaResult<Option<TskvTableSchemaRef>> {
        let schema = self
            .tenant_meta()
            .await?
            .get_tskv_table_schema_by_meta(&self.database_name, tab)
            .await?;

        Ok(schema)
    }

    pub async fn list_tables(&self) -> SchemaResult<Vec<String>> {
        let tables = self.tenant_meta().await?.list_tables(&self.database_name)?;

        Ok(tables)
    }

    pub async fn del_table_schema(&self, tab: &str) -> SchemaResult<()> {
        self.tenant_meta()
            .await?
            .drop_table(&self.database_name, tab)
            .await?;
        Ok(())
    }

    pub async fn db_schema_by_meta(&self) -> SchemaResult<DatabaseSchema> {
        let db_schema = self
            .tenant_meta()
            .await?
            .get_db_schema(&self.database_name)?
            .ok_or_else(|| MetaError::DatabaseNotFound {
                database: self.database_name.clone(),
            })?;
        Ok(db_schema)
    }
}
