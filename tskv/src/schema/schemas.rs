use crate::schema::error::{MetaSnafu, Result, SchemaError};
use meta::error::MetaError;
use meta::meta_client::{MetaClientRef, MetaRef};
use models::codec::Encoding;
use models::schema::{
    ColumnType, DatabaseSchema, TableColumn, TableSchema, TenantOptions, TskvTableSchema,
};
use models::{ColumnId, SeriesId};
use parking_lot::RwLock;
use protos::models::Point;
use snafu::ResultExt;
use std::collections::HashMap;

use crate::Error;
use trace::{error, info, warn};

const TIME_STAMP_NAME: &str = "time";

#[derive(Debug)]
pub struct DBschemas {
    tenant_name: String,
    database_name: String,
    client: MetaClientRef,
}

impl DBschemas {
    pub fn new(db_schema: DatabaseSchema, meta: MetaRef) -> Result<Self> {
        let table_schemas: HashMap<String, TskvTableSchema> = HashMap::new();
        let client = meta
            .tenant_manager()
            .tenant_meta(db_schema.tenant_name())
            .ok_or(SchemaError::TenantNotFound {
                tenant: db_schema.tenant_name().to_string(),
            })?;
        if client.get_db_schema(db_schema.database_name())?.is_none() {
            client.create_db(db_schema.clone())?;
        }
        Ok(Self {
            tenant_name: db_schema.tenant_name().to_string(),
            database_name: db_schema.database_name().to_string(),
            client,
        })
    }

    pub fn database_name(&self) -> String {
        self.database_name.clone()
    }

    pub fn alter_db_schema(&self, db_schema: DatabaseSchema) -> Result<()> {
        // todo: client need alter db action
        Ok(())
    }

    pub fn check_field_type_from_cache(&self, info: &Point) -> Result<()> {
        let table_name =
            unsafe { String::from_utf8_unchecked(info.tab().unwrap().bytes().to_vec()) };
        let schema = self
            .client
            .get_tskv_table_schema(&self.database_name, &table_name)?
            .ok_or(SchemaError::DatabaseNotFound {
                database: self.database_name.clone(),
            })?;
        for field in info.fields().unwrap() {
            let field_name = String::from_utf8(field.name().unwrap().bytes().to_vec()).unwrap();
            if let Some(v) = schema.column(&field_name) {
                if field.type_().0 != v.column_type.field_type() as i32 {
                    error!(
                        "type mismatch, point: {}, schema: {}",
                        field.type_().0,
                        v.column_type.field_type()
                    );
                    return Err(SchemaError::FieldType {
                        field: field_name.to_owned(),
                    });
                }
            } else {
                return Err(SchemaError::NotFoundField {
                    field: field_name.to_string(),
                });
            }
        }
        for tag in info.tags().unwrap() {
            let tag_name: String = String::from_utf8(tag.key().unwrap().bytes().to_vec()).unwrap();
            if let Some(v) = schema.column(&tag_name) {
                if ColumnType::Tag != v.column_type {
                    error!("type mismatch, point: tag, schema: {}", &v.column_type);
                    return Err(SchemaError::FieldType {
                        field: tag_name.to_owned(),
                    });
                }
            } else {
                return Err(SchemaError::NotFoundField {
                    field: tag_name.to_owned(),
                });
            }
        }
        Ok(())
    }

    pub fn check_field_type_or_else_add(&self, info: &Point) -> Result<()> {
        //load schema first from cache,or else from storage and than cache it!
        let table_name =
            unsafe { String::from_utf8_unchecked(info.tab().unwrap().bytes().to_vec()) };
        let db_name = unsafe { String::from_utf8_unchecked(info.db().unwrap().bytes().to_vec()) };
        let schema = self.client.get_tskv_table_schema(&db_name, &table_name)?;
        let mut new_schema = false;
        let mut schema = match schema {
            None => {
                let mut schema = TskvTableSchema::default();
                schema.tenant = self.tenant_name.clone();
                schema.db = db_name;
                schema.name = table_name;
                new_schema = true;
                schema
            }
            Some(schema) => schema,
        };

        let mut schema_change = false;
        let mut check_fn = |field: &mut TableColumn| -> Result<()> {
            let encoding = match schema.column(&field.name) {
                None => Encoding::Default,
                Some(v) => v.encoding,
            };
            field.encoding = encoding;

            match schema.column(&field.name) {
                Some(v) => {
                    if field.column_type != v.column_type {
                        trace::debug!(
                            "type mismatch, point: {}, schema: {}",
                            &field.column_type,
                            &v.column_type
                        );
                        trace::debug!("type mismatch, schema: {:?}", &schema);
                        return Err(SchemaError::FieldType {
                            field: field.name.to_owned(),
                        });
                    }
                }
                None => {
                    schema_change = true;
                    field.id = schema.columns().len() as ColumnId;
                    schema.add_column(field.clone());
                }
            }
            Ok(())
        };
        //check timestamp
        check_fn(&mut TableColumn::new_with_default(
            TIME_STAMP_NAME.to_string(),
            ColumnType::Time,
        ))?;

        //check tags
        for tag in info.tags().unwrap() {
            let tag_key =
                unsafe { String::from_utf8_unchecked(tag.key().unwrap().bytes().to_vec()) };
            check_fn(&mut TableColumn::new_with_default(tag_key, ColumnType::Tag))?
        }

        //check fields
        for field in info.fields().unwrap() {
            let field_name =
                unsafe { String::from_utf8_unchecked(field.name().unwrap().bytes().to_vec()) };
            check_fn(&mut TableColumn::new_with_default(
                field_name,
                ColumnType::from_i32(field.type_().0),
            ))?
        }

        //schema changed store it
        if new_schema {
            schema.schema_id = 0;
            self.client
                .create_table(&TableSchema::TsKvTableSchema(schema.clone()))?;
        } else if schema_change {
            schema.schema_id += 1;
            self.client
                .update_table(&TableSchema::TsKvTableSchema(schema.clone()))?;
        }
        Ok(())
    }

    pub fn get_table_schema(&self, tab: &str) -> Result<Option<TskvTableSchema>> {
        let schema = self
            .client
            .get_tskv_table_schema(&self.database_name, tab)?;

        Ok(schema)
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        let tables = self.client.list_tables(&self.database_name)?;
        Ok(tables)
    }

    pub fn del_table_schema(&self, tab: &str) -> Result<()> {
        self.client.drop_table(&self.database_name, tab)?;
        Ok(())
    }

    pub fn db_schema(&self) -> Result<DatabaseSchema> {
        let db_schema =
            self.client
                .get_db_schema(&self.database_name)?
                .ok_or(MetaError::DatabaseNotFound {
                    database: self.database_name.clone(),
                })?;
        Ok(db_schema)
    }
}
