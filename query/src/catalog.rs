use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
    error::{DataFusionError, Result},
};
use parking_lot::RwLock;

use tskv::engine::EngineRef;

use crate::{
    schema::{TableFiled, TableSchema},
    table::ClusterTable,
};

pub type CatalogRef = Arc<dyn CatalogProvider>;

pub struct UserCatalog {
    engine: EngineRef,
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl UserCatalog {
    pub fn new(engine: EngineRef) -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            engine,
        }
    }
    pub fn deregister_schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut db = self.schemas.write();
        Ok(db.remove(name))
    }
}

impl CatalogProvider for UserCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        {
            let schemas = self.schemas.read();
            if let Some(v) = schemas.get(name) {
                return Some(v.clone());
            }
        }

        let mut schemas = self.schemas.write();
        let v = schemas
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(DatabaseSchema::new(name.to_owned(), self.engine.clone())));

        Some(v.clone())
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut schemas = self.schemas.write();
        Ok(schemas.insert(name.into(), schema))
    }
}

pub struct DatabaseSchema {
    db_name: String,
    engine: EngineRef,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl DatabaseSchema {
    pub fn new(db: String, engine: EngineRef) -> Self {
        Self {
            db_name: db,
            tables: RwLock::new(HashMap::new()),
            engine,
        }
    }
}

impl SchemaProvider for DatabaseSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read();
        tables.keys().cloned().collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        {
            let tables = self.tables.read();
            if let Some(v) = tables.get(name) {
                return Some(v.clone());
            }
        }

        let mut tables = self.tables.write();
        if let Ok(Some(v)) = self
            .engine
            .get_table_schema(&self.db_name, &name.to_string())
        {
            let mut fields = BTreeMap::new();
            for item in v {
                let field = TableFiled::from(&item);
                fields.insert(field.name.clone(), field);
            }
            let schema = TableSchema::new(self.db_name.clone(), name.to_owned(), fields);
            let table = Arc::new(ClusterTable::new(self.engine.clone(), schema));
            tables.insert(name.to_owned(), table.clone());
            return Some(table);
        }

        None
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            //todo: use crate::error::Error
            return Err(DataFusionError::Execution(format!(
                "The table {} already exists",
                name
            )));
        }
        let mut tables = self.tables.write();
        Ok(tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write();
        Ok(tables.remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read();
        tables.contains_key(name)
    }
}
