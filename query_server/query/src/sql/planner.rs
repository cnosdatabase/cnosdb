use datafusion::arrow::datatypes::DataType;
use std::collections::{HashMap, HashSet};
use std::iter;
use std::option::Option;
use std::sync::Arc;

use datafusion::common::{Column, DFField, DFSchema, ToDFSchema};
use datafusion::datasource::{source_as_provider, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::logical_plan::Analyze;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{
    cast, lit, BinaryExpr, BuiltinScalarFunction, Case, EmptyRelation, Explain, Expr, Extension,
    LogicalPlan, LogicalPlanBuilder, Operator, PlanType, Projection, TableSource,
    ToStringifiedPlan, Union,
};
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::CreateExternalTable as AstCreateExternalTable;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{
    DataType as SQLDataType, Ident, ObjectName, Query, Statement,
};
use datafusion::sql::TableReference;
use models::schema::{
    ColumnType, TableColumn, TskvTableSchema, TskvTableSchemaRef, TIME_FIELD_NAME,
};
use models::utils::SeqIdGenerator;
use models::{ColumnId, ValueType};
use snafu::ResultExt;
use spi::query::ast::{
    AlterDatabase as ASTAlterDatabase, AlterTable as ASTAlterTable,
    AlterTableAction as ASTAlterTableAction, ColumnOption, CreateDatabase as ASTCreateDatabase,
    CreateTable as ASTCreateTable, DatabaseOptions as ASTDatabaseOptions,
    DescribeDatabase as DescribeDatabaseOptions, DescribeTable as DescribeTableOptions, DropObject,
    ExtStatement, ShowSeries as ASTShowSeries, ShowTagBody, ShowTagValues as ASTShowTagValues,
    With,
};
use spi::query::logical_planner::{
    self, affected_row_expr, merge_affected_row_expr, AlterDatabase, AlterTable, AlterTableAction,
    CreateDatabase, CreateTable, DDLPlan, DescribeDatabase, DescribeTable, DropPlan, ExternalSnafu,
    LogicalPlanner, LogicalPlannerError, Plan, QueryPlan, SYSPlan, MISMATCHED_COLUMNS,
    MISSING_COLUMN,
};
use spi::query::session::IsiphoSessionCtx;

use models::schema::{DatabaseOptions, Duration, Precision};
use spi::catalog::MetadataError;
use spi::query::logical_planner::Result;
use spi::query::UNEXPECTED_EXTERNAL_PLAN;
use trace::debug;

use crate::extension::logical::plan_node::table_writer::TableWriterPlanNode;
use crate::sql::parser::{merge_object_name, normalize_ident, normalize_sql_object_name};
use crate::table::ClusterTable;
use spi::query::logical_planner::MetadataSnafu;

/// CnosDB SQL query planner
#[derive(Debug)]
pub struct SqlPlaner<S> {
    schema_provider: S,
}

impl<S: ContextProvider> SqlPlaner<S> {
    /// Create a new query planner
    pub fn new(schema_provider: S) -> Self {
        SqlPlaner { schema_provider }
    }

    /// Generate a logical plan from an  Extent SQL statement
    pub(crate) fn statement_to_plan(&self, statement: ExtStatement) -> Result<Plan> {
        match statement {
            ExtStatement::SqlStatement(stmt) => self.df_sql_to_plan(*stmt),
            ExtStatement::CreateExternalTable(stmt) => self.external_table_to_plan(stmt),
            ExtStatement::CreateTable(stmt) => self.create_table_to_plan(stmt),
            ExtStatement::CreateDatabase(stmt) => self.database_to_plan(stmt),
            ExtStatement::CreateUser(_) => todo!(),
            ExtStatement::Drop(s) => self.drop_object_to_plan(s),
            ExtStatement::DropUser(_) => todo!(),
            ExtStatement::DescribeTable(stmt) => self.table_to_describe(stmt),
            ExtStatement::DescribeDatabase(stmt) => self.database_to_describe(stmt),
            ExtStatement::ShowDatabases() => self.database_to_show(),
            ExtStatement::ShowTables(stmt) => self.table_to_show(stmt),
            ExtStatement::ShowSeries(stmt) => self.show_series_to_plan(*stmt),
            ExtStatement::ShowTagValues(stmt) => self.show_tag_values(*stmt),
            ExtStatement::Explain(stmt) => {
                self.explain_statement_to_plan(stmt.analyze, stmt.verbose, *stmt.ext_statement)
            }
            ExtStatement::AlterDatabase(stmt) => self.database_to_alter(stmt),
            ExtStatement::AlterTable(stmt) => self.table_to_alter(stmt),
            // system statement
            ExtStatement::ShowQueries => Ok(Plan::SYSTEM(SYSPlan::ShowQueries)),
        }
    }

    fn df_sql_to_plan(&self, stmt: Statement) -> Result<Plan> {
        match stmt {
            Statement::Query(_) => {
                let df_planner = SqlToRel::new(&self.schema_provider);
                let df_plan = df_planner
                    .sql_statement_to_plan(stmt)
                    .context(ExternalSnafu)?;
                Ok(Plan::Query(QueryPlan { df_plan }))
            }
            Statement::Insert {
                table_name: ref sql_object_name,
                columns: ref sql_column_names,
                source,
                ..
            } => self.insert_to_plan(sql_object_name, sql_column_names, source),
            Statement::Kill { id, .. } => Ok(Plan::SYSTEM(SYSPlan::KillQuery(id.into()))),
            _ => Err(LogicalPlannerError::NotImplemented {
                err: stmt.to_string(),
            }),
        }
    }

    /// Generate a plan for EXPLAIN ... that will print out a plan
    pub fn explain_statement_to_plan(
        &self,
        verbose: bool,
        analyze: bool,
        statement: ExtStatement,
    ) -> Result<Plan> {
        let plan = self.statement_to_plan(statement)?;

        let input_df_plan = match plan {
            Plan::Query(query) => Arc::new(query.df_plan),
            _ => {
                return Err(LogicalPlannerError::NotImplemented {
                    err: "explain non-query statement.".to_string(),
                })
            }
        };

        let schema = LogicalPlan::explain_schema()
            .to_dfschema_ref()
            .context(ExternalSnafu)?;

        let df_plan = if analyze {
            LogicalPlan::Analyze(Analyze {
                verbose,
                input: input_df_plan,
                schema,
            })
        } else {
            let stringified_plans =
                vec![input_df_plan.to_stringified(PlanType::InitialLogicalPlan)];
            LogicalPlan::Explain(Explain {
                verbose,
                plan: input_df_plan,
                stringified_plans,
                schema,
            })
        };

        Ok(Plan::Query(QueryPlan { df_plan }))
    }

    /// Add a projection operation (if necessary)
    /// 1. Iterate over all fields of the table
    ///   1.1. Construct the col expression
    ///   1.2. Check if the current field exists in columns
    ///     1.2.1. does not exist: add cast(null as target_type) expression to save
    ///     1.2.1. Exist: save if the type matches, add cast(expr as target_type) to save if it does not exist
    fn add_projection_between_source_and_insert_node_if_necessary(
        &self,
        target_table: Arc<dyn TableSource>,
        source_plan: LogicalPlan,
        insert_columns: Vec<String>,
    ) -> Result<LogicalPlan> {
        let insert_col_name_with_source_field_tuples: Vec<(&String, &DFField)> = insert_columns
            .iter()
            .zip(source_plan.schema().fields())
            .collect();

        debug!(
            "Insert col name with source field tuples: {:?}",
            insert_col_name_with_source_field_tuples
        );
        debug!("Target table: {:?}", target_table.schema());

        let assignments: Vec<Expr> = target_table
            .schema()
            .fields()
            .iter()
            .map(|column| {
                let target_column_name = column.name();
                let target_column_data_type = column.data_type();

                let expr = if let Some((_, source_field)) = insert_col_name_with_source_field_tuples
                    .iter()
                    .find(|(insert_col_name, _)| *insert_col_name == target_column_name)
                {
                    // insert column exists in the target table
                    if source_field.data_type() == target_column_data_type {
                        // save if type matches col(source_field_name)
                        Expr::Column(source_field.qualified_column())
                    } else {
                        // Add cast(source_col as target_type) if it doesn't exist
                        cast(
                            Expr::Column(source_field.qualified_column()),
                            target_column_data_type.clone(),
                        )
                    }
                } else {
                    // The specified column in the target table is missing from the insert
                    // then add cast(null as target_type)
                    cast(lit(ScalarValue::Null), target_column_data_type.clone())
                };

                expr.alias(target_column_name)
            })
            .collect();

        debug!("assignments: {:?}", &assignments);

        Ok(LogicalPlan::Projection(
            Projection::try_new(assignments, Arc::new(source_plan), None)
                .context(logical_planner::ExternalSnafu)?,
        ))
    }

    fn insert_to_plan(
        &self,
        sql_object_name: &ObjectName,
        sql_column_names: &[Ident],
        source: Box<Query>,
    ) -> Result<Plan> {
        // Transform subqueries
        let source_plan = SqlToRel::new(&self.schema_provider)
            .query_to_plan(*source, &mut HashMap::new())
            .context(logical_planner::ExternalSnafu)?;

        let table_name = normalize_sql_object_name(sql_object_name);
        let columns = sql_column_names
            .iter()
            .map(normalize_ident)
            .collect::<Vec<String>>();

        // Get the metadata of the target table
        let target_table = self.get_table_source(&table_name)?;
        let insert_columns = self.extract_column_names(columns.as_ref(), target_table.clone());

        // Check if the plan is legal
        semantic_check(insert_columns.as_ref(), &source_plan, target_table.clone())?;

        let final_source_logical_plan = self
            .add_projection_between_source_and_insert_node_if_necessary(
                target_table.clone(),
                source_plan,
                insert_columns,
            )?;

        let df_plan = table_write_plan_node(table_name, target_table, final_source_logical_plan)
            .context(logical_planner::ExternalSnafu)?;

        debug!("Insert plan:\n{}", df_plan.display_indent_schema());

        Ok(Plan::Query(QueryPlan { df_plan }))
    }

    fn drop_object_to_plan(&self, stmt: DropObject) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::Drop(DropPlan {
            if_exist: stmt.if_exist,
            object_name: normalize_sql_object_name(&stmt.object_name),
            obj_type: stmt.obj_type,
        })))
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    pub fn external_table_to_plan(&self, statement: AstCreateExternalTable) -> Result<Plan> {
        let df_planner = SqlToRel::new(&self.schema_provider);

        let logical_plan = df_planner
            .external_table_to_plan(statement)
            .context(ExternalSnafu)?;

        if let LogicalPlan::CreateExternalTable(plan) = logical_plan {
            return Ok(Plan::DDL(DDLPlan::CreateExternalTable(plan)));
        }

        Err(DataFusionError::Internal(
            UNEXPECTED_EXTERNAL_PLAN.to_string(),
        ))
        .context(ExternalSnafu)
    }

    pub fn create_table_to_plan(&self, statement: ASTCreateTable) -> Result<Plan> {
        let ASTCreateTable {
            name,
            if_not_exists,
            columns,
        } = statement;
        let id_generator = SeqIdGenerator::default();
        // all col: time col, tag col, field col
        // sys inner time column
        let mut schema: Vec<TableColumn> = Vec::with_capacity(columns.len() + 1);

        let time_col = TableColumn::new_time_column(id_generator.next_id() as ColumnId);
        // Append time column at the start
        schema.push(time_col);

        for column_opt in columns {
            let col_id = id_generator.next_id() as ColumnId;
            let column = Self::column_opt_to_table_column(column_opt, col_id)?;
            schema.push(column);
        }

        let mut column_name = HashSet::new();
        for col in schema.iter() {
            if !column_name.insert(col.name.as_str()) {
                return Err(LogicalPlannerError::Semantic {
                    err: "Field or Tag name should not have same".to_string(),
                });
            }
        }
        Ok(Plan::DDL(DDLPlan::CreateTable(CreateTable {
            schema,
            name: normalize_sql_object_name(&name),
            if_not_exists,
        })))
    }

    fn column_opt_to_table_column(column_opt: ColumnOption, id: ColumnId) -> Result<TableColumn> {
        Self::check_column_encoding(&column_opt)?;

        let col = if column_opt.is_tag {
            TableColumn::new_tag_column(id, normalize_ident(&column_opt.name))
        } else {
            TableColumn::new(
                id,
                normalize_ident(&column_opt.name),
                Self::make_data_type(&column_opt.data_type)?,
                column_opt.encoding.unwrap_or_default(),
            )
        };
        Ok(col)
    }

    fn database_to_describe(&self, statement: DescribeDatabaseOptions) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::DescribeDatabase(DescribeDatabase {
            database_name: normalize_sql_object_name(&statement.database_name),
        })))
    }

    fn table_to_describe(&self, opts: DescribeTableOptions) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::DescribeTable(DescribeTable {
            table_name: normalize_sql_object_name(&opts.table_name),
        })))
    }

    fn table_to_alter(&self, statement: ASTAlterTable) -> Result<Plan> {
        let table_name = normalize_sql_object_name(&statement.table_name);
        let table_schema = self.get_tskv_schema(&table_name)?;

        let alter_action = match statement.alter_action {
            ASTAlterTableAction::AddColumn { column } => {
                let table_column = Self::column_opt_to_table_column(column, ColumnId::default())?;
                if table_schema.contains_column(&table_column.name) {
                    return Err(LogicalPlannerError::Semantic {
                        err: format!(
                            "column {} already exists in table {}",
                            &table_column.name, table_schema.name
                        ),
                    });
                }
                AlterTableAction::AddColumn { table_column }
            }
            ASTAlterTableAction::DropColumn { ref column_name } => {
                let column_name = normalize_ident(column_name);
                let table_column = table_schema.column(&column_name).ok_or_else(|| {
                    LogicalPlannerError::Semantic {
                        err: format!(
                            "column {} not exists in table {}",
                            &table_schema.name, column_name
                        ),
                    }
                })?;

                if table_column.column_type.is_tag() {
                    return Err(LogicalPlannerError::Semantic {
                        err: "can't drop tag".to_string(),
                    });
                }

                if table_column.column_type.is_field() && table_schema.field_num() == 1 {
                    return Err(LogicalPlannerError::Semantic {
                        err: "table must hava a field".to_string(),
                    });
                }

                if table_column.column_type.is_time() {
                    return Err(LogicalPlannerError::Semantic {
                        err: format!("can't drop {} column", TIME_FIELD_NAME),
                    });
                }

                AlterTableAction::DropColumn { column_name }
            }

            ASTAlterTableAction::AlterColumnEncoding {
                ref column_name,
                encoding,
            } => {
                let column_name = normalize_ident(column_name);
                let column = table_schema.column(&column_name).ok_or_else(|| {
                    LogicalPlannerError::Semantic {
                        err: format!(
                            "column {} not exists in table {}",
                            column_name, &table_schema.name
                        ),
                    }
                })?;
                if column.column_type.is_tag() {
                    return Err(LogicalPlannerError::Semantic {
                        err: "tag does not support compression".to_string(),
                    });
                }

                if column.column_type.is_time() {
                    return Err(LogicalPlannerError::Semantic {
                        err: format!("can't modify codec type of {} column", TIME_FIELD_NAME),
                    });
                }
                let mut new_column = column.clone();
                new_column.encoding = encoding;

                AlterTableAction::AlterColumn {
                    column_name,
                    new_column,
                }
            }
        };
        Ok(Plan::DDL(DDLPlan::AlterTable(AlterTable {
            table_name,
            alter_action,
        })))
    }

    fn database_to_show(&self) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::ShowDatabases()))
    }

    fn table_to_show(&self, database: Option<ObjectName>) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::ShowTables(
            database.map(|db_name| normalize_sql_object_name(&db_name)),
        )))
    }
    fn show_tag_body(
        &self,
        body: ShowTagBody,
        projection_function: impl FnOnce(
            &TskvTableSchema,
            LogicalPlanBuilder,
            bool,
        ) -> Result<LogicalPlan>,
    ) -> Result<LogicalPlan> {
        // merge db a.b table b.c to a.b.c
        let table = match merge_object_name(body.database_name, Some(body.table)) {
            Some(table) => table,
            None => {
                return Err(LogicalPlannerError::Semantic {
                    err: "db conflict with table".to_string(),
                })
            }
        };

        let table_name = normalize_sql_object_name(&table);
        let table_source = self.get_table_source(&table_name)?;
        let table_schema = self.get_tskv_schema(&table_name)?;
        let table_df_schema = table_source
            .schema()
            .to_dfschema_ref()
            .context(logical_planner::ExternalSnafu)?;
        let sql_to_rel = SqlToRel::new(&self.schema_provider);

        // build from
        let mut plan_builder =
            LogicalPlanBuilder::scan(&table_schema.name, table_source.clone(), None)
                .context(ExternalSnafu)?;

        // build where
        let selection = match body.selection {
            Some(expr) => Some(
                sql_to_rel
                    .sql_to_rex(expr, &table_df_schema, &mut HashMap::new())
                    .context(logical_planner::ExternalSnafu)?,
            ),
            None => None,
        };

        // get all columns in expr
        let mut columns = HashSet::new();
        if let Some(selection) = &selection {
            expr_to_columns(selection, &mut columns).context(ExternalSnafu)?;
        }

        // check column
        check_show_series_expr(&columns, &table_schema)?;

        if let Some(selection) = selection {
            plan_builder = plan_builder
                .filter(selection)
                .context(logical_planner::ExternalSnafu)?;
        }

        // get where has time column
        let where_contain_time = columns
            .iter()
            .flat_map(|c: &Column| table_schema.column(&c.name))
            .any(|c: &TableColumn| c.column_type.is_time());

        // build projection
        let plan = projection_function(&table_schema, plan_builder, where_contain_time)?;

        // build order by
        let mut plan_builder = self.order_by(body.order_by, plan)?;

        // build limit
        if body.limit.is_some() || body.offset.is_some() {
            plan_builder =
                self.limit_offset_to_plan(body.limit, body.offset, plan_builder, &table_df_schema)?;
        }

        plan_builder.build().context(logical_planner::ExternalSnafu)
    }

    fn show_series_to_plan(&self, stmt: ASTShowSeries) -> Result<Plan> {
        let plan = self.show_tag_body(stmt.body, show_series_projection)?;
        Ok(Plan::Query(QueryPlan { df_plan: plan }))
    }

    fn show_tag_values(&self, stmt: ASTShowTagValues) -> Result<Plan> {
        // merge db a.b table b.c to a.b.c
        let plan = self.show_tag_body(stmt.body, |schema, plan_builder, where_contain_time| {
            show_tag_value_projections(schema, plan_builder, where_contain_time, stmt.with)
        })?;
        Ok(Plan::Query(QueryPlan { df_plan: plan }))
    }

    fn database_to_plan(&self, stmt: ASTCreateDatabase) -> Result<Plan> {
        let ASTCreateDatabase {
            name,
            if_not_exists,
            options,
        } = stmt;
        let options = self.make_database_option(options)?;
        Ok(Plan::DDL(DDLPlan::CreateDatabase(CreateDatabase {
            name: normalize_sql_object_name(&name),
            if_not_exists,
            options,
        })))
    }

    fn order_by(
        &self,
        exprs: Vec<ast::OrderByExpr>,
        plan: LogicalPlan,
    ) -> Result<LogicalPlanBuilder> {
        if exprs.is_empty() {
            return Ok(LogicalPlanBuilder::from(plan));
        }
        let schema = plan.schema();
        let sql_to_rel = SqlToRel::new(&self.schema_provider);

        let mut sort_exprs = Vec::new();
        for ast::OrderByExpr {
            expr,
            asc,
            nulls_first,
        } in exprs
        {
            let expr = sql_to_rel
                .sql_to_rex(expr, schema, &mut HashMap::new())
                .context(logical_planner::ExternalSnafu)?;
            let asc = asc.unwrap_or(true);
            let sort_expr = Expr::Sort {
                expr: Box::new(expr),
                asc,
                // when asc is true, by default nulls last to be consistent with postgres
                // postgres rule: https://www.postgresql.org/docs/current/queries-order.html
                nulls_first: nulls_first.unwrap_or(!asc),
            };
            sort_exprs.push(sort_expr);
        }

        LogicalPlanBuilder::from(plan)
            .sort(sort_exprs)
            .context(logical_planner::ExternalSnafu)
    }

    fn limit_offset_to_plan(
        &self,
        limit: Option<ast::Expr>,
        offset: Option<ast::Offset>,
        plan_builder: LogicalPlanBuilder,
        schema: &DFSchema,
    ) -> Result<LogicalPlanBuilder> {
        let sql_to_rel = SqlToRel::new(&self.schema_provider);
        let skip = match offset {
            Some(offset) => match sql_to_rel
                .sql_to_rex(offset.value, schema, &mut HashMap::new())
                .context(logical_planner::ExternalSnafu)?
            {
                Expr::Literal(ScalarValue::Int64(Some(m))) => {
                    if m < 0 {
                        return Err(LogicalPlannerError::Semantic {
                            err: format!("OFFSET must be >= 0, '{}' was provided.", m),
                        });
                    } else {
                        m as usize
                    }
                }
                _ => {
                    return Err(LogicalPlannerError::Semantic {
                        err: "The OFFSET clause must be a constant of BIGINT type".to_string(),
                    })
                }
            },
            None => 0,
        };

        let fetch = match limit {
            Some(exp) => match sql_to_rel
                .sql_to_rex(exp, schema, &mut HashMap::new())
                .context(logical_planner::ExternalSnafu)?
            {
                Expr::Literal(ScalarValue::Int64(Some(n))) => {
                    if n < 0 {
                        return Err(LogicalPlannerError::Semantic {
                            err: format!("LIMIT must be >= 0, '{}' was provided.", n),
                        });
                    } else {
                        Some(n as usize)
                    }
                }
                _ => {
                    return Err(LogicalPlannerError::Semantic {
                        err: "The LIMIT clause must be a constant of BIGINT type".to_string(),
                    })
                }
            },
            None => None,
        };

        let plan_builder = plan_builder
            .limit(skip, fetch)
            .context(logical_planner::ExternalSnafu)?;
        Ok(plan_builder)
    }

    fn database_to_alter(&self, stmt: ASTAlterDatabase) -> Result<Plan> {
        let ASTAlterDatabase { name, options } = stmt;
        let options = self.make_database_option(options)?;
        Ok(Plan::DDL(DDLPlan::AlterDatabase(AlterDatabase {
            database_name: normalize_sql_object_name(&name),
            database_options: options,
        })))
    }

    fn make_database_option(&self, options: ASTDatabaseOptions) -> Result<DatabaseOptions> {
        let mut plan_options = DatabaseOptions::default();
        if let Some(ttl) = options.ttl {
            plan_options.with_ttl(self.str_to_duration(&ttl)?);
        }
        if let Some(replica) = options.replica {
            plan_options.with_replica(replica);
        }
        if let Some(shard_num) = options.shard_num {
            plan_options.with_shard_num(shard_num);
        }
        if let Some(vnode_duration) = options.vnode_duration {
            plan_options.with_vnode_duration(self.str_to_duration(&vnode_duration)?);
        }
        if let Some(precision) = options.precision {
            plan_options.with_precision(Precision::new(&precision).ok_or(
                LogicalPlannerError::Semantic {
                    err: format!(
                        "{} is not a valid precision, use like 'ms', 'us', 'ns'",
                        precision
                    ),
                },
            )?);
        }
        Ok(plan_options)
    }

    fn str_to_duration(&self, text: &str) -> Result<Duration> {
        let duration = match Duration::new(text) {
            None => {
                return Err(LogicalPlannerError::Semantic {
                    err: format!(
                        "{} is not a valid precision, use like 'ms', 'us', 'ns'",
                        text
                    ),
                })
            }
            Some(v) => v,
        };
        Ok(duration)
    }

    fn make_data_type(data_type: &SQLDataType) -> Result<ColumnType> {
        match data_type {
            // todo : should support get time unit for database
            SQLDataType::Timestamp(_) => Ok(ColumnType::Time),
            SQLDataType::BigInt(_) => Ok(ColumnType::Field(ValueType::Integer)),
            SQLDataType::UnsignedBigInt(_) => Ok(ColumnType::Field(ValueType::Unsigned)),
            SQLDataType::Double => Ok(ColumnType::Field(ValueType::Float)),
            SQLDataType::String => Ok(ColumnType::Field(ValueType::String)),
            SQLDataType::Boolean => Ok(ColumnType::Field(ValueType::Boolean)),
            _ => Err(LogicalPlannerError::Semantic {
                err: format!("Unexpected data type {}", data_type),
            }),
        }
    }

    fn check_column_encoding(column: &ColumnOption) -> Result<()> {
        // tag无压缩，直接返回
        if column.is_tag {
            return Ok(());
        }
        // 数据类型 -> 压缩方式 校验
        let encoding = column.encoding.unwrap_or_default();
        let is_ok = match column.data_type {
            SQLDataType::Timestamp(_) => encoding.is_timestamp_encoding(),
            SQLDataType::BigInt(_) => encoding.is_bigint_encoding(),
            SQLDataType::UnsignedBigInt(_) => encoding.is_unsigned_encoding(),
            SQLDataType::Double => encoding.is_double_encoding(),
            SQLDataType::String => encoding.is_string_encoding(),
            SQLDataType::Boolean => encoding.is_bool_encoding(),
            _ => false,
        };
        if !is_ok {
            return Err(LogicalPlannerError::Semantic {
                err: format!(
                    "Unsupported encoding type {:?} for {}",
                    column.encoding, column.data_type
                ),
            });
        }
        Ok(())
    }

    fn extract_column_names(
        &self,
        columns: &[String],
        target_table_source_ref: Arc<dyn TableSource>,
    ) -> Vec<String> {
        if columns.is_empty() {
            target_table_source_ref
                .schema()
                .fields()
                .iter()
                .map(|e| e.name().clone())
                .collect()
        } else {
            columns.to_vec()
        }
    }

    fn get_table_source(&self, table_name: &str) -> Result<Arc<dyn TableSource>> {
        let table_ref = TableReference::from(table_name);
        self.schema_provider
            .get_table_provider(table_ref)
            .context(logical_planner::ExternalSnafu)
    }

    fn get_table_provider(&self, table_name: &str) -> Result<Arc<dyn TableProvider>> {
        let table_source = self.get_table_source(table_name)?;
        source_as_provider(&table_source)
            .map_err(|_| MetadataError::InvalidSchema {
                error_msg: format!(
                    "can't convert table source {} to table provider ",
                    table_name
                ),
            })
            .context(MetadataSnafu)
    }

    fn get_tskv_schema(&self, table_name: &str) -> Result<TskvTableSchemaRef> {
        Ok(self
            .get_table_provider(table_name)?
            .as_any()
            .downcast_ref::<ClusterTable>()
            .ok_or_else(|| MetadataError::TableIsNotTsKv {
                table_name: table_name.to_string(),
            })
            .context(MetadataSnafu)?
            .table_schema())
    }
}

fn semantic_check(
    insert_columns: &[String],
    source_plan: &LogicalPlan,
    target_table: Arc<dyn TableSource>,
) -> Result<()> {
    let target_table_schema = target_table.schema();
    let target_table_fields = target_table_schema.fields();

    let source_field_num = source_plan.schema().fields().len();
    let insert_field_num = insert_columns.len();
    let target_table_field_num = target_table_fields.len();

    if insert_field_num > source_field_num {
        return Err(LogicalPlannerError::Semantic {
            err: MISMATCHED_COLUMNS.to_string(),
        });
    }

    if insert_field_num == 0 && source_field_num != target_table_field_num {
        return Err(LogicalPlannerError::Semantic {
            err: MISMATCHED_COLUMNS.to_string(),
        });
    }
    // The target table must contain all insert fields
    for insert_col in insert_columns {
        target_table_fields
            .iter()
            .find(|e| e.name() == insert_col)
            .ok_or_else(|| LogicalPlannerError::Semantic {
                err: format!(
                    "{} {}, expected: {}",
                    MISSING_COLUMN,
                    insert_col,
                    target_table_fields
                        .iter()
                        .map(|e| e.name().as_str())
                        .collect::<Vec<&str>>()
                        .join(",")
                ),
            })?;
    }

    Ok(())
}

// check
// show series can't include field column
fn check_show_series_expr(columns: &HashSet<Column>, table_schema: &TskvTableSchema) -> Result<()> {
    for column in columns.iter() {
        match table_schema.column(&column.name) {
            Some(table_column) => {
                if table_column.column_type.is_field() {
                    return Err(LogicalPlannerError::Semantic {
                        err: format!(
                            "SHOW SERIES does not support where clause contains field {}",
                            column
                        ),
                    });
                }
            }

            None => {
                return Err(LogicalPlannerError::Semantic {
                    err: format!("column {} does not exits", column),
                })
            }
        }
    }

    Ok(())
}

fn show_series_projection(
    table_schema: &TskvTableSchema,
    mut plan_builder: LogicalPlanBuilder,
    where_contain_time: bool,
) -> Result<LogicalPlan> {
    let tags = table_schema
        .columns()
        .iter()
        .filter(|c| c.column_type.is_tag())
        .collect::<Vec<&TableColumn>>();

    // If the time column is included,
    //   all field columns will be scanned at rewrite_tag_scan,
    //   so this projection needs to be added
    if where_contain_time {
        let exp = tags
            .iter()
            .map(|tag| table_column_to_expr(table_schema, tag))
            .collect::<Vec<Expr>>();
        plan_builder = plan_builder
            .project(exp)
            .context(logical_planner::ExternalSnafu)?;
    };

    plan_builder = plan_builder
        .distinct()
        .context(logical_planner::ExternalSnafu)?;

    // concat tag_key=tag_value projection
    let tag_concat_expr_iter = tags.iter().map(|tag| {
        let column_expr = Box::new(Expr::Column(Column::new(
            Some(&table_schema.name),
            &tag.name,
        )));
        let is_null_expr = Box::new(column_expr.clone().is_null());
        let when_then_expr = vec![(is_null_expr, Box::new(lit(ScalarValue::Null)))];
        let else_expr = Some(Box::new(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit(format!("{}=", &tag.name))),
            op: Operator::StringConcat,
            right: column_expr,
        })));
        Expr::Case(Case::new(None, when_then_expr, else_expr))
    });

    let concat_ws_args = iter::once(lit(","))
        .chain(iter::once(lit(&table_schema.name)))
        .chain(tag_concat_expr_iter)
        .collect::<Vec<Expr>>();
    let concat_ws = Expr::ScalarFunction {
        fun: BuiltinScalarFunction::ConcatWithSeparator,
        args: concat_ws_args,
    }
    .alias("key");
    plan_builder
        .project(iter::once(concat_ws))
        .context(logical_planner::ExternalSnafu)?
        .build()
        .context(logical_planner::ExternalSnafu)
}

fn show_tag_value_projections(
    table_schema: &TskvTableSchema,
    mut plan_builder: LogicalPlanBuilder,
    where_contain_time: bool,
    with: With,
) -> Result<LogicalPlan> {
    let mut tag_key_filter: Box<dyn FnMut(&TableColumn) -> bool> = match with {
        With::Equal(ident) => Box::new(move |column| normalize_ident(&ident).eq(&column.name)),
        With::UnEqual(ident) => Box::new(move |column| normalize_ident(&ident).ne(&column.name)),
        With::In(idents) => Box::new(move |column| {
            idents
                .iter()
                .map(normalize_ident)
                .any(|name| column.name.eq(&name))
        }),
        With::NotIn(idents) => Box::new(move |column| {
            idents
                .iter()
                .map(normalize_ident)
                .all(|name| column.name.ne(&name))
        }),
        _ => {
            return Err(LogicalPlannerError::NotImplemented {
                err: "Not implemented Match, UnMatch".to_string(),
            })
        }
    };

    let tags = table_schema
        .columns()
        .iter()
        .filter(|column| column.column_type.is_tag())
        .filter(|column| tag_key_filter(column))
        .collect::<Vec<&TableColumn>>();

    if tags.is_empty() {
        return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![
                        DFField::new(None, "key", DataType::Utf8, false),
                        DFField::new(None, "value", DataType::Utf8, false),
                    ],
                    HashMap::new(),
                )
                .context(logical_planner::ExternalSnafu)?,
            ),
        }));
    }

    // If the time column is included,
    //   all field columns will be scanned at rewrite_tag_scan,
    //   so this projection needs to be added
    if where_contain_time {
        let exprs = tags
            .iter()
            .map(|tag| table_column_to_expr(table_schema, tag))
            .collect::<Vec<Expr>>();

        plan_builder = plan_builder
            .project(exprs)
            .context(logical_planner::ExternalSnafu)?;
    };

    plan_builder = plan_builder
        .distinct()
        .context(logical_planner::ExternalSnafu)?;

    let mut projections = Vec::new();
    for tag in tags {
        let key_column = lit(&tag.name).alias("key");
        let value_column = table_column_to_expr(table_schema, tag).alias("value");
        let projection = plan_builder
            .project(vec![key_column, value_column.clone()])
            .context(logical_planner::ExternalSnafu)?;
        let filter_expr = value_column.is_not_null();
        let projection = projection
            .filter(filter_expr)
            .context(logical_planner::ExternalSnafu)?
            .build()
            .context(logical_planner::ExternalSnafu)?;
        projections.push(Arc::new(projection));
    }
    let df_schema = projections[0].schema().clone();

    let union = LogicalPlan::Union(Union {
        inputs: projections,
        schema: df_schema,
        alias: None,
    });

    Ok(union)
}

fn table_column_to_expr(table_schema: &TskvTableSchema, column: &TableColumn) -> Expr {
    Expr::Column(Column::new(
        Some(table_schema.name.to_string()),
        column.name.to_string(),
    ))
}

fn table_write_plan_node(
    table_name: String,
    target_table: Arc<dyn TableSource>,
    input: LogicalPlan,
) -> std::result::Result<LogicalPlan, DataFusionError> {
    // output variable for insert operation
    let expr = input
        .schema()
        .fields()
        .iter()
        .last()
        .map(|e| Expr::Column(e.qualified_column()));

    debug_assert!(
        expr.is_some(),
        "invalid table write node's input logical plan"
    );

    let expr = unsafe { expr.unwrap_unchecked() };

    let affected_row_expr = affected_row_expr(expr);

    // construct table writer logical node
    let node = Arc::new(TableWriterPlanNode::try_new(
        table_name,
        target_table,
        Arc::new(input),
        vec![affected_row_expr],
    )?);

    let df_plan = LogicalPlan::Extension(Extension { node });

    let group_expr: Vec<Expr> = vec![];

    LogicalPlanBuilder::from(df_plan)
        .aggregate(group_expr, vec![merge_affected_row_expr()])?
        .build()
}

impl<S: ContextProvider> LogicalPlanner for SqlPlaner<S> {
    fn create_logical_plan(
        &self,
        statement: ExtStatement,
        _session: &IsiphoSessionCtx,
    ) -> Result<Plan> {
        self.statement_to_plan(statement)
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::ExtParser;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::logical_expr::{Aggregate, AggregateUDF, ScalarUDF, TableSource};
    use datafusion::sql::planner::ContextProvider;
    use datafusion::sql::TableReference;
    use std::any::Any;
    use std::ops::Deref;
    use std::sync::Arc;

    use super::*;
    use datafusion::error::Result;
    use models::codec::Encoding;

    #[derive(Debug)]
    struct MockContext {}

    impl ContextProvider for MockContext {
        fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
            let schema = match name.table() {
                "test_tb" => Ok(Schema::new(vec![
                    Field::new("field_int", DataType::Int32, false),
                    Field::new("field_string", DataType::Utf8, false),
                ])),
                _ => {
                    unimplemented!("use test_tb for test")
                }
            };
            match schema {
                Ok(tb) => Ok(Arc::new(TestTable::new(Arc::new(tb)))),
                Err(e) => Err(e),
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            unimplemented!()
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            unimplemented!()
        }

        fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
            unimplemented!()
        }
    }
    struct TestTable {
        table_schema: SchemaRef,
    }

    impl TestTable {
        fn new(table_schema: SchemaRef) -> Self {
            Self { table_schema }
        }
    }

    impl TableSource for TestTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.table_schema.clone()
        }
    }

    #[test]
    fn test_drop() {
        let sql = "drop table if exists test_tb";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
        if let Plan::DDL(DDLPlan::Drop(drop)) = plan {
            println!("{:?}", drop);
        } else {
            panic!("expected drop plan")
        }
    }

    #[test]
    fn test_create_table() {
        let sql = "CREATE TABLE IF NOT EXISTS test\
            (column1 BIGINT CODEC(DELTA),\
            column2 STRING CODEC(GZIP),\
            column3 BIGINT UNSIGNED CODEC(NULL),\
            column4 BOOLEAN,\
            column5 DOUBLE CODEC(GORILLA),\
            TAGS(column6, column7))";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
        if let Plan::DDL(DDLPlan::CreateTable(create)) = plan {
            assert_eq!(
                create,
                CreateTable {
                    schema: vec![
                        TableColumn {
                            id: 0,
                            name: "time".to_string(),
                            column_type: ColumnType::Time,
                            encoding: Encoding::Default
                        },
                        TableColumn {
                            id: 1,
                            name: "column6".to_string(),
                            column_type: ColumnType::Tag,
                            encoding: Encoding::Default,
                        },
                        TableColumn {
                            id: 2,
                            name: "column7".to_string(),
                            column_type: ColumnType::Tag,
                            encoding: Encoding::Default,
                        },
                        TableColumn {
                            id: 3,
                            name: "column1".to_string(),
                            column_type: ColumnType::Field(ValueType::Integer),
                            encoding: Encoding::Delta,
                        },
                        TableColumn {
                            id: 4,
                            name: "column2".to_string(),
                            column_type: ColumnType::Field(ValueType::String),
                            encoding: Encoding::Gzip,
                        },
                        TableColumn {
                            id: 5,
                            name: "column3".to_string(),
                            column_type: ColumnType::Field(ValueType::Unsigned),
                            encoding: Encoding::Null,
                        },
                        TableColumn {
                            id: 6,
                            name: "column4".to_string(),
                            column_type: ColumnType::Field(ValueType::Boolean),
                            encoding: Encoding::Default,
                        },
                        TableColumn {
                            id: 7,
                            name: "column5".to_string(),
                            column_type: ColumnType::Field(ValueType::Float),
                            encoding: Encoding::Gorilla,
                        }
                    ],
                    name: "test".to_string(),
                    if_not_exists: true
                }
            );
        } else {
            panic!("expected create table plan")
        }
    }

    #[test]
    fn test_create_database() {
        let sql = "CREATE DATABASE test WITH TTL '10' SHARD 5 VNODE_DURATION '3d' REPLICA 10 PRECISION 'us';";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
        if let Plan::DDL(DDLPlan::CreateDatabase(create)) = plan {
            let ans = format!("{:?}", create);
            println!("{ans}");
            let expected = r#"CreateDatabase { name: "test", if_not_exists: false, options: DatabaseOptions { ttl: Some(Duration { time_num: 10, unit: Day }), shard_num: Some(5), vnode_duration: Some(Duration { time_num: 3, unit: Day }), replica: Some(10), precision: Some(US) } }"#;
            assert_eq!(ans, expected);
        } else {
            panic!("expected create table plan")
        }
    }

    #[test]
    #[should_panic(expected = "Field or Tag name should not have same")]
    fn test_create_table_filed_name_same() {
        let sql = "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,presssure DOUBLE,presssure DOUBLE,TAGS(station));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "Field or Tag name should not have same")]
    fn test_create_table_tag_name_same() {
        let sql = "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,presssure DOUBLE,TAGS(station,station));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "Field or Tag name should not have same")]
    fn test_create_table_tag_field_same_name() {
        let sql = "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,presssure DOUBLE,TAGS(station,presssure));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
    }

    #[test]
    fn test_insert_select() {
        let sql = "insert test_tb(field_int, field_string)
                         select column1, column2
                         from
                         (values
                             (7, '7a'));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();

        match plan {
            Plan::Query(QueryPlan {
                df_plan: LogicalPlan::Aggregate(Aggregate { input, .. }),
            }) => match input.as_ref() {
                LogicalPlan::Extension(Extension { node }) => {
                    match node.as_any().downcast_ref::<TableWriterPlanNode>() {
                        Some(TableWriterPlanNode {
                            target_table_name, ..
                        }) => {
                            assert_eq!(target_table_name.deref(), "test_tb");
                        }
                        _ => panic!(),
                    }
                }
                _ => panic!(),
            },
            _ => panic!(),
        }
    }
}
