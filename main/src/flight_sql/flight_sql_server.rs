use arrow_flight::sql::{ActionCreatePreparedStatementResult, ProstAnyExt, SqlInfo};
use arrow_flight::{
    flight_service_server, Action, FlightData, FlightEndpoint, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use chrono::format::Item;
use dashmap::DashMap;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use futures::Stream;
use http_protocol::header::{AUTHORIZATION, BASIC_PREFIX, BEARER_PREFIX, DB, TENANT};
use models::auth::user::{User, UserInfo};
use models::oid::{MemoryOidGenerator, Oid, OidGenerator, UuidGenerator};
use moka::sync::Cache;
use prost::Message;
use prost_types::Any;
use query::dispatcher::manager::SimpleQueryDispatcher;
use query::instance::Cnosdbms;
use spi::query::dispatcher::QueryDispatcher;
use spi::query::execution::Output;
use spi::server::dbms::{DBMSRef, DatabaseManagerSystem, DatabaseManagerSystemMock};
use spi::service::protocol::{Context, ContextBuilder, Query, QueryHandle, QueryId};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use trace::debug;

use arrow_flight::{
    flight_service_server::FlightService,
    flight_service_server::FlightServiceServer,
    sql::{
        server::FlightSqlService, ActionClosePreparedStatementRequest,
        ActionCreatePreparedStatementRequest, CommandGetCatalogs, CommandGetCrossReference,
        CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
        CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
        CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate,
        TicketStatementQuery,
    },
    utils as flight_utils, FlightDescriptor, FlightInfo,
};

use crate::flight_sql::auth_middleware::AuthResult;
use crate::flight_sql::utils;
use crate::http::header::Header;

use super::auth_middleware::CallHeaderAuthenticator;

pub struct FlightSqlServiceImpl<T> {
    instance: DBMSRef,
    authenticator: T,
    id_generator: UuidGenerator,

    result_cache: Cache<Vec<u8>, Output>,
}

impl<T> FlightSqlServiceImpl<T> {
    pub fn new(instance: DBMSRef, authenticator: T) -> Self {
        let result_cache = Cache::builder()
            .thread_pool_enabled(false)
            // Time to live (TTL): 2 minutes
            // The query results are only cached for 2 minutes and expire after 2 minutes
            .time_to_live(Duration::from_secs(2 * 60))
            .build();

        Self {
            instance,
            authenticator,
            id_generator: Default::default(),
            result_cache,
        }
    }
}

impl<T> FlightSqlServiceImpl<T>
where
    T: CallHeaderAuthenticator + Send + Sync + 'static,
{
    async fn precess_statement_query_req(
        &self,
        sql: String,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let (result_ident, query_result) = self.auth_and_execute(sql, request.metadata()).await?;

        // get result metadata
        let output = query_result.result();
        let schema = output.schema();
        let total_records = output.num_rows();

        // cache result wait cli fetching
        self.result_cache.insert(result_ident.clone(), output);

        // construct response start
        let flight_info = self.construct_flight_info(
            result_ident,
            schema.as_ref(),
            total_records as i64,
            request.into_inner(),
        )?;

        Ok(Response::new(flight_info))
    }

    /// 1. auth request
    /// 2. execute query
    async fn auth_and_execute(
        &self,
        sql: String,
        metadata: &MetadataMap,
    ) -> Result<(Vec<u8>, QueryHandle), Status> {
        let auth_result = self.authenticator.authenticate(metadata)?;
        let user = auth_result.identity();

        // construct context by user_info and headers(parse tenant & default database)
        let ctx = self.construct_context(user, metadata)?;

        // execute sql
        let query_result = self.execute(sql, ctx).await?;

        // generate result identifier
        let result_ident = self.id_generator.next_id().to_le_bytes().to_vec();

        Ok((result_ident, query_result))
    }

    fn construct_flight_info(
        &self,
        result_ident: impl Into<Vec<u8>>,
        schema: &Schema,
        total_records: i64,
        flight_descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let option = IpcWriteOptions::default();
        let ipc_message = SchemaAsIpc::new(schema, &option)
            .try_into()
            .map_err(|e| Status::internal(format!("{}", e)))?;
        let tkt = TicketStatementQuery {
            statement_handle: result_ident.into(),
        };
        let endpoint = utils::endpoint(tkt, Default::default()).map_err(Status::internal)?;

        let flight_info = FlightInfo::new(
            ipc_message,
            Some(flight_descriptor),
            vec![endpoint],
            total_records,
            -1,
        );

        Ok(flight_info)
    }

    fn construct_context(
        &self,
        user_info: User,
        metadata: &MetadataMap,
    ) -> Result<Context, Status> {
        // parse tenant & default database
        let tenant = utils::get_value_from_header(metadata, TENANT, "");
        let db = utils::get_value_from_header(metadata, DB, "");
        let ctx = ContextBuilder::new(user_info)
            .with_tenant(tenant)
            .with_database(db)
            .build();

        Ok(ctx)
    }

    async fn execute(&self, sql: String, ctx: Context) -> Result<QueryHandle, Status> {
        // execute sql
        let query = Query::new(ctx, sql);
        let query_result = self.instance.execute(&query).await.map_err(|e| {
            // TODO convert error message
            Status::internal(format!("{}", e))
        })?;

        Ok(query_result)
    }

    fn fetch_result_set(
        &self,
        statement_handle: &[u8],
    ) -> Result<Vec<Result<FlightData, Status>>, Status> {
        let output = self.result_cache.get(statement_handle).ok_or_else(|| {
            Status::internal(format!(
                "The result of query({:?}) does not exist or has expired",
                statement_handle
            ))
        })?;

        let options = IpcWriteOptions::default();

        let schema = std::iter::once(Ok(
            SchemaAsIpc::new(output.schema().as_ref(), &options).into()
        ));

        let batches = output
            .chunk_result()
            .iter()
            .enumerate()
            .flat_map(|(counter, batch)| {
                let (dictionary_flight_data, mut batch_flight_data) =
                    flight_utils::flight_data_from_arrow_batch(batch, &options);

                // Only the record batch's FlightData gets app_metadata
                let metadata = counter.to_string().into_bytes();
                batch_flight_data.app_metadata = metadata;

                dictionary_flight_data
                    .into_iter()
                    .chain(std::iter::once(batch_flight_data))
                    .map(Ok)
            });

        let result = schema.chain(batches).collect::<Vec<_>>();

        Ok(result)
    }

    fn fetch_affected_rows_count(&self, statement_handle: &[u8]) -> Result<i64, Status> {
        let result_set = self.result_cache.get(statement_handle).ok_or_else(|| {
            Status::internal(format!(
                "The result of query({:?}) does not exist or has expired",
                statement_handle
            ))
        })?;

        Ok(result_set.affected_rows())
    }
}

/// use jdbc to execute statement query:
///
/// e.g.
/// ```java
/// .   final Properties properties = new Properties();
/// .   
/// .   properties.put(ArrowFlightConnectionProperty.USER.camelName(), user);
/// .   properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), password);
/// .   properties.put("tenant", "cnosdb");
/// .   //        properties.put("db", "db1");
/// .   properties.put("useEncryption", false);
/// .   
/// .   try (Connection connection = DriverManager.getConnection(
/// .           "jdbc:arrow-flight-sql://" + host + ":" + port, properties);
/// .        Statement stmt = connection.createStatement()) {
/// .   //            assert stmt.execute("DROP DATABASE IF EXISTS oceanic_station;");
/// .   //            assert stmt.execute("CREATE DATABASE IF NOT EXISTS oceanic_station;");
/// .       stmt.execute("CREATE TABLE IF NOT EXISTS air\n" +
/// .               "(\n" +
/// .               "    visibility  DOUBLE,\n" +
/// .               "    temperature DOUBLE,\n" +
/// .               "    pressure    DOUBLE,\n" +
/// .               "    TAGS(station)\n" +
/// .               ");");
/// .       stmt.execute("INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES\n" +
/// .               "    (1666165200290401000, 'XiaoMaiDao', 56, 69, 77);");
/// .   
/// .       ResultSet resultSet = stmt.executeQuery("select * from air limit 1;");
/// .   
/// .       while (resultSet.next()) {
/// .           assertNotNull(resultSet.getString(1));
/// .       }
/// .   }
/// ```
/// 1. do_handshake: basic auth -> baerar token
/// 2. do_action_create_prepared_statement: sql(baerar token) -> sql
/// 3. do_put_prepared_statement_update: not use
/// 4. get_flight_info_prepared_statement: sql(baerar token) -> address of resut set
/// 5. do_get_statement: address of resut set(baerar token) -> resut set stream
/// ```
///
/// use flight sql to execute statement query:
///
/// e.g.
/// ```
/// 1. do_handshake: basic auth -> baerar token
/// 4. get_flight_info_statement: sql(baerar token) -> address of resut set
/// 5. do_get_statement: address of resut set(baerar token) -> resut set stream
/// ```
#[tonic::async_trait]
impl<T> FlightSqlService for FlightSqlServiceImpl<T>
where
    T: CallHeaderAuthenticator + Send + Sync + 'static,
{
    type FlightService = FlightSqlServiceImpl<T>;

    /// Perform client authentication
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        debug!("do_handshake: {:?}", request);

        let auth_result = self.authenticator.authenticate(request.metadata())?;

        let output: Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>> =
            Box::pin(futures::stream::empty());
        let mut resp = Response::new(output);

        // Append the token generated by authenticator to the response header
        auth_result.append_to_outgoing_headers(resp.metadata_mut())?;

        return Ok(resp);
    }

    /// Execute an ad-hoc SQL query.
    ///
    /// Return the address of the result set,
    /// waiting to call [`Self::do_get_statement`] to get the result set.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_statement: query: {:?}, request: {:?}",
            query, request
        );

        let CommandStatementQuery { query: sql } = query;

        self.precess_statement_query_req(sql, request).await
    }

    /// Fetch meta of the prepared statement.
    ///
    /// The prepared statement can be reused after fetching results.
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        let CommandPreparedStatementQuery {
            prepared_statement_handle,
        } = query;

        // get metadata of result from cache
        let output = self
            .result_cache
            .get(&prepared_statement_handle)
            .ok_or_else(|| {
                Status::internal(format!(
                    "The result of query({:?}) does not exist or has expired",
                    prepared_statement_handle
                ))
            })?;
        let schema = output.schema();
        let total_records = output.num_rows();

        // construct response start
        let flight_info = self.construct_flight_info(
            prepared_statement_handle,
            schema.as_ref(),
            total_records as i64,
            request.into_inner(),
        )?;

        Ok(Response::new(flight_info))
    }

    /// TODO support
    /// wait for https://github.com/cnosdb/cnosdb/issues/642
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_catalogs: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_catalogs not implemented",
        ))
    }

    /// TODO support
    /// wait for https://github.com/cnosdb/cnosdb/issues/642
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_schemas: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_schemas not implemented",
        ))
    }

    /// TODO support
    /// wait for https://github.com/cnosdb/cnosdb/issues/642
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_tables: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_tables not implemented",
        ))
    }

    /// TODO support
    /// wait for https://github.com/cnosdb/cnosdb/issues/642
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_table_types: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_table_types not implemented",
        ))
    }

    /// Fetch the ad-hoc SQL query's result set
    ///
    /// [`TicketStatementQuery`] is the result obtained after calling [`Self::get_flight_info_statement`]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_statement: query: {:?}, request: {:?}",
            ticket, request
        );

        let TicketStatementQuery { statement_handle } = ticket;

        let batches = self.fetch_result_set(&statement_handle)?;

        let output = futures::stream::iter(batches);

        // clear cache of this query
        self.result_cache.invalidate(&statement_handle);

        Ok(Response::new(
            Box::pin(output) as <Self as FlightService>::DoGetStream
        ))
    }

    /// Fetch the prepared SQL query's result set
    ///
    /// [`CommandPreparedStatementQuery`] is the result obtained after calling [`Self::get_flight_info_prepared_statement`]
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        let CommandPreparedStatementQuery {
            prepared_statement_handle,
        } = query;

        let batches = self.fetch_result_set(&prepared_statement_handle)?;

        let output = futures::stream::iter(batches);

        // clear cache of this query
        self.result_cache.invalidate(&prepared_statement_handle);

        Ok(Response::new(
            Box::pin(output) as <Self as FlightService>::DoGetStream
        ))
    }

    /// TODO support
    /// wait for https://github.com/cnosdb/cnosdb/issues/642
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_catalogs: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_catalogs not implemented"))
    }

    /// TODO support
    /// wait for https://github.com/cnosdb/cnosdb/issues/642
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_schemas: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_schemas not implemented"))
    }

    /// TODO support
    /// wait for https://github.com/cnosdb/cnosdb/issues/642
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_tables: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_tables not implemented"))
    }

    /// TODO support
    /// wait for https://github.com/cnosdb/cnosdb/issues/642
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_table_types: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_table_types not implemented"))
    }

    /// Execute an ad-hoc SQL query and return the number of affected rows.
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        debug!(
            "do_put_statement_update: query: {:?}, request: {:?}",
            ticket, request
        );

        let CommandStatementUpdate { query } = ticket;

        let metadata = request.metadata();

        let (_, query_result) = self.auth_and_execute(query, metadata).await?;

        let affected_rows = query_result.result().affected_rows();

        Ok(affected_rows)
    }

    /// Execute the query and return the number of affected rows.
    /// The prepared statement can be reused afterwards.
    ///
    /// Prepared statement is not supported,
    /// because ad-hoc statement of flight jdbc needs to call this interface, so it is simple to implement
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        debug!(
            "do_put_prepared_statement_update: query: {:?}, request: {:?}",
            query, request
        );

        let CommandPreparedStatementUpdate {
            ref prepared_statement_handle,
        } = query;

        let rows_count = self.fetch_affected_rows_count(prepared_statement_handle)?;

        Ok(rows_count)
    }

    /// Prepared statement is not supported,
    /// because ad-hoc statement of flight jdbc needs to call this interface,
    /// so directly return the sql as the result.
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        debug!(
            "do_action_create_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        let ActionCreatePreparedStatementRequest { query: sql } = query;
        let metadata = request.metadata();

        let user_info = self.authenticator.authenticate(metadata)?.identity();

        // construct context by user_info and headers(parse tenant & default database)
        let ctx = self.construct_context(user_info, metadata)?;

        // execute sql
        let query_result = self.execute(sql, ctx).await?;

        // generate result identifier
        let result_ident = self.id_generator.next_id().to_le_bytes().to_vec();

        // get result metadata
        let output = query_result.result();
        let schema = output.schema();
        let total_records = output.num_rows();

        // cache result wait cli fetching
        self.result_cache.insert(result_ident.clone(), output);

        // construct response start
        let IpcMessage(dataset_schema) = IpcMessage::try_from(SchemaAsIpc::new(
            schema.as_ref(),
            &IpcWriteOptions::default(),
        ))
        .map_err(|e| Status::internal(format!("{}", e)))?;
        let result = ActionCreatePreparedStatementResult {
            prepared_statement_handle: result_ident,
            dataset_schema,
            ..Default::default()
        };

        Ok(result)
    }

    /// Close a previously created prepared statement.
    ///
    /// Empty logic, because we not save created prepared statement.
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) {
        debug!(
            "do_action_close_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );
    }

    /// not support
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        debug!(
            "do_put_prepared_statement_query: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_put_prepared_statement_query not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_sql_info: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_sql_info not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_primary_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_primary_keys not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_exported_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_exported_keys not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_imported_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_cross_reference: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    /// not support
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_sql_info: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_sql_info not implemented"))
    }

    /// not support
    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_primary_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_primary_keys not implemented"))
    }

    /// not support
    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_exported_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_get_exported_keys not implemented",
        ))
    }

    /// not support
    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_imported_keys not implemented",
        ))
    }

    /// not support
    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_cross_reference not implemented",
        ))
    }

    /// not support
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        debug!("register_sql_info: _id: {:?}, request: {:?}", _id, _result);
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use arrow_flight::{
        flight_service_client::FlightServiceClient,
        flight_service_server::FlightServiceServer,
        sql::{CommandStatementQuery, ProstAnyExt},
        utils as flight_utils, FlightData, FlightDescriptor, HandshakeRequest, IpcMessage,
    };
    use datafusion::arrow::{self, buffer::Buffer, datatypes::Schema, ipc};
    use futures::{StreamExt, TryStreamExt};
    use http_protocol::header::AUTHORIZATION;
    use moka::sync::Cache;
    use prost::Message;
    use spi::server::dbms::DatabaseManagerSystemMock;
    use tokio::time;
    use tonic::{
        client::Grpc,
        metadata::{AsciiMetadataValue, KeyAndValueRef, MetadataValue},
        service::Interceptor,
        transport::{Channel, Endpoint, Server},
        Code, Request, Status, Streaming,
    };

    use crate::flight_sql::{
        auth_middleware::{
            basic_call_header_authenticator::BasicCallHeaderAuthenticator,
            generated_bearer_token_authenticator::GeneratedBearerTokenAuthenticator,
        },
        flight_sql_server::FlightSqlServiceImpl,
        utils,
    };

    async fn run_test_server() {
        let addr = "0.0.0.0:31004".parse().expect("parse address");

        let instance = Arc::new(DatabaseManagerSystemMock {});
        let authenticator = GeneratedBearerTokenAuthenticator::new(
            BasicCallHeaderAuthenticator::new(instance.clone()),
        );

        let svc = FlightServiceServer::new(FlightSqlServiceImpl::new(instance, authenticator));

        println!("Listening on {:?}", addr);

        let server = Server::builder().add_service(svc).serve(addr);

        let _handle = tokio::spawn(server);
    }

    #[tokio::test]
    async fn test_client() {
        trace::init_default_global_tracing("/tmp", "test_rust.log", "info");

        run_test_server().await;

        let endpoint = Endpoint::from_static("http://localhost:31004");
        let mut client = FlightServiceClient::connect(endpoint)
            .await
            .expect("connect");

        // 1. handshake, basic authentication
        let mut req = Request::new(futures::stream::iter(vec![HandshakeRequest::default()]));
        req.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            MetadataValue::from_static("Basic cm9vdDo="),
        );
        let resp = client.handshake(req).await.expect("handshake");
        println!("handshake resp: {:?}", resp.metadata());

        // 2. execute query, get result metadata
        let cmd = CommandStatementQuery {
            query: "select 1;".to_string(),
        };
        let any = prost_types::Any::pack(&cmd).expect("pack");
        let fd = FlightDescriptor::new_cmd(any.encode_to_vec());
        let mut req = Request::new(fd);
        req.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            resp.metadata().get(AUTHORIZATION.as_str()).unwrap().clone(),
        );
        let resp = client.get_flight_info(req).await.expect("get_flight_info");

        // 3. get result set
        let flight_info = resp.into_inner();
        let schema_ref =
            Arc::new(Schema::try_from(IpcMessage(flight_info.schema)).expect("Schema::try_from"));

        for ep in flight_info.endpoint {
            if let Some(tkt) = ep.ticket {
                let resp = client.do_get(tkt).await.expect("do_get");

                let mut stream = resp.into_inner();
                let mut dictionaries_by_id = HashMap::new();
                let mut chunks = vec![];

                while let Some(Ok(data)) = stream.next().await {
                    let message = arrow::ipc::root_as_message(&data.data_header[..])
                        .expect("root_as_message");

                    match message.header_type() {
                        ipc::MessageHeader::Schema => {
                            println!("a schema when messages are read",);
                        }
                        ipc::MessageHeader::RecordBatch => {
                            let batch = utils::record_batch_from_message(
                                message,
                                &Buffer::from(data.data_body),
                                schema_ref.clone(),
                                &dictionaries_by_id,
                            )
                            .expect("record_batch_from_message");

                            println!("ipc::MessageHeader::RecordBatch: {:?}", batch);

                            chunks.push(batch);
                        }
                        ipc::MessageHeader::DictionaryBatch => {
                            utils::dictionary_from_message(
                                message,
                                &Buffer::from(data.data_body),
                                schema_ref.clone(),
                                &mut dictionaries_by_id,
                            )
                            .expect("dictionary_from_message");
                        }
                        t => {
                            panic!("Reading types other than record batches not yet supported");
                        }
                    }
                }
            };
        }
    }
}
