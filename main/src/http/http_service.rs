#![allow(clippy::too_many_arguments)]

use std::borrow::Borrow;
use std::ops::Not;
use std::{collections::HashMap, convert::Infallible, net::SocketAddr, sync::Arc};

use coordinator::service::CoordinatorRef;
use http_protocol::header::{ACCEPT, AUTHORIZATION};
use http_protocol::parameter::{SqlParam, WriteParam};
use http_protocol::response::ErrorResponse;
use query::prom::remote_server::PromRemoteSqlServer;
use spi::server::prom::PromRemoteServerRef;

use super::header::Header;
use super::Error as HttpError;
use crate::http::response::ResponseBuilder;
use crate::http::result_format::fetch_record_batches;
use crate::http::result_format::ResultFormat;
use crate::http::Error;
use crate::http::ParseLineProtocolSnafu;
use crate::http::QuerySnafu;
use crate::server::{Service, ServiceHandle};
use crate::{server, VERSION};
use chrono::Local;
use config::TLSConfig;
use coordinator::hh_queue::HintedOffManager;
use coordinator::writer::{PointWriter, VnodeMapping};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::parquet::data_type::AsBytes;
use flatbuffers::FlatBufferBuilder;
use line_protocol::{line_protocol_to_lines, parse_lines_to_points, Line};
use meta::error::MetaError;
use meta::MetaClientRef;
use metrics::{gather_metrics, sample_point_write_duration, sample_query_read_duration};
use models::auth::privilege::{DatabasePrivilege, Privilege, TenantObjectPrivilege};
use models::consistency_level::ConsistencyLevel;
use models::error_code::{ErrorCode, UnknownCode, UnknownCodeWithMessage};
use models::oid::{Identifier, Oid};
use models::schema::DEFAULT_CATALOG;
use protos::kv_service::{Meta, WritePointsRpcRequest};
use protos::models as fb_models;
use protos::models::{FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder};
use snafu::ResultExt;
use spi::server::dbms::DBMSRef;
use spi::service::protocol::Query;
use spi::service::protocol::{Context, ContextBuilder};
use spi::QueryError;
use std::time::Instant;
use tokio::sync::oneshot;
use trace::debug;
use trace::info;
use tskv::engine::EngineRef;
use warp::hyper::body::Bytes;
use warp::hyper::Body;
use warp::reject::MethodNotAllowed;
use warp::reject::MissingHeader;
use warp::reject::PayloadTooLarge;
use warp::reply::Response;
use warp::Rejection;
use warp::Reply;
use warp::{header, reject, Filter};

pub struct HttpService {
    tls_config: Option<TLSConfig>,
    addr: SocketAddr,
    dbms: DBMSRef,
    kv_inst: EngineRef,
    coord: CoordinatorRef,
    prs: PromRemoteServerRef,
    handle: Option<ServiceHandle<()>>,
    query_body_limit: u64,
    write_body_limit: u64,
}

impl HttpService {
    pub fn new(
        dbms: DBMSRef,
        kv_inst: EngineRef,
        coord: CoordinatorRef,
        addr: SocketAddr,
        tls_config: Option<TLSConfig>,
        query_body_limit: u64,
        write_body_limit: u64,
    ) -> Self {
        let prs = Arc::new(PromRemoteSqlServer::new(dbms.clone()));

        Self {
            tls_config,
            addr,
            dbms,
            kv_inst,
            coord,
            prs,
            handle: None,
            query_body_limit,
            write_body_limit,
        }
    }

    /// user_id
    /// database
    /// =》
    /// Authorization
    /// Accept
    fn handle_header(&self) -> impl Filter<Extract = (Header,), Error = warp::Rejection> + Clone {
        header::optional::<String>(ACCEPT.as_str())
            .and(header::<String>(AUTHORIZATION.as_str()))
            .and_then(|accept, authorization| async move {
                let res: Result<Header, warp::Rejection> = Ok(Header::with(accept, authorization));
                res
            })
    }
    fn with_dbms(&self) -> impl Filter<Extract = (DBMSRef,), Error = Infallible> + Clone {
        let dbms = self.dbms.clone();
        warp::any().map(move || dbms.clone())
    }
    fn with_kv_inst(&self) -> impl Filter<Extract = (EngineRef,), Error = Infallible> + Clone {
        let kv_inst = self.kv_inst.clone();
        warp::any().map(move || kv_inst.clone())
    }
    fn with_coord(&self) -> impl Filter<Extract = (CoordinatorRef,), Error = Infallible> + Clone {
        let coord = self.coord.clone();
        warp::any().map(move || coord.clone())
    }
    fn with_prom_remote_server(
        &self,
    ) -> impl Filter<Extract = (PromRemoteServerRef,), Error = Infallible> + Clone {
        let prs = self.prs.clone();
        warp::any().map(move || prs.clone())
    }

    fn routes(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.ping()
            .or(self.query())
            .or(self.write_line_protocol())
            .or(self.metrics())
            .or(self.debug_jeprof())
            .or(self.debug_pprof())
            .or(self.print_meta())
            .or(self.prom_remote_read())
            .or(self.prom_remote_write())
    }

    fn ping(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "ping")
            .and(warp::get().or(warp::head()))
            .map(|_| {
                let mut resp = HashMap::new();
                resp.insert("version", VERSION.as_str());
                resp.insert("status", "healthy");
                warp::reply::json(&resp)
            })
    }

    fn query(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        // let dbms = self.dbms.clone();
        warp::path!("api" / "v1" / "sql")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.query_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<SqlParam>())
            .and(self.with_dbms())
            .and_then(
                |req: Bytes, header: Header, param: SqlParam, dbms: DBMSRef| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive http sql request, header: {:?}, param: {:?}",
                        header, param
                    );

                    // Parse req、header and param to construct query request
                    let query = construct_query(req, &header, param, dbms.clone())
                        .await
                        .map_err(|e| {
                            sample_query_read_duration("", "", false, 0.0);
                            reject::custom(e)
                        })?;
                    let result = sql_handle(&query, header, dbms).await.map_err(|e| {
                        trace::error!("Failed to handle http sql request, err: {}", e);
                        reject::custom(e)
                    });
                    let tenant = query.context().tenant();
                    let db = query.context().database();

                    sample_query_read_duration(
                        tenant,
                        db,
                        result.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
                    result
                },
            )
    }

    fn write_line_protocol(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "write")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.write_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<WriteParam>())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef| async move {
                    let start = Instant::now();
                    let ctx = construct_write_context(header, param, dbms, coord.clone())
                        .await
                        .map_err(reject::custom)?;

                    let req = construct_write_points_request(req, &ctx).map_err(reject::custom)?;

                    let resp: Result<(), HttpError> = coord
                        .write_points(ctx.tenant().to_string(), ConsistencyLevel::Any, req)
                        .await
                        .map_err(|e| e.into());

                    sample_point_write_duration(
                        ctx.tenant(),
                        ctx.database(),
                        resp.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
                    resp.map(|_| ResponseBuilder::ok()).map_err(reject::custom)
                },
            )
    }

    fn print_meta(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "meta")
            .and(self.handle_header())
            .and(self.with_coord())
            .and_then(|header: Header, coord: CoordinatorRef| async move {
                let tenant = DEFAULT_CATALOG.to_string();

                let meta_client = match coord.tenant_meta(&tenant).await {
                    Some(client) => client,
                    None => {
                        return Err(reject::custom(HttpError::Meta {
                            source: meta::error::MetaError::TenantNotFound { tenant },
                        }));
                    }
                };
                let data = meta_client.print_data();

                Ok(data)
            })
    }

    fn debug_pprof(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "pprof").and_then(|| async move {
            let res = utils::pprof_tools::gernate_pprof().await;
            info!("debug pprof: {:?}", res);
            match res {
                Ok(v) => Ok(v),
                Err(e) => Err(reject::custom(HttpError::PProfError { reason: e })),
            }
        })
    }

    fn debug_jeprof(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "jeprof").and_then(|| async move {
            let res = utils::pprof_tools::gernate_jeprof();
            info!("debug jeprof: {:?}", res);
            match res {
                Ok(v) => Ok(v),
                Err(e) => Err(reject::custom(HttpError::PProfError { reason: e })),
            }
        })
    }

    fn metrics(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("metrics").map(|| {
            debug!("prometheus access");
            warp::reply::Response::new(Body::from(gather_metrics()))
        })
    }

    fn prom_remote_read(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "prom" / "read")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.query_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<SqlParam>())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_prom_remote_server())
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: SqlParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 prs: PromRemoteServerRef| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive rest prom remote read request, header: {:?}, param: {:?}",
                        header, param
                    );

                    // Parse req、header and param to construct query request
                    let user_info = header.try_get_basic_auth().map_err(reject::custom)?;
                    let tenant = param.tenant;
                    let user = dbms
                        .authenticate(&user_info, tenant.as_deref())
                        .await
                        .map_err(|e| reject::custom(HttpError::from(e)))?;
                    let context = ContextBuilder::new(user)
                        .with_tenant(tenant)
                        .with_database(param.db)
                        .with_target_partitions(param.target_partitions)
                        .build();

                    let result = prs
                        .remote_read(&context, coord.meta_manager(), req)
                        .await
                        .map(|_| ResponseBuilder::ok())
                        .map_err(|e| {
                            trace::error!("Failed to handle prom remote read request, err: {}", e);
                            reject::custom(HttpError::from(e))
                        });

                    sample_query_read_duration(
                        context.tenant(),
                        context.database(),
                        result.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
                    result
                },
            )
    }

    fn prom_remote_write(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "prom" / "write")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.query_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<WriteParam>())
            .and(self.with_coord())
            .and(self.with_dbms())
            .and(self.with_prom_remote_server())
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 coord: CoordinatorRef,
                 dbms: DBMSRef,
                 prs: PromRemoteServerRef| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive rest prom remote write request, header: {:?}, param: {:?}",
                        header, param
                    );
                    let ctx = construct_write_context(header, param, dbms, coord.clone())
                        .await
                        .map_err(reject::custom)?;

                    let result = prs
                        .remote_write(req, &ctx, coord)
                        .await
                        .map(|_| ResponseBuilder::ok())
                        .map_err(|e| {
                            trace::error!("Failed to handle prom remote write request, err: {}", e);
                            reject::custom(HttpError::from(e))
                        });

                    sample_point_write_duration(
                        ctx.tenant(),
                        ctx.database(),
                        result.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
                    result
                },
            )
    }
}

#[async_trait::async_trait]
impl Service for HttpService {
    fn start(&mut self) -> Result<(), server::Error> {
        let routes = self.routes().recover(handle_rejection);
        let (shutdown, rx) = oneshot::channel();
        let signal = async {
            rx.await.ok();
            info!("http server graceful shutdown!");
        };
        let join_handle = if let Some(TLSConfig {
            certificate,
            private_key,
        }) = &self.tls_config
        {
            let (addr, server) = warp::serve(routes)
                .tls()
                .cert_path(certificate)
                .key_path(private_key)
                .bind_with_graceful_shutdown(self.addr, signal);
            info!("http server start addr: {}", addr);
            tokio::spawn(server)
        } else {
            let (addr, server) = warp::serve(routes).bind_with_graceful_shutdown(self.addr, signal);
            info!("http server start addr: {}", addr);
            tokio::spawn(server)
        };
        self.handle = Some(ServiceHandle::new(
            "http service".to_string(),
            join_handle,
            shutdown,
        ));
        Ok(())
    }

    async fn stop(&mut self, force: bool) {
        if let Some(stop) = self.handle.take() {
            stop.shutdown(force).await
        };
    }
}

async fn construct_query(
    req: Bytes,
    header: &Header,
    param: SqlParam,
    dbms: DBMSRef,
) -> Result<Query, HttpError> {
    let user_info = header.try_get_basic_auth()?;

    let tenant = param.tenant;
    let user = dbms
        .authenticate(&user_info, tenant.as_deref())
        .await
        .context(QuerySnafu)?;

    let context = ContextBuilder::new(user)
        .with_tenant(tenant)
        .with_database(param.db)
        .with_target_partitions(param.target_partitions)
        .build();

    Ok(Query::new(
        context,
        String::from_utf8_lossy(req.as_ref()).to_string(),
    ))
}

fn construct_write_db_privilege(tenant_id: Oid, database: &str) -> Privilege<Oid> {
    Privilege::TenantObject(
        TenantObjectPrivilege::Database(DatabasePrivilege::Write, Some(database.to_string())),
        Some(tenant_id),
    )
}

// construct context and check privilege
async fn construct_write_context(
    header: Header,
    param: WriteParam,
    dbms: DBMSRef,
    coord: CoordinatorRef,
) -> Result<Context, HttpError> {
    let user_info = header.try_get_basic_auth()?;
    let tenant = param.tenant;

    let user = dbms.authenticate(&user_info, tenant.as_deref()).await?;

    let context = ContextBuilder::new(user)
        .with_tenant(tenant)
        .with_database(Some(param.db))
        .build();

    let tenant_id = *coord
        .tenant_meta(context.tenant())
        .await
        .ok_or_else(|| MetaError::TenantNotFound {
            tenant: context.tenant().to_string(),
        })?
        .tenant()
        .id();

    let privilege = Privilege::TenantObject(
        TenantObjectPrivilege::Database(
            DatabasePrivilege::Write,
            Some(context.database().to_string()),
        ),
        Some(tenant_id),
    );
    if !context.user_info().check_privilege(&privilege) {
        return Err(HttpError::Query {
            source: QueryError::InsufficientPrivileges {
                privilege: format!("{privilege}"),
            },
        });
    }
    Ok(context)
}

fn construct_write_points_request(
    req: Bytes,
    ctx: &Context,
) -> Result<WritePointsRpcRequest, HttpError> {
    let lines = String::from_utf8_lossy(req.as_ref());
    let line_protocol_lines = line_protocol_to_lines(&lines, Local::now().timestamp_nanos())
        .map_err(|e| HttpError::ParseLineProtocol { source: e })?;

    let points = parse_lines_to_points(ctx.database(), &line_protocol_lines);

    let req = WritePointsRpcRequest {
        version: 1,
        meta: None,
        points,
    };
    Ok(req)
}

async fn sql_handle(query: &Query, header: Header, dbms: DBMSRef) -> Result<Response, HttpError> {
    debug!("prepare to execute: {:?}", query.content());

    let fmt = ResultFormat::try_from(header.get_accept())?;

    let result = dbms.execute(query).await.context(QuerySnafu)?;

    let batches = fetch_record_batches(result)
        .await
        .map_err(|e| HttpError::FetchResult {
            reason: format!("{e}"),
        })?;

    fmt.wrap_batches_to_response(&batches)
}

/*************** top ****************/
// Custom rejection handler that maps rejections into responses.
async fn handle_rejection(err: Rejection) -> Result<impl Reply, std::convert::Infallible> {
    if err.is_not_found() {
        Ok(ResponseBuilder::not_found())
    } else if err.find::<MethodNotAllowed>().is_some() {
        Ok(ResponseBuilder::method_not_allowed())
    } else if err.find::<PayloadTooLarge>().is_some() {
        Ok(ResponseBuilder::payload_too_large())
    } else if let Some(e) = err.find::<MissingHeader>() {
        let error_resp = ErrorResponse::new(&UnknownCodeWithMessage(e.to_string()));
        Ok(ResponseBuilder::bad_request(&error_resp))
    } else if let Some(e) = err.find::<HttpError>() {
        let resp: Response = e.into();
        Ok(resp)
    } else {
        trace::warn!("unhandled rejection: {:?}", err);
        Ok(ResponseBuilder::internal_server_error())
    }
}
/**************** bottom *****************/
#[cfg(test)]
mod test {
    use tokio::time;

    #[tokio::test]
    async fn test1() {
        use warp::Filter;
        // use futures_util::future::TryFutureExt;
        use tokio::sync::oneshot;

        let routes = warp::any().map(|| "Hello, World!");

        let (tx, rx) = oneshot::channel();

        let (_addr, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 30001), async {
                rx.await.ok();
            });

        // Spawn the server into a runtime
        tokio::task::spawn(server);
        dbg!("Server started");
        time::sleep(time::Duration::from_secs(1)).await;
        // Later, start the shutdown...
        dbg!("Server stop");
        let _ = tx.send(());
    }
}
