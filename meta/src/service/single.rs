use std::collections::HashSet;
use std::convert::Infallible as StdInfallible;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use replication::ApplyStorage;
use tokio::sync::RwLock;
use tracing::{debug, info};
use warp::{hyper, Filter};

use crate::error::{MetaError, MetaResult};
use crate::store::command::*;
use crate::store::dump::dump_impl;
use crate::store::storage::{BtreeMapSnapshotData, StateMachine};

pub async fn start_singe_meta_server(
    path: String,
    cluster_name: String,
    addr: String,
    size: usize,
) {
    let db_path = format!("{}/meta/{}.data", path, 0);
    let mut storage = StateMachine::open(db_path, size).unwrap();

    let init_data = crate::store::config::MetaInit {
        cluster_name,
        admin_user: models::auth::user::ROOT.to_string(),
        system_tenant: models::schema::DEFAULT_CATALOG.to_string(),
        default_database: vec![
            models::schema::USAGE_SCHEMA.to_string(),
            models::schema::DEFAULT_DATABASE.to_string(),
        ],
    };
    super::init::init_meta(&mut storage, init_data).await;

    tracing::info!("single meta http server start addr: {}", addr);
    let storage = Arc::new(RwLock::new(storage));
    let server = SingleServer { addr, storage };

    tokio::spawn(async move { server.start().await });
}

pub struct SingleServer {
    pub addr: String,
    pub storage: Arc<RwLock<StateMachine>>,
}

impl SingleServer {
    pub async fn start(&self) {
        let addr: SocketAddr = self.addr.parse().unwrap();
        warp::serve(self.routes()).run(addr).await;
    }

    fn routes(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.read()
            .or(self.write())
            .or(self.watch())
            .or(self.dump())
            .or(self.dump_sql())
            .or(self.restore())
            .or(self.watch_meta_membership())
            .or(self.debug())
    }

    fn with_addr(&self) -> impl Filter<Extract = (String,), Error = StdInfallible> + Clone {
        let addr = self.addr.clone();
        warp::any().map(move || addr.clone())
    }

    fn with_storage(
        &self,
    ) -> impl Filter<Extract = (Arc<RwLock<StateMachine>>,), Error = StdInfallible> + Clone {
        let storage = self.storage.clone();
        warp::any().map(move || storage.clone())
    }

    fn watch_meta_membership(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("watch_meta_membership")
            .and(warp::body::bytes())
            .and(self.with_addr())
            .and_then(|_req: hyper::body::Bytes, addr: String| async move {
                let nodes = vec![addr];
                let data = crate::store::storage::response_encode(Ok(nodes));

                let res: Result<String, warp::Rejection> = Ok(data);

                res
            })
    }

    fn read(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("read")
            .and(warp::body::bytes())
            .and(self.with_storage())
            .and_then(
                |req: hyper::body::Bytes, storage: Arc<RwLock<StateMachine>>| async move {
                    let req: ReadCommand = serde_json::from_slice(&req)
                        .map_err(MetaError::from)
                        .map_err(warp::reject::custom)?;

                    let rsp = storage.read().await.process_read_command(&req);
                    let res: Result<String, warp::Rejection> = Ok(rsp);
                    res
                },
            )
    }

    fn write(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("write")
            .and(warp::body::bytes())
            .and(self.with_storage())
            .and_then(
                |req: hyper::body::Bytes, storage: Arc<RwLock<StateMachine>>| async move {
                    let req: WriteCommand = serde_json::from_slice(&req)
                        .map_err(MetaError::from)
                        .map_err(warp::reject::custom)?;

                    let rsp = storage.write().await.process_write_command(&req);
                    let res: Result<String, warp::Rejection> = Ok(rsp);
                    res
                },
            )
    }

    fn watch(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("watch")
            .and(warp::body::bytes())
            .and(self.with_storage())
            .and_then(
                |req: hyper::body::Bytes, storage: Arc<RwLock<StateMachine>>| async move {
                    let data = Self::process_watch(req, storage)
                        .await
                        .map_err(warp::reject::custom)?;

                    let res: Result<String, warp::Rejection> = Ok(data);
                    res
                },
            )
    }

    fn dump(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("dump").and(self.with_storage()).and_then(
            |storage: Arc<RwLock<StateMachine>>| async move {
                let data = storage
                    .write()
                    .await
                    .snapshot()
                    .await
                    .map_err(MetaError::from)
                    .map_err(warp::reject::custom)?;

                let data: BtreeMapSnapshotData = serde_json::from_slice(&data)
                    .map_err(MetaError::from)
                    .map_err(warp::reject::custom)?;

                let mut rsp = "".to_string();
                for (key, val) in data.map.iter() {
                    rsp = rsp + &format!("{}: {}\n", key, val);
                }

                let res: Result<String, warp::Rejection> = Ok(rsp);
                res
            },
        )
    }

    fn dump_sql(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        let opt = warp::path::param::<String>()
            .map(Some)
            .or_else(|_| async { Ok::<(Option<String>,), std::convert::Infallible>((None,)) });
        let prefix = warp::path!("dump" / "sql" / "ddl" / String / ..);

        let route = prefix.and(opt).and(warp::path::end());

        route
            .and(self.with_storage())
            .and_then(
                |cluster: String, tenant: Option<String>, storage: Arc<RwLock<StateMachine>>| async move {
                    let machine = storage.read().await;
                    let res = dump_impl(&cluster, tenant.as_deref(), machine.deref())
                        .await
                        .map_err(warp::reject::custom)?;
                    Ok::<String, warp::Rejection>(res)
                },
            )
    }

    fn restore(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("restore")
            .and(warp::body::bytes())
            .and(self.with_storage())
            .and_then(
                |req: hyper::body::Bytes, storage: Arc<RwLock<StateMachine>>| async move {
                    info!("restore data length:{}", req.len());

                    let mut count = 0;
                    let req = String::from_utf8_lossy(&req).to_string();
                    let lines: Vec<&str> = req.split('\n').collect();
                    for line in lines {
                        let strs: Vec<&str> = line.splitn(2, ": ").collect();
                        if strs.len() != 2 {
                            continue;
                        }

                        let command = WriteCommand::Set {
                            key: strs[0].to_string(),
                            value: strs[1].to_string(),
                        };

                        let _ = storage.write().await.process_write_command(&command);

                        count += 1;
                    }

                    let data = format!("Restore Data Success, Total: {} ", count);
                    let res: Result<String, warp::Rejection> = Ok(data);

                    res
                },
            )
    }

    fn debug(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug").and(self.with_storage()).and_then(
            |storage: Arc<RwLock<StateMachine>>| async move {
                let data = storage
                    .write()
                    .await
                    .dump()
                    .await
                    .map_err(warp::reject::custom)?;

                let res: Result<String, warp::Rejection> = Ok(data);
                res
            },
        )
    }

    pub async fn process_watch(
        req: hyper::body::Bytes,
        storage: Arc<RwLock<StateMachine>>,
    ) -> MetaResult<String> {
        let req: (String, String, HashSet<String>, u64) = serde_json::from_slice(&req)?;
        let (client, cluster, tenants, base_ver) = req;
        debug!(
            "watch all  args: client-id: {}, cluster: {}, tenants: {:?}, version: {}",
            client, cluster, tenants, base_ver
        );

        let mut notify = {
            let storage = storage.read().await;
            let watch_data = storage.read_change_logs(&cluster, &tenants, base_ver);
            if watch_data.need_return(base_ver) {
                return Ok(crate::store::storage::response_encode(Ok(watch_data)));
            }

            storage.watch.subscribe()
        };

        let mut follow_ver = base_ver;
        let now = std::time::Instant::now();
        loop {
            let _ = tokio::time::timeout(Duration::from_secs(20), notify.recv()).await;

            let watch_data = storage
                .read()
                .await
                .read_change_logs(&cluster, &tenants, follow_ver);
            debug!("watch notify {} {}.{}", client, base_ver, follow_ver);
            if watch_data.need_return(base_ver) || now.elapsed() > Duration::from_secs(30) {
                return Ok(crate::store::storage::response_encode(Ok(watch_data)));
            }

            if follow_ver < watch_data.max_ver {
                follow_ver = watch_data.max_ver;
            }
        }
    }
}
