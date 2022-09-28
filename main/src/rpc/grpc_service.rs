use crate::rpc::tskv::TskvServiceImpl;
use crate::server::{Service, ServiceHandle};
use crate::{info, server};
use config::TLSConfig;
use protos::kv_service::tskv_service_server::TskvServiceServer;
use spi::server::dbms::DBMSRef;
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tskv::engine::EngineRef;

pub struct GrpcService {
    tls_config: Option<TLSConfig>,
    addr: SocketAddr,
    //todo grpc support sql query
    _dbms: DBMSRef,
    kv_inst: EngineRef,
    handle: Option<ServiceHandle<Result<(), tonic::transport::Error>>>,
}

impl GrpcService {
    pub fn new(
        dbms: DBMSRef,
        kv_inst: EngineRef,
        addr: SocketAddr,
        tls_config: Option<TLSConfig>,
    ) -> Self {
        Self {
            tls_config,
            addr,
            _dbms: dbms,
            kv_inst,
            handle: None,
        }
    }
}

fn build_grpc_server(tls_config: &Option<TLSConfig>) -> server::Result<Server> {
    let server = Server::builder();
    if tls_config.is_none() {
        return Ok(server);
    }

    let TLSConfig {
        certificate,
        private_key,
    } = tls_config.as_ref().unwrap();
    let cert = std::fs::read(certificate)?;
    let key = std::fs::read(private_key)?;
    let identity = Identity::from_pem(&cert, &key);
    let server = server.tls_config(ServerTlsConfig::new().identity(identity))?;

    Ok(server)
}

#[async_trait::async_trait]
impl Service for GrpcService {
    fn start(&mut self) -> server::Result<()> {
        let (shutdown, rx) = oneshot::channel();
        let tskv_grpc_service = TskvServiceServer::new(TskvServiceImpl {
            kv_engine: self.kv_inst.clone(),
        });
        let mut grpc_builder = build_grpc_server(&self.tls_config)?;
        let grpc_router = grpc_builder.add_service(tskv_grpc_service);
        let server = grpc_router.serve_with_shutdown(self.addr, async {
            rx.await.ok();
            info!("grpc server graceful shutdown!");
        });
        info!("grpc server start addr: {}", self.addr);
        let grpc_handle = tokio::spawn(server);
        self.handle = Some(ServiceHandle::new(
            "grpc service".to_string(),
            grpc_handle,
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
