use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use config::TLSConfig;
use spi::server::dbms::DBMSRef;
use tokio::sync::oneshot;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use trace::info;
use warp::trace::Info;

use self::flight_sql_server::FlightSqlServiceImpl;
use crate::flight_sql::auth_middleware::basic_call_header_authenticator::BasicCallHeaderAuthenticator;
use crate::flight_sql::auth_middleware::generated_bearer_token_authenticator::GeneratedBearerTokenAuthenticator;
use crate::server::{Service, ServiceHandle};

mod auth_middleware;
pub mod flight_sql_server;
mod utils;

pub struct FlightSqlServiceAdapter {
    dbms: DBMSRef,

    addr: SocketAddr,
    tls_config: Option<TLSConfig>,
    handle: Option<ServiceHandle<Result<(), tonic::transport::Error>>>,
}

impl FlightSqlServiceAdapter {
    pub fn new(dbms: DBMSRef, addr: SocketAddr, tls_config: Option<TLSConfig>) -> Self {
        Self {
            dbms,
            addr,
            tls_config,
            handle: None,
        }
    }
}

#[async_trait::async_trait]
impl Service for FlightSqlServiceAdapter {
    fn start(&mut self) -> crate::server::Result<()> {
        let (shutdown, rx) = oneshot::channel();

        let server = Server::builder();

        let mut server = if let Some(TLSConfig {
            certificate,
            private_key,
        }) = self.tls_config.as_ref()
        {
            let cert = std::fs::read(certificate)?;
            let key = std::fs::read(private_key)?;
            let identity = Identity::from_pem(cert, key);
            server.tls_config(ServerTlsConfig::new().identity(identity))?
        } else {
            server
        };

        let authenticator = GeneratedBearerTokenAuthenticator::new(
            BasicCallHeaderAuthenticator::new(self.dbms.clone()),
        );
        let svc =
            FlightServiceServer::new(FlightSqlServiceImpl::new(self.dbms.clone(), authenticator));

        let server = server
            .add_service(svc)
            .serve_with_shutdown(self.addr, async {
                rx.await.ok();
                info!("flight rpc server graceful shutdown!");
            });

        let handle = tokio::spawn(server);
        self.handle = Some(ServiceHandle::new(
            "flight rpc service".to_string(),
            handle,
            shutdown,
        ));

        info!("flight rpc server start addr: {}", self.addr);

        Ok(())
    }

    async fn stop(&mut self, force: bool) {
        if let Some(stop) = self.handle.take() {
            stop.shutdown(force).await
        };
    }
}
