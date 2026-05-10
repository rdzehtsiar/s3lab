// SPDX-License-Identifier: Apache-2.0

pub mod routes;
pub mod state;

use crate::config::{ConfigError, RuntimeConfig};
use axum::Router;
use routes::handle_request;
use state::ServerState;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use tokio::net::TcpListener;

pub const PHASE1_SERVER_SCOPE: &str = "path-style-local-s3";

#[derive(Debug)]
pub enum ServerError {
    Config {
        source: ConfigError,
    },
    Bind {
        endpoint: String,
        source: std::io::Error,
    },
    LocalAddress {
        source: std::io::Error,
    },
    Run {
        source: std::io::Error,
    },
}

pub fn router(state: ServerState) -> Router {
    Router::new().fallback(handle_request).with_state(state)
}

pub async fn bind_listener(config: &RuntimeConfig) -> Result<TcpListener, ServerError> {
    config
        .validate()
        .map_err(|source| ServerError::Config { source })?;
    TcpListener::bind((config.bind_host(), config.port))
        .await
        .map_err(|source| ServerError::Bind {
            endpoint: config.endpoint(),
            source,
        })
}

pub fn listener_endpoint(listener: &TcpListener) -> Result<String, ServerError> {
    listener
        .local_addr()
        .map(|address| format!("http://{address}"))
        .map_err(|source| ServerError::LocalAddress { source })
}

pub async fn serve_listener_until<F>(
    listener: TcpListener,
    state: ServerState,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let app = router(state);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|source| ServerError::Run { source })
}

impl Display for ServerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config { source } => Display::fmt(source, formatter),
            Self::Bind { endpoint, source } => {
                write!(
                    formatter,
                    "failed to bind local endpoint {endpoint}: {source}"
                )
            }
            Self::LocalAddress { source } => {
                write!(
                    formatter,
                    "failed to read bound local endpoint address: {source}"
                )
            }
            Self::Run { source } => {
                write!(formatter, "local server failed: {source}")
            }
        }
    }
}

impl Error for ServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Config { source } => Some(source),
            Self::Bind { source, .. } => Some(source),
            Self::LocalAddress { source } => Some(source),
            Self::Run { source } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{bind_listener, listener_endpoint, ServerError};
    use crate::config::{ConfigError, RuntimeConfig, DEFAULT_DATA_DIR};
    use std::error::Error;
    use std::io;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn bind_listener_normalizes_localhost_to_ipv4_loopback() {
        let config = RuntimeConfig::new("localhost", 0, DEFAULT_DATA_DIR);

        let listener = bind_listener(&config)
            .await
            .expect("bind localhost listener");
        let address = listener.local_addr().expect("read bound address");

        assert!(address.ip().is_loopback());
        assert_eq!(address.ip().to_string(), "127.0.0.1");
    }

    #[tokio::test]
    async fn listener_endpoint_reports_bound_ephemeral_endpoint() {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind ephemeral listener");
        let address = listener.local_addr().expect("read bound address");

        assert_eq!(
            listener_endpoint(&listener).expect("format listener endpoint"),
            format!("http://{address}")
        );
    }

    #[test]
    fn server_error_display_and_source_expose_config_error() {
        let source = ConfigError::NonLoopbackHost {
            host: "example.com".to_owned(),
        };
        let expected = source.to_string();
        let error = ServerError::Config { source };

        assert_eq!(error.to_string(), expected);
        assert_eq!(error.source().expect("config source").to_string(), expected);
    }

    #[test]
    fn server_error_display_and_source_expose_bind_error() {
        let error = ServerError::Bind {
            endpoint: "http://127.0.0.1:9000".to_owned(),
            source: io_error(io::ErrorKind::AddrInUse, "address already in use"),
        };

        assert_eq!(
            error.to_string(),
            "failed to bind local endpoint http://127.0.0.1:9000: address already in use"
        );
        assert_eq!(
            error.source().expect("bind source").to_string(),
            "address already in use"
        );
    }

    #[test]
    fn server_error_display_and_source_expose_local_address_error() {
        let error = ServerError::LocalAddress {
            source: io_error(io::ErrorKind::NotConnected, "listener is not connected"),
        };

        assert_eq!(
            error.to_string(),
            "failed to read bound local endpoint address: listener is not connected"
        );
        assert_eq!(
            error.source().expect("local address source").to_string(),
            "listener is not connected"
        );
    }

    #[test]
    fn server_error_display_and_source_expose_run_error() {
        let error = ServerError::Run {
            source: io_error(io::ErrorKind::ConnectionAborted, "server aborted"),
        };

        assert_eq!(error.to_string(), "local server failed: server aborted");
        assert_eq!(
            error.source().expect("run source").to_string(),
            "server aborted"
        );
    }

    fn io_error(kind: io::ErrorKind, message: &'static str) -> io::Error {
        io::Error::new(kind, message)
    }
}
