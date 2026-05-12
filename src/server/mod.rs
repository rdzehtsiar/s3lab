// SPDX-License-Identifier: Apache-2.0

pub mod routes;
pub mod state;

use crate::config::{ConfigError, RuntimeConfig};
use crate::trace::InMemoryTraceStore;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::Json;
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

pub fn inspector_router(trace_store: InMemoryTraceStore) -> Router {
    Router::new()
        .route("/", get(inspector_root))
        .route("/health", get(inspector_health))
        .route("/api/requests", get(inspector_requests))
        .route("/api/requests/{request_id}", get(inspector_request_events))
        .with_state(trace_store)
}

async fn inspector_root() -> &'static str {
    "S3Lab inspector\n"
}

async fn inspector_health() -> &'static str {
    "ok\n"
}

async fn inspector_requests(
    State(trace_store): State<InMemoryTraceStore>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "requests": trace_store.request_summaries(),
    }))
}

async fn inspector_request_events(
    State(trace_store): State<InMemoryTraceStore>,
    Path(request_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    trace_store
        .request_events(&request_id)
        .map(|events| {
            Json(serde_json::json!({
                "request_id": request_id,
                "events": events,
            }))
        })
        .ok_or(StatusCode::NOT_FOUND)
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

pub async fn bind_inspector_listener(config: &RuntimeConfig) -> Result<TcpListener, ServerError> {
    config
        .validate()
        .map_err(|source| ServerError::Config { source })?;
    TcpListener::bind((config.inspector_bind_host(), config.inspector_port))
        .await
        .map_err(|source| ServerError::Bind {
            endpoint: config.inspector_endpoint(),
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

pub async fn serve_inspector_listener_until<F>(
    listener: TcpListener,
    trace_store: InMemoryTraceStore,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, inspector_router(trace_store))
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
    use super::{
        bind_inspector_listener, bind_listener, inspector_router, listener_endpoint, ServerError,
    };
    use crate::config::{ConfigError, RuntimeConfig, DEFAULT_DATA_DIR};
    use crate::server::router;
    use crate::server::state::{FixedRequestIdGenerator, ServerState};
    use crate::storage::fs::FilesystemStorage;
    use crate::trace::InMemoryTraceStore;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use std::error::Error;
    use std::io;
    use tokio::net::TcpListener;
    use tower::ServiceExt;

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
    async fn bind_inspector_listener_uses_loopback_config() {
        let config =
            RuntimeConfig::new("127.0.0.1", 0, DEFAULT_DATA_DIR).with_inspector("localhost", 0);

        let listener = bind_inspector_listener(&config)
            .await
            .expect("bind inspector listener");
        let address = listener.local_addr().expect("read inspector address");

        assert!(address.ip().is_loopback());
        assert_eq!(address.ip().to_string(), "127.0.0.1");
    }

    #[tokio::test]
    async fn inspector_router_serves_minimal_health_response() {
        let response = inspector_router(InMemoryTraceStore::default())
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("health request"),
            )
            .await
            .expect("health response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn inspector_router_serves_trace_json_after_s3_request() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let trace_store = InMemoryTraceStore::new(8);
        let state = ServerState::with_request_id_generator_and_trace_sink(
            FilesystemStorage::new(temp_dir.path()),
            FixedRequestIdGenerator::new("s3lab-0000000000000001"),
            trace_store.clone(),
        );

        let create_bucket = router(state)
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/bucket-a")
                    .body(Body::empty())
                    .expect("create bucket request"),
            )
            .await
            .expect("create bucket response");
        assert_eq!(create_bucket.status(), StatusCode::OK);

        let requests_response = inspector_router(trace_store.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/requests")
                    .body(Body::empty())
                    .expect("requests request"),
            )
            .await
            .expect("requests response");
        assert_eq!(requests_response.status(), StatusCode::OK);
        let requests_body = response_json(requests_response).await;
        assert_eq!(
            requests_body["requests"][0]["request_id"],
            "s3lab-0000000000000001"
        );
        assert_eq!(requests_body["requests"][0]["method"], "PUT");
        assert_eq!(requests_body["requests"][0]["path"], "/bucket-a");
        assert_eq!(requests_body["requests"][0]["status_code"], 200);

        let events_response = inspector_router(trace_store)
            .oneshot(
                Request::builder()
                    .uri("/api/requests/s3lab-0000000000000001")
                    .body(Body::empty())
                    .expect("request events request"),
            )
            .await
            .expect("events response");
        assert_eq!(events_response.status(), StatusCode::OK);
        let events_body = response_json(events_response).await;
        assert_eq!(events_body["request_id"], "s3lab-0000000000000001");
        assert_eq!(events_body["events"][0]["type"], "request_received");
        assert!(!events_body.to_string().contains("Authorization:"));
        assert!(!events_body.to_string().contains("Signature="));
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

    async fn response_json(response: axum::http::Response<Body>) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");

        serde_json::from_slice(&bytes).expect("valid json")
    }
}
