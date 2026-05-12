// SPDX-License-Identifier: Apache-2.0

pub mod routes;
pub mod state;

use crate::config::{ConfigError, RuntimeConfig};
use crate::s3::bucket::{is_valid_s3_bucket_name, BucketName};
use crate::storage::{
    ListObjectsOptions, MultipartUploadListing, StorageError, StoredObjectMetadata, StoredPart,
};
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
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::net::TcpListener;

pub const PHASE1_SERVER_SCOPE: &str = "path-style-local-s3";

#[derive(Clone)]
pub struct InspectorState {
    server_state: ServerState,
    trace_store: InMemoryTraceStore,
}

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

pub fn inspector_router(server_state: ServerState, trace_store: InMemoryTraceStore) -> Router {
    Router::new()
        .route("/", get(inspector_root))
        .route("/health", get(inspector_health))
        .route("/api/requests", get(inspector_requests))
        .route("/api/requests/{request_id}", get(inspector_request_events))
        .route("/api/buckets", get(inspector_buckets))
        .route(
            "/api/buckets/{bucket}/objects",
            get(inspector_bucket_objects),
        )
        .route("/api/multipart-uploads", get(inspector_multipart_uploads))
        .route("/api/snapshots", get(inspector_snapshots))
        .with_state(InspectorState {
            server_state,
            trace_store,
        })
}

async fn inspector_root() -> &'static str {
    "S3Lab inspector\n"
}

async fn inspector_health() -> &'static str {
    "ok\n"
}

async fn inspector_requests(State(state): State<InspectorState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "requests": state.trace_store.request_summaries(),
    }))
}

async fn inspector_request_events(
    State(state): State<InspectorState>,
    Path(request_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .trace_store
        .request_events(&request_id)
        .map(|events| {
            Json(serde_json::json!({
                "request_id": request_id,
                "events": events,
            }))
        })
        .ok_or(StatusCode::NOT_FOUND)
}

async fn inspector_buckets(
    State(state): State<InspectorState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let buckets = state
        .server_state
        .storage()
        .list_buckets()
        .map_err(inspector_storage_status)?;

    Ok(Json(serde_json::json!({
        "buckets": buckets
            .into_iter()
            .map(|bucket| serde_json::json!({ "name": bucket.name.as_str() }))
            .collect::<Vec<_>>(),
    })))
}

async fn inspector_bucket_objects(
    State(state): State<InspectorState>,
    Path(bucket): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if !is_valid_s3_bucket_name(&bucket) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let bucket = BucketName::new(bucket);
    let listing = state
        .server_state
        .storage()
        .list_objects(
            &bucket,
            ListObjectsOptions {
                max_keys: usize::MAX,
                ..ListObjectsOptions::default()
            },
        )
        .map_err(inspector_storage_status)?;

    Ok(Json(serde_json::json!({
        "bucket": bucket.as_str(),
        "objects": listing
            .objects
            .iter()
            .map(object_json)
            .collect::<Result<Vec<_>, _>>()?,
    })))
}

async fn inspector_multipart_uploads(
    State(state): State<InspectorState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let uploads = state
        .server_state
        .storage()
        .list_multipart_uploads()
        .map_err(inspector_storage_status)?;

    Ok(Json(serde_json::json!({
        "multipart_uploads": uploads
            .iter()
            .map(multipart_upload_json)
            .collect::<Result<Vec<_>, _>>()?,
    })))
}

async fn inspector_snapshots(
    State(state): State<InspectorState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let snapshots = state
        .server_state
        .storage()
        .list_snapshots()
        .map_err(inspector_storage_status)?;

    Ok(Json(serde_json::json!({
        "snapshots": snapshots
            .into_iter()
            .map(|snapshot| serde_json::json!({ "name": snapshot.name }))
            .collect::<Vec<_>>(),
    })))
}

fn object_json(object: &StoredObjectMetadata) -> Result<serde_json::Value, StatusCode> {
    Ok(serde_json::json!({
        "key": object.key.as_str(),
        "etag": object.etag.as_str(),
        "content_length": object.content_length,
        "content_type": object.content_type.as_deref(),
        "last_modified": rfc3339(object.last_modified)?,
    }))
}

fn multipart_upload_json(upload: &MultipartUploadListing) -> Result<serde_json::Value, StatusCode> {
    Ok(serde_json::json!({
        "bucket": upload.upload.bucket.as_str(),
        "key": upload.upload.key.as_str(),
        "upload_id": upload.upload.upload_id.as_str(),
        "initiated": rfc3339(upload.upload.initiated)?,
        "content_type": upload.upload.content_type.as_deref(),
        "part_count": upload.parts.len(),
        "parts": upload
            .parts
            .iter()
            .map(part_json)
            .collect::<Result<Vec<_>, _>>()?,
    }))
}

fn part_json(part: &StoredPart) -> Result<serde_json::Value, StatusCode> {
    Ok(serde_json::json!({
        "part_number": part.part_number,
        "etag": part.etag.as_str(),
        "content_length": part.content_length,
        "last_modified": rfc3339(part.last_modified)?,
    }))
}

fn rfc3339(timestamp: OffsetDateTime) -> Result<String, StatusCode> {
    timestamp
        .format(&Rfc3339)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn inspector_storage_status(error: StorageError) -> StatusCode {
    match error {
        StorageError::NoSuchBucket { .. } => StatusCode::NOT_FOUND,
        StorageError::InvalidBucketName { .. } | StorageError::InvalidArgument { .. } => {
            StatusCode::BAD_REQUEST
        }
        StorageError::BucketAlreadyExists { .. }
        | StorageError::BucketNotEmpty { .. }
        | StorageError::NoSuchKey { .. }
        | StorageError::InvalidObjectKey { .. }
        | StorageError::CorruptState { .. }
        | StorageError::Io { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
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
    state: ServerState,
    trace_store: InMemoryTraceStore,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, inspector_router(state, trace_store))
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
    use crate::storage::fs::{FilesystemStorage, StorageClock};
    use crate::storage::{
        CreateMultipartUploadRequest, PutObjectRequest, Storage, UploadPartRequest,
    };
    use crate::trace::InMemoryTraceStore;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::error::Error;
    use std::io;
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};
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
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let state = ServerState::from_storage(FilesystemStorage::new(temp_dir.path()));

        let response = inspector_router(state, InMemoryTraceStore::default())
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

        let create_bucket = router(state.clone())
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

        let requests_response = inspector_router(state.clone(), trace_store.clone())
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

        let events_response = inspector_router(state, trace_store)
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
    async fn inspector_router_serves_sorted_storage_state_without_payloads_or_paths() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let storage =
            FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedStorageClock);
        let bucket_a = crate::s3::bucket::BucketName::new("bucket-a");
        let bucket_b = crate::s3::bucket::BucketName::new("bucket-b");
        storage.create_bucket(&bucket_b).expect("create bucket b");
        storage.create_bucket(&bucket_a).expect("create bucket a");
        storage
            .put_object(PutObjectRequest {
                bucket: bucket_a.clone(),
                key: crate::s3::object::ObjectKey::new("z-object.txt"),
                bytes: b"secret body".to_vec(),
                content_type: Some("text/plain".to_owned()),
                user_metadata: BTreeMap::from([("secret".to_owned(), "private".to_owned())]),
            })
            .expect("put z object");
        storage
            .put_object(PutObjectRequest {
                bucket: bucket_a.clone(),
                key: crate::s3::object::ObjectKey::new("a-object.txt"),
                bytes: b"body".to_vec(),
                content_type: None,
                user_metadata: BTreeMap::new(),
            })
            .expect("put a object");
        let upload = storage
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: bucket_b.clone(),
                key: crate::s3::object::ObjectKey::new("large.bin"),
                content_type: None,
                user_metadata: BTreeMap::from([("secret".to_owned(), "private".to_owned())]),
            })
            .expect("create multipart upload");
        storage
            .upload_part(UploadPartRequest {
                bucket: bucket_b,
                key: crate::s3::object::ObjectKey::new("large.bin"),
                upload_id: upload.upload_id,
                part_number: 1,
                bytes: b"part body".to_vec(),
            })
            .expect("upload part");
        storage
            .save_snapshot("z-snapshot")
            .expect("save z snapshot");
        storage
            .save_snapshot("a-snapshot")
            .expect("save a snapshot");

        let state = ServerState::from_storage(storage);
        let app = inspector_router(state, InMemoryTraceStore::default());

        let buckets = response_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/buckets")
                        .body(Body::empty())
                        .expect("buckets request"),
                )
                .await
                .expect("buckets response"),
        )
        .await;
        assert_eq!(buckets["buckets"][0]["name"], "bucket-a");
        assert_eq!(buckets["buckets"][1]["name"], "bucket-b");

        let objects = response_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/buckets/bucket-a/objects")
                        .body(Body::empty())
                        .expect("objects request"),
                )
                .await
                .expect("objects response"),
        )
        .await;
        assert_eq!(objects["objects"][0]["key"], "a-object.txt");
        assert_eq!(objects["objects"][1]["key"], "z-object.txt");
        assert_eq!(objects["objects"][1]["content_length"], 11);
        assert_eq!(
            objects["objects"][1]["last_modified"],
            "2026-05-10T12:34:56Z"
        );

        let uploads = response_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/multipart-uploads")
                        .body(Body::empty())
                        .expect("uploads request"),
                )
                .await
                .expect("uploads response"),
        )
        .await;
        assert_eq!(uploads["multipart_uploads"][0]["bucket"], "bucket-b");
        assert_eq!(uploads["multipart_uploads"][0]["key"], "large.bin");
        assert_eq!(uploads["multipart_uploads"][0]["part_count"], 1);
        assert_eq!(
            uploads["multipart_uploads"][0]["parts"][0]["content_length"],
            9
        );

        let snapshots = response_json(
            app.oneshot(
                Request::builder()
                    .uri("/api/snapshots")
                    .body(Body::empty())
                    .expect("snapshots request"),
            )
            .await
            .expect("snapshots response"),
        )
        .await;
        assert_eq!(snapshots["snapshots"][0]["name"], "a-snapshot");
        assert_eq!(snapshots["snapshots"][1]["name"], "z-snapshot");

        let combined = format!("{buckets}{objects}{uploads}{snapshots}");
        assert!(!combined.contains("secret body"));
        assert!(!combined.contains("part body"));
        assert!(!combined.contains("private"));
        assert!(!combined.contains(temp_dir.path().to_string_lossy().as_ref()));
    }

    #[tokio::test]
    async fn inspector_bucket_objects_reports_missing_bucket_as_not_found() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let state = ServerState::from_storage(FilesystemStorage::new(temp_dir.path()));

        let response = inspector_router(state, InMemoryTraceStore::default())
            .oneshot(
                Request::builder()
                    .uri("/api/buckets/missing-bucket/objects")
                    .body(Body::empty())
                    .expect("objects request"),
            )
            .await
            .expect("objects response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
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

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct FixedStorageClock;

    impl StorageClock for FixedStorageClock {
        fn now_utc(&self) -> OffsetDateTime {
            PrimitiveDateTime::new(
                Date::from_calendar_date(2026, Month::May, 10).expect("valid test date"),
                Time::from_hms(12, 34, 56).expect("valid test time"),
            )
            .assume_utc()
        }
    }

    async fn response_json(response: axum::http::Response<Body>) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");

        serde_json::from_slice(&bytes).expect("valid json")
    }
}
