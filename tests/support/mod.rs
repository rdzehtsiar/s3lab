// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::http::{HeaderValue, Method, Request, Response};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use s3lab::s3::error::STATIC_REQUEST_ID;
use s3lab::server::serve_listener_until;
use s3lab::server::state::ServerState;
use s3lab::storage::fs::FilesystemStorage;
use s3lab::storage::Storage;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub const TEST_SUPPORT_MARKER: &str = "offline-deterministic-tests";

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct TestServer {
    _temp_dir: Option<TempDir>,
    data_dir: Option<PathBuf>,
    base_url: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl TestServer {
    pub async fn start() -> Self {
        let temp_dir = TempDir::new().expect("create test server data dir");
        let data_dir = temp_dir.path().to_path_buf();

        Self::start_inner(data_dir, Some(temp_dir)).await
    }

    pub async fn start_with_data_dir(path: impl Into<PathBuf>) -> Self {
        Self::start_inner(path.into(), None).await
    }

    async fn start_inner(data_dir: PathBuf, temp_dir: Option<TempDir>) -> Self {
        Self::start_with_state(
            test_server_state(FilesystemStorage::new(data_dir.clone())),
            temp_dir,
            Some(data_dir),
        )
        .await
    }

    pub async fn start_with_state(
        state: ServerState,
        temp_dir: Option<TempDir>,
        data_dir: Option<PathBuf>,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test server to loopback ephemeral port");
        let address = listener
            .local_addr()
            .expect("read bound test server address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move {
            serve_listener_until(listener, state, async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(std::io::Error::other)
        });

        Self {
            _temp_dir: temp_dir,
            data_dir,
            base_url: format!("http://{address}"),
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn data_dir(&self) -> Option<&std::path::Path> {
        self.data_dir.as_deref()
    }

    pub fn url(&self, path_and_query: &str) -> String {
        if path_and_query.starts_with('/') {
            format!("{}{}", self.base_url, path_and_query)
        } else {
            format!("{}/{}", self.base_url, path_and_query)
        }
    }

    pub async fn shutdown(mut self) {
        self.send_shutdown();
        if let Some(task) = self.task.take() {
            task.await
                .expect("test server task joins")
                .expect("test server shuts down cleanly");
        }
    }

    fn send_shutdown(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.send_shutdown();
    }
}

pub async fn request(
    method: Method,
    url: &str,
    body: impl Into<Bytes>,
    headers: &[(&str, &str)],
) -> TestResult<Response<Incoming>> {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let mut builder = Request::builder().method(method).uri(url);

    for (name, value) in headers {
        builder = builder.header(*name, HeaderValue::from_str(value)?);
    }

    let request = builder.body(Full::new(body.into()))?;
    Ok(client.request(request).await?)
}

pub async fn response_bytes(response: Response<Incoming>) -> TestResult<Bytes> {
    Ok(response.into_body().collect().await?.to_bytes())
}

pub async fn response_text(response: Response<Incoming>) -> TestResult<String> {
    let bytes = response_bytes(response).await?;
    Ok(String::from_utf8(bytes.to_vec())?)
}

pub fn test_server_state(storage: impl Storage + Send + Sync + 'static) -> ServerState {
    ServerState::with_fixed_request_id(storage, STATIC_REQUEST_ID)
}
