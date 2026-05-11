// SPDX-License-Identifier: Apache-2.0

use crate::s3::error::S3RequestId;
use crate::storage::Storage;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct ServerState {
    storage: Arc<dyn Storage + Send + Sync>,
    request_id_generator: Arc<dyn RequestIdGenerator>,
}

pub trait RequestIdGenerator: Send + Sync {
    fn next_request_id(&self) -> S3RequestId;
}

#[derive(Debug, Default)]
pub struct SequentialRequestIdGenerator {
    next: AtomicU64,
}

#[derive(Debug)]
pub struct FixedRequestIdGenerator {
    request_id: S3RequestId,
}

impl ServerState {
    pub fn from_storage(storage: impl Storage + Send + Sync + 'static) -> Self {
        Self::with_request_id_generator(storage, SequentialRequestIdGenerator::default())
    }

    pub fn with_request_id_generator(
        storage: impl Storage + Send + Sync + 'static,
        request_id_generator: impl RequestIdGenerator + 'static,
    ) -> Self {
        Self {
            storage: Arc::new(storage),
            request_id_generator: Arc::new(request_id_generator),
        }
    }

    pub fn with_fixed_request_id(
        storage: impl Storage + Send + Sync + 'static,
        request_id: impl Into<String>,
    ) -> Self {
        Self::with_request_id_generator(storage, FixedRequestIdGenerator::new(request_id))
    }

    pub fn storage(&self) -> &(dyn Storage + Send + Sync) {
        self.storage.as_ref()
    }

    pub fn next_request_id(&self) -> S3RequestId {
        self.request_id_generator.next_request_id()
    }
}

impl SequentialRequestIdGenerator {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RequestIdGenerator for SequentialRequestIdGenerator {
    fn next_request_id(&self) -> S3RequestId {
        let value = self.next.fetch_add(1, Ordering::Relaxed) + 1;

        S3RequestId::new(format!("s3lab-{value:016}"))
    }
}

impl FixedRequestIdGenerator {
    pub fn new(request_id: impl Into<String>) -> Self {
        Self {
            request_id: S3RequestId::new(request_id),
        }
    }
}

impl RequestIdGenerator for FixedRequestIdGenerator {
    fn next_request_id(&self) -> S3RequestId {
        self.request_id.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{RequestIdGenerator, SequentialRequestIdGenerator, ServerState};
    use crate::storage::fs::FilesystemStorage;

    #[test]
    fn from_storage_wraps_filesystem_backed_state() {
        let state = ServerState::from_storage(FilesystemStorage::new("./s3lab-data"));

        assert_eq!(
            state
                .storage()
                .list_buckets()
                .expect("empty filesystem storage lists buckets"),
            []
        );
    }

    #[test]
    fn state_can_be_cloned_without_losing_storage_access() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let state = ServerState::from_storage(FilesystemStorage::new(temp_dir.path()));
        let cloned = state.clone();

        assert_eq!(
            cloned.storage().list_buckets().expect("list from clone"),
            state.storage().list_buckets().expect("list from original")
        );
    }

    #[test]
    fn sequential_request_ids_are_process_local_and_zero_padded() {
        let generator = SequentialRequestIdGenerator::new();

        assert_eq!(
            generator.next_request_id().as_str(),
            "s3lab-0000000000000001"
        );
        assert_eq!(
            generator.next_request_id().as_str(),
            "s3lab-0000000000000002"
        );
    }
}
