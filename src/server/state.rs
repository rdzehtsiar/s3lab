// SPDX-License-Identifier: Apache-2.0

use crate::storage::fs::FilesystemStorage;
use crate::storage::Storage;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct ServerState {
    storage: Arc<dyn Storage + Send + Sync>,
}

impl ServerState {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self::filesystem(data_dir)
    }

    pub fn from_storage(storage: impl Storage + Send + Sync + 'static) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }

    pub fn filesystem(root: impl Into<PathBuf>) -> Self {
        Self::from_storage(FilesystemStorage::new(root))
    }

    pub fn storage(&self) -> &(dyn Storage + Send + Sync) {
        self.storage.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::ServerState;

    #[test]
    fn new_builds_filesystem_backed_state() {
        let state = ServerState::new("./s3lab-data");

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
        let state = ServerState::filesystem(temp_dir.path());
        let cloned = state.clone();

        assert_eq!(
            cloned.storage().list_buckets().expect("list from clone"),
            state.storage().list_buckets().expect("list from original")
        );
    }
}
