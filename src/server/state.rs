// SPDX-License-Identifier: Apache-2.0

use crate::storage::Storage;
use std::sync::Arc;

#[derive(Clone)]
pub struct ServerState {
    storage: Arc<dyn Storage + Send + Sync>,
}

impl ServerState {
    pub fn from_storage(storage: impl Storage + Send + Sync + 'static) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }

    pub fn storage(&self) -> &(dyn Storage + Send + Sync) {
        self.storage.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::ServerState;
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
}
