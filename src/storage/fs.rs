// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FilesystemStorage {
    pub root: PathBuf,
}

impl FilesystemStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::FilesystemStorage;

    #[test]
    fn new_stores_root_without_normalizing() {
        let storage = FilesystemStorage::new("./s3lab-data");

        assert_eq!(storage.root, std::path::PathBuf::from("./s3lab-data"));
    }

    #[test]
    fn storage_can_be_cloned_without_changing_values() {
        let storage = FilesystemStorage::new("data");

        assert_eq!(storage.clone(), storage);
    }
}
