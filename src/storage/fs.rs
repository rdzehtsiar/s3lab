// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FilesystemStorage {
    pub root: String,
}

impl FilesystemStorage {
    pub fn new(root: impl Into<String>) -> Self {
        Self { root: root.into() }
    }
}
