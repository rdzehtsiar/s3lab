// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ServerState {
    pub data_dir: String,
}

impl ServerState {
    pub fn new(data_dir: impl Into<String>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }
}
