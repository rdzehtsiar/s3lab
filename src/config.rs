// SPDX-License-Identifier: Apache-2.0

pub const DEFAULT_HOST: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 9000;
pub const DEFAULT_DATA_DIR: &str = "./s3lab-data";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: String,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_owned(),
            port: DEFAULT_PORT,
            data_dir: DEFAULT_DATA_DIR.to_owned(),
        }
    }
}
