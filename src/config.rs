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

#[cfg(test)]
mod tests {
    use super::{RuntimeConfig, DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_PORT};

    #[test]
    fn default_config_uses_local_only_endpoint() {
        assert_eq!(
            RuntimeConfig::default(),
            RuntimeConfig {
                host: DEFAULT_HOST.to_owned(),
                port: DEFAULT_PORT,
                data_dir: DEFAULT_DATA_DIR.to_owned(),
            }
        );
    }

    #[test]
    fn config_can_be_cloned_without_changing_values() {
        let config = RuntimeConfig::default();

        assert_eq!(config.clone(), config);
    }

    #[test]
    fn debug_output_names_config_fields() {
        let debug = format!("{:?}", RuntimeConfig::default());

        assert!(debug.contains("host"));
        assert!(debug.contains("port"));
        assert!(debug.contains("data_dir"));
    }
}
