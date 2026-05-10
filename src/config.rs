// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

pub const DEFAULT_HOST: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 9000;
pub const DEFAULT_DATA_DIR: &str = "./s3lab-data";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: PathBuf,
}

#[derive(Debug)]
pub enum ConfigError {
    DataDirIsFile {
        path: PathBuf,
    },
    CreateDataDir {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_owned(),
            port: DEFAULT_PORT,
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
        }
    }
}

impl RuntimeConfig {
    pub fn new(host: impl Into<String>, port: u16, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            host: host.into(),
            port,
            data_dir: data_dir.into(),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    pub fn ensure_data_dir(&self) -> Result<(), ConfigError> {
        ensure_data_dir(&self.data_dir)
    }
}

pub fn ensure_data_dir(path: &Path) -> Result<(), ConfigError> {
    if path.is_file() {
        return Err(ConfigError::DataDirIsFile {
            path: path.to_path_buf(),
        });
    }

    std::fs::create_dir_all(path).map_err(|source| ConfigError::CreateDataDir {
        path: path.to_path_buf(),
        source,
    })
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DataDirIsFile { path } => write!(
                formatter,
                "data dir path is a file, not a directory: {}",
                path.display()
            ),
            Self::CreateDataDir { path, source } => write!(
                formatter,
                "failed to create or open data dir {}: {source}",
                path.display()
            ),
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DataDirIsFile { .. } => None,
            Self::CreateDataDir { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_data_dir, ConfigError, RuntimeConfig, DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_PORT,
    };
    use std::path::PathBuf;

    #[test]
    fn default_config_uses_local_only_endpoint() {
        assert_eq!(
            RuntimeConfig::default(),
            RuntimeConfig {
                host: DEFAULT_HOST.to_owned(),
                port: DEFAULT_PORT,
                data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            }
        );
    }

    #[test]
    fn explicit_config_overrides_defaults() {
        let config = RuntimeConfig::new("0.0.0.0", 4567, "custom-data");

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 4567);
        assert_eq!(config.data_dir, PathBuf::from("custom-data"));
    }

    #[test]
    fn endpoint_formats_http_host_and_port() {
        let config = RuntimeConfig::new(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATA_DIR);

        assert_eq!(config.endpoint(), "http://127.0.0.1:9000");
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

    #[test]
    fn ensure_data_dir_creates_missing_directory() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("missing").join("s3lab-data");

        ensure_data_dir(&data_dir).expect("data dir should be created");

        assert!(data_dir.is_dir());
    }

    #[test]
    fn ensure_data_dir_accepts_existing_directory() {
        let data_dir = tempfile::tempdir().expect("temp dir");

        ensure_data_dir(data_dir.path()).expect("existing dir should be accepted");
    }

    #[test]
    fn ensure_data_dir_rejects_existing_file() {
        let parent = tempfile::tempdir().expect("temp dir");
        let file_path = parent.path().join("s3lab-data");
        std::fs::write(&file_path, b"not a directory").expect("write test file");

        let error = ensure_data_dir(&file_path).expect_err("file path should fail");

        assert!(matches!(error, ConfigError::DataDirIsFile { ref path } if path == &file_path));
        assert!(error.to_string().contains("not a directory"));
    }
}
