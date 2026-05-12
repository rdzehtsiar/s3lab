// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::net::{IpAddr, Ipv6Addr};
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub const DEFAULT_HOST: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 9000;
pub const DEFAULT_INSPECTOR_HOST: &str = "127.0.0.1";
pub const DEFAULT_INSPECTOR_PORT: u16 = 9001;
pub const DEFAULT_DATA_DIR: &str = "./s3lab-data";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub host: String,
    pub port: u16,
    pub inspector_host: String,
    pub inspector_port: u16,
    pub data_dir: PathBuf,
}

#[derive(Debug)]
pub enum ConfigError {
    NonLoopbackHost {
        host: String,
    },
    DataDirIsFile {
        path: PathBuf,
    },
    DataDirIsReparsePoint {
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
            inspector_host: DEFAULT_INSPECTOR_HOST.to_owned(),
            inspector_port: DEFAULT_INSPECTOR_PORT,
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
        }
    }
}

impl RuntimeConfig {
    pub fn new(host: impl Into<String>, port: u16, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            host: host.into(),
            port,
            inspector_host: DEFAULT_INSPECTOR_HOST.to_owned(),
            inspector_port: DEFAULT_INSPECTOR_PORT,
            data_dir: data_dir.into(),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}:{}", host_for_endpoint(&self.host), self.port)
    }

    pub fn inspector_endpoint(&self) -> String {
        format!(
            "http://{}:{}",
            host_for_endpoint(&self.inspector_host),
            self.inspector_port
        )
    }

    pub fn bind_host(&self) -> &str {
        bind_host(&self.host)
    }

    pub fn inspector_bind_host(&self) -> &str {
        bind_host(&self.inspector_host)
    }

    pub fn with_inspector(
        mut self,
        inspector_host: impl Into<String>,
        inspector_port: u16,
    ) -> Self {
        self.inspector_host = inspector_host.into();
        self.inspector_port = inspector_port;
        self
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_loopback_host(&self.host)?;
        validate_loopback_host(&self.inspector_host)?;

        Ok(())
    }

    pub fn ensure_data_dir(&self) -> Result<(), ConfigError> {
        ensure_data_dir(&self.data_dir)
    }
}

fn bind_host(host: &str) -> &str {
    if host.eq_ignore_ascii_case("localhost") {
        "127.0.0.1"
    } else {
        host
    }
}

fn validate_loopback_host(host: &str) -> Result<(), ConfigError> {
    if !is_loopback_host(host) {
        return Err(ConfigError::NonLoopbackHost {
            host: host.to_owned(),
        });
    }

    Ok(())
}

pub fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn host_for_endpoint(host: &str) -> String {
    if host.parse::<Ipv6Addr>().is_ok() {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

pub fn ensure_data_dir(path: &Path) -> Result<(), ConfigError> {
    reject_existing_reparse_points(path)?;

    if path.is_file() {
        return Err(ConfigError::DataDirIsFile {
            path: path.to_path_buf(),
        });
    }

    fs::create_dir_all(path).map_err(|source| ConfigError::CreateDataDir {
        path: path.to_path_buf(),
        source,
    })?;

    let metadata = fs::symlink_metadata(path).map_err(|source| ConfigError::CreateDataDir {
        path: path.to_path_buf(),
        source,
    })?;
    if is_reparse_point(&metadata) {
        return Err(ConfigError::DataDirIsReparsePoint {
            path: path.to_path_buf(),
        });
    }
    if !metadata.is_dir() {
        return Err(ConfigError::DataDirIsFile {
            path: path.to_path_buf(),
        });
    }

    Ok(())
}

fn reject_existing_reparse_points(path: &Path) -> Result<(), ConfigError> {
    for ancestor in path.ancestors() {
        if ancestor.as_os_str().is_empty() {
            continue;
        }

        let metadata = match fs::symlink_metadata(ancestor) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(ConfigError::CreateDataDir {
                    path: ancestor.to_path_buf(),
                    source,
                });
            }
        };

        if is_reparse_point(&metadata) {
            return Err(ConfigError::DataDirIsReparsePoint {
                path: ancestor.to_path_buf(),
            });
        }
    }

    Ok(())
}

fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink() || has_windows_reparse_point_attribute(metadata)
}

#[cfg(windows)]
fn has_windows_reparse_point_attribute(metadata: &fs::Metadata) -> bool {
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;

    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn has_windows_reparse_point_attribute(_metadata: &fs::Metadata) -> bool {
    false
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonLoopbackHost { host } => write!(
                formatter,
                "serve --host must be a loopback host for Phase 1; got {host}. Use 127.0.0.1, localhost, or ::1."
            ),
            Self::DataDirIsFile { path } => write!(
                formatter,
                "data dir path is a file, not a directory: {}",
                path.display()
            ),
            Self::DataDirIsReparsePoint { path } => write!(
                formatter,
                "data dir path must be a real directory, not a symlink or reparse point: {}",
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
            Self::NonLoopbackHost { .. } => None,
            Self::DataDirIsFile { .. } => None,
            Self::DataDirIsReparsePoint { .. } => None,
            Self::CreateDataDir { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_data_dir, is_loopback_host, ConfigError, RuntimeConfig, DEFAULT_DATA_DIR,
        DEFAULT_HOST, DEFAULT_INSPECTOR_HOST, DEFAULT_INSPECTOR_PORT, DEFAULT_PORT,
    };
    use std::error::Error;
    use std::fmt::{Display, Formatter};
    use std::io;
    use std::path::{Path, PathBuf};

    #[test]
    fn default_config_uses_local_only_endpoint() {
        assert_eq!(
            RuntimeConfig::default(),
            RuntimeConfig {
                host: DEFAULT_HOST.to_owned(),
                port: DEFAULT_PORT,
                inspector_host: DEFAULT_INSPECTOR_HOST.to_owned(),
                inspector_port: DEFAULT_INSPECTOR_PORT,
                data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            }
        );
    }

    #[test]
    fn explicit_config_overrides_defaults() {
        let config = RuntimeConfig::new("localhost", 4567, "custom-data");

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 4567);
        assert_eq!(config.inspector_host, DEFAULT_INSPECTOR_HOST);
        assert_eq!(config.inspector_port, DEFAULT_INSPECTOR_PORT);
        assert_eq!(config.data_dir, PathBuf::from("custom-data"));
    }

    #[test]
    fn explicit_inspector_config_overrides_inspector_defaults() {
        let config =
            RuntimeConfig::new("127.0.0.1", 4567, "custom-data").with_inspector("::1", 7654);

        assert_eq!(config.inspector_host, "::1");
        assert_eq!(config.inspector_port, 7654);
        assert_eq!(config.inspector_endpoint(), "http://[::1]:7654");
    }

    #[test]
    fn endpoint_formats_http_host_and_port() {
        let config = RuntimeConfig::new(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATA_DIR);

        assert_eq!(config.endpoint(), "http://127.0.0.1:9000");
    }

    #[test]
    fn endpoint_formats_ipv6_loopback_with_brackets() {
        let config = RuntimeConfig::new("::1", DEFAULT_PORT, DEFAULT_DATA_DIR);

        assert_eq!(config.endpoint(), "http://[::1]:9000");
    }

    #[test]
    fn bind_host_normalizes_localhost_without_changing_ip_literals() {
        assert_eq!(
            RuntimeConfig::new("localhost", DEFAULT_PORT, DEFAULT_DATA_DIR).bind_host(),
            "127.0.0.1"
        );
        assert_eq!(
            RuntimeConfig::new("LOCALHOST", DEFAULT_PORT, DEFAULT_DATA_DIR).bind_host(),
            "127.0.0.1"
        );
        assert_eq!(
            RuntimeConfig::new("127.0.0.1", DEFAULT_PORT, DEFAULT_DATA_DIR).bind_host(),
            "127.0.0.1"
        );
        assert_eq!(
            RuntimeConfig::new("::1", DEFAULT_PORT, DEFAULT_DATA_DIR).bind_host(),
            "::1"
        );
        assert_eq!(
            RuntimeConfig::new("127.0.0.1", DEFAULT_PORT, DEFAULT_DATA_DIR)
                .with_inspector("localhost", DEFAULT_INSPECTOR_PORT)
                .inspector_bind_host(),
            "127.0.0.1"
        );
    }

    #[test]
    fn loopback_host_validation_accepts_phase1_local_hosts() {
        for host in ["127.0.0.1", "127.42.0.1", "localhost", "LOCALHOST", "::1"] {
            assert!(is_loopback_host(host), "host should be loopback: {host}");
            RuntimeConfig::new(host, DEFAULT_PORT, DEFAULT_DATA_DIR)
                .validate()
                .expect("loopback host should validate");
        }
    }

    #[test]
    fn loopback_host_validation_rejects_wildcard_and_non_loopback_hosts() {
        for host in ["0.0.0.0", "::", "192.168.1.10", "example.com", ""] {
            let error = RuntimeConfig::new(host, DEFAULT_PORT, DEFAULT_DATA_DIR)
                .validate()
                .expect_err("non-loopback host should fail");

            assert!(matches!(
                error,
                ConfigError::NonLoopbackHost { host: ref failed_host } if failed_host == host
            ));
            assert!(error.to_string().contains("loopback host"));
        }
    }

    #[test]
    fn loopback_host_validation_rejects_non_loopback_inspector_hosts() {
        let error = RuntimeConfig::new("127.0.0.1", DEFAULT_PORT, DEFAULT_DATA_DIR)
            .with_inspector("example.com", DEFAULT_INSPECTOR_PORT)
            .validate()
            .expect_err("non-loopback inspector host should fail");

        assert!(matches!(
            error,
            ConfigError::NonLoopbackHost { ref host } if host == "example.com"
        ));
        assert!(error.to_string().contains("loopback host"));
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
        assert!(debug.contains("inspector_host"));
        assert!(debug.contains("inspector_port"));
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

    #[test]
    fn ensure_data_dir_rejects_symlinked_directory_when_platform_allows_symlinks() {
        let parent = tempfile::tempdir().expect("temp dir");
        let target = parent.path().join("target-data");
        let data_dir = parent.path().join("s3lab-data");
        std::fs::create_dir(&target).expect("create target dir");
        if let Err(skip) = create_dir_symlink_or_skip(&target, &data_dir) {
            println!("{skip}");
            return;
        }

        let error = ensure_data_dir(&data_dir).expect_err("symlinked data dir should fail");

        assert!(
            matches!(error, ConfigError::DataDirIsReparsePoint { ref path } if path == &data_dir)
        );
        assert!(error.to_string().contains("symlink or reparse point"));
    }

    #[test]
    fn ensure_data_dir_rejects_symlinked_existing_ancestor_when_platform_allows_symlinks() {
        let parent = tempfile::tempdir().expect("temp dir");
        let target = parent.path().join("target-data");
        let link = parent.path().join("linked-data");
        let data_dir = link.join("nested").join("s3lab-data");
        std::fs::create_dir(&target).expect("create target dir");
        if let Err(skip) = create_dir_symlink_or_skip(&target, &link) {
            println!("{skip}");
            return;
        }

        let error = ensure_data_dir(&data_dir).expect_err("symlinked ancestor should fail");

        assert!(matches!(error, ConfigError::DataDirIsReparsePoint { ref path } if path == &link));
        assert!(error.to_string().contains("symlink or reparse point"));
        assert!(
            !target.join("nested").exists(),
            "data dir creation must not follow a symlinked ancestor"
        );
    }

    #[test]
    fn ensure_data_dir_reports_create_failures() {
        let parent = tempfile::tempdir().expect("temp dir");
        let file_path = parent.path().join("not-a-directory");
        let data_dir = file_path.join("s3lab-data");
        std::fs::write(&file_path, b"not a directory").expect("write test file");

        let error = ensure_data_dir(&data_dir).expect_err("nested path under file should fail");

        assert!(matches!(error, ConfigError::CreateDataDir { ref path, .. } if path == &data_dir));
        assert!(error.source().is_some());
        assert!(error
            .to_string()
            .contains("failed to create or open data dir"));
    }

    #[test]
    fn data_dir_reparse_point_error_display_is_actionable_and_has_no_source() {
        let path = PathBuf::from("linked-data");
        let error = ConfigError::DataDirIsReparsePoint { path: path.clone() };

        assert!(error.to_string().contains("symlink or reparse point"));
        assert!(error.to_string().contains(&path.display().to_string()));
        assert!(error.source().is_none());
    }

    #[test]
    fn create_data_dir_error_display_includes_source_and_exposes_it() {
        let path = PathBuf::from("missing").join("s3lab-data");
        let error = ConfigError::CreateDataDir {
            path: path.clone(),
            source: io::Error::new(io::ErrorKind::PermissionDenied, "permission denied"),
        };

        assert!(error
            .to_string()
            .contains("failed to create or open data dir"));
        assert!(error.to_string().contains(&path.display().to_string()));
        assert!(error.to_string().contains("permission denied"));
        assert_eq!(
            error.source().expect("create error has source").to_string(),
            "permission denied"
        );
    }

    #[test]
    fn runtime_config_ensure_data_dir_delegates_to_configured_path() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("configured-data");
        let config = RuntimeConfig::new(DEFAULT_HOST, DEFAULT_PORT, data_dir.clone());

        config
            .ensure_data_dir()
            .expect("configured dir should be created");

        assert!(data_dir.is_dir());
    }

    fn create_dir_symlink_or_skip(target: &Path, link: &Path) -> Result<(), SymlinkTestSkipped> {
        try_create_dir_symlink(target, link).map_err(|source| SymlinkTestSkipped {
            target: target.to_path_buf(),
            link: link.to_path_buf(),
            source,
        })
    }

    struct SymlinkTestSkipped {
        target: PathBuf,
        link: PathBuf,
        source: io::Error,
    }

    impl Display for SymlinkTestSkipped {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                formatter,
                "skipped config data-dir symlink safety test: symlink creation unavailable from {} to {}: {}",
                self.link.display(),
                self.target.display(),
                self.source
            )
        }
    }

    #[cfg(unix)]
    fn try_create_dir_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn try_create_dir_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }
}
