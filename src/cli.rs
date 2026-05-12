// SPDX-License-Identifier: Apache-2.0

use crate::config::{
    RuntimeConfig, DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_INSPECTOR_HOST, DEFAULT_INSPECTOR_PORT,
    DEFAULT_PORT,
};
use crate::server::state::ServerState;
use crate::storage::fs::FilesystemStorage;
use clap::{Parser, Subcommand};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
pub enum CliError {
    Parse(clap::Error),
    Config(crate::config::ConfigError),
    Storage(crate::storage::StorageError),
    Server(crate::server::ServerError),
    ShutdownSignal(std::io::Error),
    Output(std::io::Error),
}

#[derive(Debug, Parser)]
#[command(name = "s3lab")]
#[command(about = "Offline S3 compatibility and protocol debugging lab.")]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the local S3 endpoint.
    Serve(ServeArgs),
    /// Save or restore local storage snapshots.
    Snapshot(SnapshotArgs),
    /// Remove current local storage state while preserving snapshots.
    Reset(ResetArgs),
}

#[derive(Debug, Parser)]
pub struct ServeArgs {
    /// Host address for the local endpoint.
    #[arg(long, default_value = DEFAULT_HOST)]
    host: String,

    /// Port for the local endpoint.
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Host address for the local inspector UI.
    #[arg(long, default_value = DEFAULT_INSPECTOR_HOST)]
    inspector_host: String,

    /// Port for the local inspector UI.
    #[arg(long, default_value_t = DEFAULT_INSPECTOR_PORT)]
    inspector_port: u16,

    /// Directory used for local S3Lab data.
    #[arg(long, default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,
}

#[derive(Debug, Parser)]
struct SnapshotArgs {
    #[command(subcommand)]
    command: SnapshotCommand,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommand {
    /// Save the current local storage state as a named snapshot.
    Save(SnapshotOperationArgs),
    /// Restore current local storage state from a named snapshot.
    Restore(SnapshotOperationArgs),
}

#[derive(Debug, Parser)]
struct SnapshotOperationArgs {
    /// Snapshot name.
    name: String,

    /// Directory used for local S3Lab data.
    #[arg(long, default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,
}

#[derive(Debug, Parser)]
struct ResetArgs {
    /// Directory used for local S3Lab data.
    #[arg(long, default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,
}

impl From<ServeArgs> for RuntimeConfig {
    fn from(args: ServeArgs) -> Self {
        Self::new(args.host, args.port, args.data_dir)
            .with_inspector(args.inspector_host, args.inspector_port)
    }
}

impl Display for CliError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(error) => Display::fmt(error, formatter),
            Self::Config(error) => Display::fmt(error, formatter),
            Self::Storage(error) => Display::fmt(error, formatter),
            Self::Server(error) => Display::fmt(error, formatter),
            Self::ShutdownSignal(error) => {
                write!(
                    formatter,
                    "failed to register Ctrl-C shutdown signal: {error}"
                )
            }
            Self::Output(error) => write!(formatter, "failed to write command output: {error}"),
        }
    }
}

impl CliError {
    pub fn exit_code(&self) -> ExitCode {
        match self {
            Self::Parse(error) if error.use_stderr() => ExitCode::FAILURE,
            Self::Parse(_) => ExitCode::SUCCESS,
            Self::Config(_)
            | Self::Storage(_)
            | Self::Server(_)
            | Self::ShutdownSignal(_)
            | Self::Output(_) => ExitCode::FAILURE,
        }
    }

    pub fn print(&self) {
        match self {
            Self::Parse(error) => {
                let _ = error.print();
            }
            Self::Config(_)
            | Self::Storage(_)
            | Self::Server(_)
            | Self::ShutdownSignal(_)
            | Self::Output(_) => eprintln!("{self}"),
        }
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse(error) => Some(error),
            Self::Config(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Server(error) => Some(error),
            Self::ShutdownSignal(error) => Some(error),
            Self::Output(error) => Some(error),
        }
    }
}

impl From<clap::Error> for CliError {
    fn from(error: clap::Error) -> Self {
        Self::Parse(error)
    }
}

impl From<crate::config::ConfigError> for CliError {
    fn from(error: crate::config::ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<crate::storage::StorageError> for CliError {
    fn from(error: crate::storage::StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<crate::server::ServerError> for CliError {
    fn from(error: crate::server::ServerError) -> Self {
        Self::Server(error)
    }
}

impl From<std::io::Error> for CliError {
    fn from(error: std::io::Error) -> Self {
        Self::Output(error)
    }
}

pub async fn run<I, S>(args: I) -> Result<(), CliError>
where
    I: IntoIterator<Item = S>,
    S: Into<std::ffi::OsString> + Clone,
{
    let mut stdout = std::io::stdout();

    run_with_writer(args, &mut stdout).await
}

pub async fn run_with_writer<I, S, W>(args: I, writer: &mut W) -> Result<(), CliError>
where
    I: IntoIterator<Item = S>,
    S: Into<std::ffi::OsString> + Clone,
    W: Write,
{
    let cli = Cli::try_parse_from(args)?;

    match cli.command {
        Command::Serve(args) => run_serve(args.into(), writer).await,
        Command::Snapshot(args) => run_snapshot(args, writer),
        Command::Reset(args) => run_reset(args, writer),
    }
}

fn run_snapshot<W>(args: SnapshotArgs, writer: &mut W) -> Result<(), CliError>
where
    W: Write,
{
    match args.command {
        SnapshotCommand::Save(args) => {
            let storage = open_storage(args.data_dir)?;
            storage.save_snapshot(&args.name)?;
            writeln!(writer, "Snapshot saved: {}", args.name)?;
        }
        SnapshotCommand::Restore(args) => {
            let storage = open_storage(args.data_dir)?;
            storage.restore_snapshot(&args.name)?;
            writeln!(writer, "Snapshot restored: {}", args.name)?;
        }
    }

    writer.flush()?;
    Ok(())
}

fn run_reset<W>(args: ResetArgs, writer: &mut W) -> Result<(), CliError>
where
    W: Write,
{
    let data_dir = args.data_dir;
    let storage = open_storage(data_dir)?;
    storage.reset()?;
    writeln!(writer, "Storage reset: {}", storage.root().display())?;
    writer.flush()?;
    Ok(())
}

fn open_storage(data_dir: PathBuf) -> Result<FilesystemStorage, CliError> {
    crate::config::ensure_data_dir(&data_dir)?;

    Ok(FilesystemStorage::new(data_dir))
}

async fn run_serve<W>(config: RuntimeConfig, writer: &mut W) -> Result<(), CliError>
where
    W: Write,
{
    run_serve_until(config, writer, async {
        tokio::signal::ctrl_c()
            .await
            .map_err(CliError::ShutdownSignal)
    })
    .await
}

async fn run_serve_until<W, F>(
    config: RuntimeConfig,
    writer: &mut W,
    shutdown: F,
) -> Result<(), CliError>
where
    W: Write,
    F: std::future::Future<Output = Result<(), CliError>> + Send + 'static,
{
    init_tracing();

    config.validate()?;
    config.ensure_data_dir()?;
    let listener = crate::server::bind_listener(&config).await?;
    let endpoint = crate::server::listener_endpoint(&listener)?;
    let inspector_listener = crate::server::bind_inspector_listener(&config).await?;
    let inspector_endpoint = crate::server::listener_endpoint(&inspector_listener)?;

    {
        writeln!(writer, "S3 endpoint:  {endpoint}")?;
        writeln!(writer, "Inspector UI: {inspector_endpoint}")?;
        writeln!(writer, "Data dir:     {}", config.data_dir.display())?;
        writer.flush()?;
    }
    tracing::info!(
        endpoint = %endpoint,
        inspector_endpoint = %inspector_endpoint,
        data_dir = %config.data_dir.display(),
        "local server and inspector listening"
    );

    let (shutdown_error_tx, shutdown_error_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        if let Err(error) = shutdown.await {
            let _ = shutdown_error_tx.send(error);
        }
        let _ = shutdown_tx.send(true);
    });

    let state = ServerState::from_storage(FilesystemStorage::new(config.data_dir));
    let mut s3_shutdown_rx = shutdown_rx.clone();
    let s3_shutdown = async move {
        let _ = s3_shutdown_rx.changed().await;
    };
    let mut inspector_shutdown_rx = shutdown_rx;
    let inspector_shutdown = async move {
        let _ = inspector_shutdown_rx.changed().await;
    };

    tokio::try_join!(
        crate::server::serve_listener_until(listener, state, s3_shutdown),
        crate::server::serve_inspector_listener_until(inspector_listener, inspector_shutdown)
    )?;

    if let Ok(error) = shutdown_error_rx.await {
        return Err(error);
    }

    Ok(())
}

fn init_tracing() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("s3lab=info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
async fn run_with_writer_until<I, S, W, F>(
    args: I,
    writer: &mut W,
    shutdown: F,
) -> Result<(), CliError>
where
    I: IntoIterator<Item = S>,
    S: Into<std::ffi::OsString> + Clone,
    W: Write,
    F: std::future::Future<Output = Result<(), CliError>> + Send + 'static,
{
    let cli = Cli::try_parse_from(args)?;

    match cli.command {
        Command::Serve(args) => run_serve_until(args.into(), writer, shutdown).await,
        Command::Snapshot(args) => run_snapshot(args, writer),
        Command::Reset(args) => run_reset(args, writer),
    }
}

#[cfg(test)]
mod tests {
    use super::{init_tracing, run_with_writer, run_with_writer_until, Cli, CliError};
    use crate::config::DEFAULT_DATA_DIR;
    use crate::s3::bucket::BucketName;
    use crate::s3::object::ObjectKey;
    use crate::storage::fs::FilesystemStorage;
    use crate::storage::{PutObjectRequest, Storage};
    use clap::{CommandFactory, Parser};
    use std::collections::BTreeMap;
    use std::error::Error;
    use std::io::Write;
    use std::path::Path;
    use std::path::PathBuf;
    use std::process::ExitCode;

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "test writer failed",
            ))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn tracing_initialization_is_idempotent_for_tests() {
        init_tracing();
        init_tracing();
    }

    #[tokio::test]
    async fn default_serve_config_binds_and_prints_local_startup_output() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let mut output = Vec::new();

        run_with_writer_until(
            [
                "s3lab",
                "serve",
                "--port",
                "0",
                "--inspector-port",
                "0",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
            async { Ok(()) },
        )
        .await
        .expect("serve should validate config");

        let output = String::from_utf8(output).expect("utf-8 output");
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("S3 endpoint:  http://127.0.0.1:"));
        assert_ne!(lines[0], "S3 endpoint:  http://127.0.0.1:0");
        assert!(lines[1].starts_with("Inspector UI: http://127.0.0.1:"));
        assert_ne!(lines[1], "Inspector UI: http://127.0.0.1:0");
        assert_eq!(lines[2], format!("Data dir:     {}", data_dir.display()));
        assert!(data_dir.is_dir());
    }

    #[tokio::test]
    async fn explicit_serve_options_override_defaults() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("custom-data");
        let mut output = Vec::new();

        run_with_writer_until(
            [
                "s3lab",
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                "0",
                "--inspector-host",
                "127.0.0.1",
                "--inspector-port",
                "0",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
            async { Ok(()) },
        )
        .await
        .expect("serve should validate config");

        let output = String::from_utf8(output).expect("utf-8 output");
        assert!(output.contains("S3 endpoint:  http://127.0.0.1:"));
        assert!(output.contains("Inspector UI: http://127.0.0.1:"));
        assert!(output.contains(&format!("Data dir:     {}", data_dir.display())));
    }

    #[test]
    fn parsed_serve_defaults_match_phase1_contract() {
        let cli = Cli::try_parse_from(["s3lab", "serve"]).expect("valid serve command");
        let super::Command::Serve(args) = cli.command else {
            panic!("parsed serve command");
        };

        assert_eq!(args.host, "127.0.0.1");
        assert_eq!(args.port, 9000);
        assert_eq!(args.inspector_host, "127.0.0.1");
        assert_eq!(args.inspector_port, 9001);
        assert_eq!(args.data_dir, PathBuf::from("./s3lab-data"));
    }

    #[test]
    fn parsed_snapshot_and_reset_defaults_match_storage_contract() {
        let cli =
            Cli::try_parse_from(["s3lab", "snapshot", "save", "baseline"]).expect("snapshot save");
        let super::Command::Snapshot(args) = cli.command else {
            panic!("parsed snapshot command");
        };
        let super::SnapshotCommand::Save(args) = args.command else {
            panic!("parsed snapshot save command");
        };

        assert_eq!(args.name, "baseline");
        assert_eq!(args.data_dir, PathBuf::from(DEFAULT_DATA_DIR));

        let cli = Cli::try_parse_from(["s3lab", "snapshot", "restore", "baseline"])
            .expect("snapshot restore");
        let super::Command::Snapshot(args) = cli.command else {
            panic!("parsed snapshot command");
        };
        let super::SnapshotCommand::Restore(args) = args.command else {
            panic!("parsed snapshot restore command");
        };

        assert_eq!(args.name, "baseline");
        assert_eq!(args.data_dir, PathBuf::from(DEFAULT_DATA_DIR));

        let cli = Cli::try_parse_from(["s3lab", "reset"]).expect("reset");
        let super::Command::Reset(args) = cli.command else {
            panic!("parsed reset command");
        };

        assert_eq!(args.data_dir, PathBuf::from(DEFAULT_DATA_DIR));
    }

    #[test]
    fn invalid_port_is_rejected() {
        let error =
            Cli::try_parse_from(["s3lab", "serve", "--port", "not-a-port"]).expect_err("bad port");

        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn unknown_command_is_rejected() {
        let error = Cli::try_parse_from(["s3lab", "unknown"]).expect_err("unknown command");

        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn unknown_serve_option_is_rejected() {
        let error =
            Cli::try_parse_from(["s3lab", "serve", "--unknown"]).expect_err("unknown option");

        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn serve_help_documents_command_and_options() {
        let mut command = Cli::command();
        let command = command
            .find_subcommand_mut("serve")
            .expect("serve subcommand");
        let mut help = Vec::new();

        command.write_long_help(&mut help).expect("write help");

        let help = String::from_utf8(help).expect("utf-8 help");
        assert!(help.contains("serve"));
        assert!(help.contains("--host"));
        assert!(help.contains("--port"));
        assert!(help.contains("--inspector-host"));
        assert!(help.contains("--inspector-port"));
        assert!(help.contains("--data-dir"));
    }

    #[test]
    fn snapshot_and_reset_help_documents_commands_and_options() {
        let mut root = Cli::command();
        let snapshot = root
            .find_subcommand_mut("snapshot")
            .expect("snapshot subcommand");
        let mut snapshot_help = Vec::new();

        snapshot
            .write_long_help(&mut snapshot_help)
            .expect("write snapshot help");

        let snapshot_help = String::from_utf8(snapshot_help).expect("utf-8 help");
        assert!(snapshot_help.contains("snapshot"));
        assert!(snapshot_help.contains("save"));
        assert!(snapshot_help.contains("restore"));

        let save = snapshot
            .find_subcommand_mut("save")
            .expect("snapshot save subcommand");
        let mut save_help = Vec::new();

        save.write_long_help(&mut save_help)
            .expect("write snapshot save help");

        let save_help = String::from_utf8(save_help).expect("utf-8 help");
        assert!(save_help.contains("<NAME>"));
        assert!(save_help.contains("--data-dir"));

        let reset = root.find_subcommand_mut("reset").expect("reset subcommand");
        let mut reset_help = Vec::new();

        reset
            .write_long_help(&mut reset_help)
            .expect("write reset help");

        let reset_help = String::from_utf8(reset_help).expect("utf-8 help");
        assert!(reset_help.contains("reset"));
        assert!(reset_help.contains("--data-dir"));
    }

    #[tokio::test]
    async fn snapshot_save_prints_success_and_creates_missing_data_dir() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("missing").join("s3lab-data");
        let mut output = Vec::new();

        run_with_writer(
            [
                "s3lab",
                "snapshot",
                "save",
                "baseline",
                "--data-dir",
                path_str(&data_dir),
            ],
            &mut output,
        )
        .await
        .expect("snapshot save succeeds");

        assert_eq!(
            String::from_utf8(output).expect("utf-8 output"),
            "Snapshot saved: baseline\n"
        );
        assert!(data_dir.is_dir());
        assert!(data_dir.join("snapshots").join("baseline").is_dir());
    }

    #[tokio::test]
    async fn snapshot_restore_prints_success_and_restores_saved_state() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let storage = FilesystemStorage::new(&data_dir);
        put_test_object(&storage, b"before restore");
        let mut output = Vec::new();
        let mut save_output = Vec::new();

        run_with_writer(
            [
                "s3lab",
                "snapshot",
                "save",
                "baseline",
                "--data-dir",
                path_str(&data_dir),
            ],
            &mut save_output,
        )
        .await
        .expect("snapshot save succeeds");
        put_test_object(&storage, b"after snapshot");

        run_with_writer(
            [
                "s3lab",
                "snapshot",
                "restore",
                "baseline",
                "--data-dir",
                path_str(&data_dir),
            ],
            &mut output,
        )
        .await
        .expect("snapshot restore succeeds");

        assert_eq!(
            String::from_utf8(output).expect("utf-8 output"),
            "Snapshot restored: baseline\n"
        );
        assert_eq!(test_object_bytes(&storage), b"before restore");
    }

    #[tokio::test]
    async fn reset_prints_success_removes_current_state_and_preserves_snapshots() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let storage = FilesystemStorage::new(&data_dir);
        put_test_object(&storage, b"snapshot body");
        storage.save_snapshot("baseline").expect("save snapshot");
        let mut output = Vec::new();

        run_with_writer(
            ["s3lab", "reset", "--data-dir", path_str(&data_dir)],
            &mut output,
        )
        .await
        .expect("reset succeeds");

        assert_eq!(
            String::from_utf8(output).expect("utf-8 output"),
            format!("Storage reset: {}\n", data_dir.display())
        );
        assert!(!storage
            .bucket_exists(&BucketName::new("cli-bucket"))
            .expect("bucket existence is readable"));
        assert!(data_dir.join("snapshots").join("baseline").is_dir());

        storage
            .restore_snapshot("baseline")
            .expect("preserved snapshot restores");
        assert_eq!(test_object_bytes(&storage), b"snapshot body");
    }

    #[tokio::test]
    async fn snapshot_commands_preserve_storage_errors_for_invalid_names_and_missing_snapshots() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");

        for args in [
            ["s3lab", "snapshot", "save", "bad/name", "--data-dir"],
            ["s3lab", "snapshot", "restore", "bad/name", "--data-dir"],
        ] {
            let mut output = Vec::new();
            let error = run_with_writer(args.into_iter().chain([path_str(&data_dir)]), &mut output)
                .await
                .expect_err("invalid snapshot name fails");

            assert!(matches!(error, CliError::Storage(_)));
            assert!(error.to_string().contains("invalid snapshot name"));
            assert!(output.is_empty());
        }

        let mut output = Vec::new();
        let error = run_with_writer(
            [
                "s3lab",
                "snapshot",
                "restore",
                "missing",
                "--data-dir",
                path_str(&data_dir),
            ],
            &mut output,
        )
        .await
        .expect_err("missing snapshot fails");

        assert!(matches!(error, CliError::Storage(_)));
        assert!(error
            .to_string()
            .contains("snapshot does not exist: missing"));
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn snapshot_and_reset_reject_file_data_dirs_before_storage_operations() {
        let parent = tempfile::tempdir().expect("temp dir");
        let file_path = parent.path().join("s3lab-data");
        std::fs::write(&file_path, b"not a directory").expect("write test file");

        for args in [
            ["s3lab", "snapshot", "save", "baseline", "--data-dir"],
            ["s3lab", "reset", "--data-dir", "", ""],
        ] {
            let args = args
                .into_iter()
                .filter(|arg| !arg.is_empty())
                .chain([path_str(&file_path)]);
            let mut output = Vec::new();

            let error = run_with_writer(args, &mut output)
                .await
                .expect_err("file data dir should fail");

            assert!(matches!(error, CliError::Config(_)));
            assert!(error.to_string().contains("not a directory"));
            assert!(output.is_empty());
        }
    }

    #[tokio::test]
    async fn serve_rejects_file_as_data_dir() {
        let parent = tempfile::tempdir().expect("temp dir");
        let file_path = parent.path().join("s3lab-data");
        std::fs::write(&file_path, b"not a directory").expect("write test file");
        let mut output = Vec::new();

        let error = run_with_writer(
            [
                "s3lab",
                "serve",
                "--data-dir",
                file_path.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
        )
        .await
        .expect_err("file data dir should fail");

        assert!(matches!(error, CliError::Config(_)));
        assert!(error.to_string().contains("not a directory"));
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn serve_rejects_non_loopback_host_before_creating_data_dir() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let mut output = Vec::new();

        let error = run_with_writer_until(
            [
                "s3lab",
                "serve",
                "--host",
                "0.0.0.0",
                "--port",
                "0",
                "--inspector-port",
                "0",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
            async { Ok(()) },
        )
        .await
        .expect_err("wildcard host should fail");

        assert!(matches!(error, CliError::Config(_)));
        assert!(error.to_string().contains("loopback host"));
        assert!(error.to_string().contains("0.0.0.0"));
        assert!(output.is_empty());
        assert!(!data_dir.exists());
    }

    #[tokio::test]
    async fn serve_rejects_non_loopback_inspector_host_before_creating_data_dir() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let mut output = Vec::new();

        let error = run_with_writer_until(
            [
                "s3lab",
                "serve",
                "--port",
                "0",
                "--inspector-host",
                "0.0.0.0",
                "--inspector-port",
                "0",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
            async { Ok(()) },
        )
        .await
        .expect_err("wildcard inspector host should fail");

        assert!(matches!(error, CliError::Config(_)));
        assert!(error.to_string().contains("loopback host"));
        assert!(error.to_string().contains("0.0.0.0"));
        assert!(output.is_empty());
        assert!(!data_dir.exists());
    }

    #[tokio::test]
    async fn run_with_writer_starts_serve_and_reports_bind_conflicts() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind occupied test port");
        let port = listener
            .local_addr()
            .expect("read occupied test port")
            .port()
            .to_string();
        let mut output = Vec::new();

        let error = run_with_writer(
            [
                "s3lab",
                "serve",
                "--port",
                &port,
                "--inspector-port",
                "0",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
        )
        .await
        .expect_err("occupied port should fail");

        assert!(matches!(error, CliError::Server(_)));
        assert!(error.to_string().contains("failed to bind local endpoint"));
        assert!(data_dir.is_dir());
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn run_with_writer_until_runs_through_stdout_path() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let mut output = Vec::new();

        run_with_writer_until(
            [
                "s3lab",
                "serve",
                "--port",
                "0",
                "--inspector-port",
                "0",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
            async { Ok(()) },
        )
        .await
        .expect("serve should run through stdout path");

        assert!(String::from_utf8(output)
            .expect("utf-8 output")
            .contains("S3 endpoint:  http://127.0.0.1:"));
        assert!(data_dir.is_dir());
    }

    #[tokio::test]
    async fn writer_failures_become_output_errors() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let mut writer = FailingWriter;

        let error = run_with_writer_until(
            [
                "s3lab",
                "serve",
                "--port",
                "0",
                "--inspector-port",
                "0",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut writer,
            async { Ok(()) },
        )
        .await
        .expect_err("failing writer should fail command");

        assert!(matches!(error, CliError::Output(_)));
        assert!(error.to_string().contains("failed to write command output"));
        assert_eq!(error.exit_code(), ExitCode::FAILURE);
    }

    #[tokio::test]
    async fn injected_shutdown_signal_errors_are_reported() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let mut output = Vec::new();

        let error = run_with_writer_until(
            [
                "s3lab",
                "serve",
                "--port",
                "0",
                "--inspector-port",
                "0",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
            async {
                Err(CliError::ShutdownSignal(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "signal registration unavailable",
                )))
            },
        )
        .await
        .expect_err("shutdown signal registration should fail command");

        assert!(matches!(error, CliError::ShutdownSignal(_)));
        assert!(error
            .to_string()
            .contains("failed to register Ctrl-C shutdown signal"));
        assert_eq!(error.exit_code(), ExitCode::FAILURE);
    }

    #[test]
    fn parse_help_error_uses_success_exit_code() {
        let error = CliError::Parse(Cli::try_parse_from(["s3lab", "--help"]).unwrap_err());

        assert_eq!(error.exit_code(), ExitCode::SUCCESS);
        assert!(error.source().is_some());
    }

    #[test]
    fn parse_usage_error_uses_failure_exit_code() {
        let error = CliError::Parse(Cli::try_parse_from(["s3lab", "unknown"]).unwrap_err());

        assert_eq!(error.exit_code(), ExitCode::FAILURE);
        assert!(error.source().is_some());
    }

    #[tokio::test]
    async fn config_error_source_is_preserved() {
        let parent = tempfile::tempdir().expect("temp dir");
        let file_path = parent.path().join("s3lab-data");
        std::fs::write(&file_path, b"not a directory").expect("write test file");
        let mut output = Vec::new();

        let error = run_with_writer(
            [
                "s3lab",
                "serve",
                "--data-dir",
                file_path.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
        )
        .await
        .expect_err("file data dir should fail");

        assert_eq!(error.exit_code(), ExitCode::FAILURE);
        assert!(error.source().is_some());
    }

    #[test]
    fn print_handles_parse_and_non_parse_errors() {
        let parse_error = CliError::Parse(Cli::try_parse_from(["s3lab", "--help"]).unwrap_err());
        parse_error.print();

        let output_error = CliError::Output(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "test writer failed",
        ));
        output_error.print();
    }

    #[test]
    fn cli_error_implements_error_trait() {
        fn assert_error_trait(error: &dyn Error) -> String {
            error.to_string()
        }

        let error = CliError::Parse(Cli::try_parse_from(["s3lab", "unknown"]).unwrap_err());

        assert!(assert_error_trait(&error).contains("unrecognized subcommand"));
    }

    fn path_str(path: &Path) -> &str {
        path.to_str().expect("utf-8 temp path")
    }

    fn put_test_object(storage: &FilesystemStorage, bytes: &[u8]) {
        let bucket = BucketName::new("cli-bucket");
        let key = ObjectKey::new("object.txt");

        if !storage
            .bucket_exists(&bucket)
            .expect("bucket lookup succeeds")
        {
            storage.create_bucket(&bucket).expect("create test bucket");
        }

        storage
            .put_object(PutObjectRequest {
                bucket,
                key,
                bytes: bytes.to_vec(),
                content_type: None,
                user_metadata: BTreeMap::new(),
            })
            .expect("put test object");
    }

    fn test_object_bytes(storage: &FilesystemStorage) -> Vec<u8> {
        storage
            .get_object_bytes(
                &BucketName::new("cli-bucket"),
                &ObjectKey::new("object.txt"),
            )
            .expect("read test object")
    }
}
