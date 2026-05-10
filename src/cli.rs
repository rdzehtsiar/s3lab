// SPDX-License-Identifier: Apache-2.0

use crate::config::{RuntimeConfig, DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_PORT};
use crate::server::state::ServerState;
use crate::storage::fs::FilesystemStorage;
use clap::{Parser, Subcommand};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Debug)]
pub enum CliError {
    Parse(clap::Error),
    Config(crate::config::ConfigError),
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
}

#[derive(Debug, Parser)]
pub struct ServeArgs {
    /// Host address for the local endpoint.
    #[arg(long, default_value = DEFAULT_HOST)]
    host: String,

    /// Port for the local endpoint.
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Directory used for local S3Lab data.
    #[arg(long, default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,
}

impl From<ServeArgs> for RuntimeConfig {
    fn from(args: ServeArgs) -> Self {
        Self::new(args.host, args.port, args.data_dir)
    }
}

impl Display for CliError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(error) => Display::fmt(error, formatter),
            Self::Config(error) => Display::fmt(error, formatter),
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
            Self::Config(_) | Self::Server(_) | Self::ShutdownSignal(_) | Self::Output(_) => {
                ExitCode::FAILURE
            }
        }
    }

    pub fn print(&self) {
        match self {
            Self::Parse(error) => {
                let _ = error.print();
            }
            Self::Config(_) | Self::Server(_) | Self::ShutdownSignal(_) | Self::Output(_) => {
                eprintln!("{self}");
            }
        }
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse(error) => Some(error),
            Self::Config(error) => Some(error),
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
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

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
    }
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
    config.validate()?;
    config.ensure_data_dir()?;
    let listener = crate::server::bind_listener(&config).await?;
    let endpoint = crate::server::listener_endpoint(&listener)?;

    writeln!(writer, "S3 endpoint:  {endpoint}")?;
    writeln!(writer, "Data dir:     {}", config.data_dir.display())?;
    writer.flush()?;

    let (shutdown_error_tx, shutdown_error_rx) = tokio::sync::oneshot::channel();
    let shutdown = async move {
        if let Err(error) = shutdown.await {
            let _ = shutdown_error_tx.send(error);
        }
    };

    let state = ServerState::from_storage(FilesystemStorage::new(config.data_dir));
    crate::server::serve_listener_until(listener, state, shutdown).await?;

    if let Ok(error) = shutdown_error_rx.await {
        return Err(error);
    }

    Ok(())
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
    }
}

#[cfg(test)]
mod tests {
    use super::{run_with_writer, run_with_writer_until, Cli, CliError};
    use clap::{CommandFactory, Parser};
    use std::error::Error;
    use std::io::Write;
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
        assert!(!output.contains("S3 endpoint:  http://127.0.0.1:0"));
        assert!(output.contains(&format!("Data dir:     {}", data_dir.display())));
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
        assert!(output.contains(&format!("Data dir:     {}", data_dir.display())));
    }

    #[test]
    fn parsed_serve_defaults_match_phase1_contract() {
        let cli = Cli::try_parse_from(["s3lab", "serve"]).expect("valid serve command");
        let super::Command::Serve(args) = cli.command;

        assert_eq!(args.host, "127.0.0.1");
        assert_eq!(args.port, 9000);
        assert_eq!(args.data_dir, PathBuf::from("./s3lab-data"));
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
        assert!(help.contains("--data-dir"));
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
}
