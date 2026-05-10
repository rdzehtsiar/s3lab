// SPDX-License-Identifier: Apache-2.0

use crate::config::{RuntimeConfig, DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_PORT};
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
            Self::Output(error) => write!(formatter, "failed to write command output: {error}"),
        }
    }
}

impl CliError {
    pub fn exit_code(&self) -> ExitCode {
        match self {
            Self::Parse(error) if error.use_stderr() => ExitCode::FAILURE,
            Self::Parse(_) => ExitCode::SUCCESS,
            Self::Config(_) | Self::Output(_) => ExitCode::FAILURE,
        }
    }

    pub fn print(&self) {
        match self {
            Self::Parse(error) => {
                let _ = error.print();
            }
            Self::Config(_) | Self::Output(_) => {
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

impl From<std::io::Error> for CliError {
    fn from(error: std::io::Error) -> Self {
        Self::Output(error)
    }
}

pub fn run<I, S>(args: I) -> Result<(), CliError>
where
    I: IntoIterator<Item = S>,
    S: Into<std::ffi::OsString> + Clone,
{
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    run_with_writer(args, &mut stdout)
}

pub fn run_with_writer<I, S, W>(args: I, writer: &mut W) -> Result<(), CliError>
where
    I: IntoIterator<Item = S>,
    S: Into<std::ffi::OsString> + Clone,
    W: Write,
{
    let cli = Cli::try_parse_from(args)?;

    match cli.command {
        Command::Serve(args) => run_serve(args.into(), writer),
    }
}

fn run_serve<W>(config: RuntimeConfig, writer: &mut W) -> Result<(), CliError>
where
    W: Write,
{
    config.ensure_data_dir()?;

    writeln!(writer, "S3 endpoint:  {}", config.endpoint())?;
    writeln!(writer, "Data dir:     {}", config.data_dir.display())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{run_with_writer, Cli, CliError};
    use clap::{CommandFactory, Parser};
    use std::path::PathBuf;

    #[test]
    fn default_serve_config_prints_local_startup_output() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("s3lab-data");
        let mut output = Vec::new();

        run_with_writer(
            [
                "s3lab",
                "serve",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
        )
        .expect("serve should validate config");

        let output = String::from_utf8(output).expect("utf-8 output");
        assert!(output.contains("S3 endpoint:  http://127.0.0.1:9000"));
        assert!(output.contains(&format!("Data dir:     {}", data_dir.display())));
        assert!(data_dir.is_dir());
    }

    #[test]
    fn explicit_serve_options_override_defaults() {
        let parent = tempfile::tempdir().expect("temp dir");
        let data_dir = parent.path().join("custom-data");
        let mut output = Vec::new();

        run_with_writer(
            [
                "s3lab",
                "serve",
                "--host",
                "0.0.0.0",
                "--port",
                "4567",
                "--data-dir",
                data_dir.to_str().expect("utf-8 temp path"),
            ],
            &mut output,
        )
        .expect("serve should validate config");

        let output = String::from_utf8(output).expect("utf-8 output");
        assert!(output.contains("S3 endpoint:  http://0.0.0.0:4567"));
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

    #[test]
    fn serve_rejects_file_as_data_dir() {
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
        .expect_err("file data dir should fail");

        assert!(matches!(error, CliError::Config(_)));
        assert!(error.to_string().contains("not a directory"));
        assert!(output.is_empty());
    }

    #[test]
    fn cli_error_implements_error_trait() {
        fn assert_error_trait(error: &dyn std::error::Error) -> String {
            error.to_string()
        }

        let error = CliError::Parse(Cli::try_parse_from(["s3lab", "unknown"]).unwrap_err());

        assert!(assert_error_trait(&error).contains("unrecognized subcommand"));
    }
}
