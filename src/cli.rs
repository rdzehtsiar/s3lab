// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Eq, PartialEq)]
pub enum CliError {
    UnsupportedCommand(String),
}

impl Display for CliError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedCommand(command) => {
                write!(formatter, "unsupported command: {command}")
            }
        }
    }
}

impl Error for CliError {}

pub fn run<I, S>(args: I) -> Result<(), CliError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut args = args.into_iter().map(Into::into);
    let _program = args.next();

    match args.next().as_deref() {
        None => Ok(()),
        Some("serve") => Ok(()),
        Some(command) => Err(CliError::UnsupportedCommand(command.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::{run, CliError};

    #[test]
    fn accepts_empty_invocation_for_initial_structure() {
        assert_eq!(run(["s3lab"]), Ok(()));
    }

    #[test]
    fn accepts_invocation_without_program_name() {
        let args: [&str; 0] = [];

        assert_eq!(run(args), Ok(()));
    }

    #[test]
    fn accepts_serve_placeholder_command() {
        assert_eq!(run(["s3lab", "serve"]), Ok(()));
    }

    #[test]
    fn ignores_extra_arguments_until_cli_contract_is_implemented() {
        assert_eq!(run(["s3lab", "serve", "--host", "127.0.0.1"]), Ok(()));
    }

    #[test]
    fn rejects_unknown_command_with_actionable_name() {
        assert_eq!(
            run(["s3lab", "unknown"]),
            Err(CliError::UnsupportedCommand("unknown".to_owned()))
        );
    }

    #[test]
    fn unsupported_command_formats_for_display() {
        let error = CliError::UnsupportedCommand("unknown".to_owned());

        assert_eq!(error.to_string(), "unsupported command: unknown");
    }

    #[test]
    fn unsupported_command_implements_error_trait() {
        fn assert_error_trait(error: &dyn std::error::Error) -> String {
            error.to_string()
        }

        let error = CliError::UnsupportedCommand("bad".to_owned());

        assert_eq!(assert_error_trait(&error), "unsupported command: bad");
    }
}
