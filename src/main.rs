// SPDX-License-Identifier: Apache-2.0

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match s3lab::cli::run(std::env::args()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let exit_code = error.exit_code();
            error.print();
            exit_code
        }
    }
}
