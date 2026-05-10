// SPDX-License-Identifier: Apache-2.0

use std::process::Command;

#[test]
fn binary_serve_prints_startup_output_and_exits_successfully() {
    let parent = tempfile::tempdir().expect("temp dir");
    let data_dir = parent.path().join("s3lab-data");

    let output = Command::new(env!("CARGO_BIN_EXE_s3lab"))
        .args([
            "serve",
            "--data-dir",
            data_dir.to_str().expect("utf-8 temp path"),
        ])
        .output()
        .expect("run s3lab binary");

    assert!(output.status.success());
    assert!(data_dir.is_dir());

    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("S3 endpoint:  http://127.0.0.1:9000"));
    assert!(stdout.contains(&format!("Data dir:     {}", data_dir.display())));
    assert!(output.stderr.is_empty());
}

#[test]
fn binary_unknown_command_exits_with_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_s3lab"))
        .arg("unknown")
        .output()
        .expect("run s3lab binary");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("unrecognized subcommand"));
}
