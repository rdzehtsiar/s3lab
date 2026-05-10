// SPDX-License-Identifier: Apache-2.0

use std::io::{BufRead, BufReader, Read};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);

#[test]
fn binary_serve_prints_startup_output_and_keeps_running() {
    let parent = tempfile::tempdir().expect("temp dir");
    let data_dir = parent.path().join("s3lab-data");

    let mut child = Command::new(env!("CARGO_BIN_EXE_s3lab"))
        .args(["serve", "--port", "0", "--data-dir"])
        .arg(&data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start s3lab binary");

    let stdout = child.stdout.take().expect("child stdout");
    let (stdout_tx, stdout_rx) = mpsc::channel();
    let stdout_reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if stdout_tx.send(line).is_err() {
                break;
            }
        }
    });

    let stdout_lines = wait_for_startup_lines(&stdout_rx, 2);

    let exited_status = child.try_wait().expect("check child status");
    let kept_running = exited_status.is_none();
    if kept_running {
        child.kill().expect("stop s3lab binary");
    }
    let final_status = child.wait().expect("wait for s3lab binary");
    stdout_reader.join().expect("stdout reader joins");

    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("child stderr")
        .read_to_string(&mut stderr)
        .expect("read child stderr");

    assert_eq!(
        stdout_lines.len(),
        2,
        "serve did not print startup output within {STARTUP_TIMEOUT:?}; status: {final_status:?}; data dir: {}; stdout: {stdout_lines:?}; stderr: {stderr}",
        data_dir.display()
    );
    assert!(
        kept_running,
        "serve exited after startup; status: {exited_status:?}; stdout: {stdout_lines:?}; stderr: {stderr}"
    );
    assert!(data_dir.is_dir());
    assert!(stdout_lines[0].starts_with("S3 endpoint:  http://127.0.0.1:"));
    assert_ne!(stdout_lines[0], "S3 endpoint:  http://127.0.0.1:0");
    assert_eq!(
        stdout_lines[1],
        format!("Data dir:     {}", data_dir.display())
    );
    assert!(stderr.is_empty());
}

fn wait_for_startup_lines(
    stdout_rx: &mpsc::Receiver<std::io::Result<String>>,
    expected_line_count: usize,
) -> Vec<String> {
    let mut stdout_lines = Vec::new();
    let deadline = Instant::now() + STARTUP_TIMEOUT;

    while stdout_lines.len() < expected_line_count {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        match stdout_rx.recv_timeout(remaining) {
            Ok(Ok(line)) => stdout_lines.push(line),
            Ok(Err(error)) => panic!("read child stdout: {error}"),
            Err(mpsc::RecvTimeoutError::Timeout) => break,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    stdout_lines
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
