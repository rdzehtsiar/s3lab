// SPDX-License-Identifier: Apache-2.0

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

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
    assert_no_child_failure_output(&stderr);
}

#[test]
fn binary_serve_answers_http_requests_after_startup() {
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
    let endpoint = parse_endpoint(
        stdout_lines
            .first()
            .expect("serve prints endpoint before accepting requests"),
    );

    let create_response = http_request(
        &endpoint,
        "PUT /binary-smoke-bucket HTTP/1.1\r\nHost: {host}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    assert!(
        create_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected create-bucket response: {create_response}"
    );

    let list_response = http_request(
        &endpoint,
        "GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n",
    );
    assert!(
        list_response.starts_with("HTTP/1.1 200 OK"),
        "unexpected list-buckets response: {list_response}"
    );
    assert!(
        list_response.contains("<Name>binary-smoke-bucket</Name>"),
        "created bucket missing from list response: {list_response}"
    );
    assert_no_extra_stdout_lines(&stdout_rx);

    child.kill().expect("stop s3lab binary");
    let final_status = child.wait().expect("wait for s3lab binary");
    stdout_reader.join().expect("stdout reader joins");

    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("child stderr")
        .read_to_string(&mut stderr)
        .expect("read child stderr");

    assert!(
        !final_status.success(),
        "test stops the long-running server process explicitly"
    );
    assert_no_child_failure_output(&stderr);
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

fn parse_endpoint(line: &str) -> String {
    line.strip_prefix("S3 endpoint:  ")
        .expect("startup line contains endpoint")
        .to_owned()
}

fn http_request(endpoint: &str, request_template: &str) -> String {
    let host = endpoint
        .strip_prefix("http://")
        .expect("endpoint uses http scheme");
    let request = request_template.replace("{host}", host);
    let mut stream = TcpStream::connect(host).expect("connect to s3lab endpoint");
    stream
        .set_read_timeout(Some(HTTP_TIMEOUT))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(HTTP_TIMEOUT))
        .expect("set write timeout");
    stream
        .write_all(request.as_bytes())
        .expect("write HTTP request");

    let mut response = Vec::new();
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&buffer[..read]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                panic!(
                    "timed out waiting for HTTP response; partial response: {}",
                    String::from_utf8_lossy(&response)
                );
            }
            Err(error) => panic!("read HTTP response: {error}"),
        }
    }

    String::from_utf8(response).expect("HTTP response is utf-8")
}

fn assert_no_child_failure_output(stderr: &str) {
    assert!(
        !stderr.contains("panicked") && !stderr.contains("ERROR"),
        "child stderr contains failure output: {stderr}"
    );
}

fn assert_no_extra_stdout_lines(stdout_rx: &mpsc::Receiver<std::io::Result<String>>) {
    match stdout_rx.recv_timeout(Duration::from_millis(200)) {
        Err(mpsc::RecvTimeoutError::Timeout) => {}
        Err(mpsc::RecvTimeoutError::Disconnected) => {}
        Ok(Ok(line)) => panic!("serve wrote unexpected request-time output to stdout: {line}"),
        Ok(Err(error)) => panic!("read child stdout: {error}"),
    }
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
