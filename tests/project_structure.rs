// SPDX-License-Identifier: Apache-2.0

mod support;

use s3lab::config::{RuntimeConfig, DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_PORT};
use s3lab::s3::bucket::BucketName;
use s3lab::s3::object::ObjectKey;
use s3lab::server::state::ServerState;
use s3lab::storage::fs::FilesystemStorage;

#[test]
fn default_runtime_config_matches_phase1_local_defaults() {
    let config = RuntimeConfig::default();

    assert_eq!(config.host, DEFAULT_HOST);
    assert_eq!(config.port, DEFAULT_PORT);
    assert_eq!(config.data_dir, DEFAULT_DATA_DIR);
}

#[test]
fn planned_module_boundaries_are_available_to_tests() {
    let bucket = BucketName::new("example-bucket");
    let object = ObjectKey::new("prefix/example.txt");
    let state = ServerState::new("./s3lab-data");
    let storage = FilesystemStorage::new("./s3lab-data");

    assert_eq!(bucket.as_str(), "example-bucket");
    assert_eq!(object.as_str(), "prefix/example.txt");
    assert_eq!(state.data_dir, storage.root);
    assert_eq!(support::TEST_SUPPORT_MARKER, "offline-deterministic-tests");
}
