// SPDX-License-Identifier: Apache-2.0

mod support;

use s3lab::config::{RuntimeConfig, DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_PORT};
use s3lab::s3::bucket::BucketName;
use s3lab::s3::error::S3ErrorCode;
use s3lab::s3::object::ObjectKey;
use s3lab::s3::operation::S3Operation;
use s3lab::s3::xml::XML_CONTENT_TYPE;
use s3lab::server::routes::RouteScope;
use s3lab::server::state::ServerState;
use s3lab::server::PHASE1_SERVER_SCOPE;
use s3lab::storage::fs::FilesystemStorage;
use s3lab::storage::key::raw_keys_are_not_filesystem_paths;
use s3lab::storage::{Storage, STORAGE_ROOT_DIR};

#[test]
fn default_runtime_config_matches_phase1_local_defaults() {
    let config = RuntimeConfig::default();

    assert_eq!(config.host, DEFAULT_HOST);
    assert_eq!(config.port, DEFAULT_PORT);
    assert_eq!(config.data_dir, std::path::PathBuf::from(DEFAULT_DATA_DIR));
}

#[test]
fn planned_module_boundaries_are_available_to_tests() {
    let bucket = BucketName::new("example-bucket");
    let object = ObjectKey::new("prefix/example.txt");
    let state = ServerState::new("./s3lab-data");
    let storage = FilesystemStorage::new("./s3lab-data");

    assert_eq!(bucket.as_str(), "example-bucket");
    assert_eq!(object.as_str(), "prefix/example.txt");
    assert_eq!(
        state
            .storage()
            .list_buckets()
            .expect("server state exposes storage"),
        storage
            .list_buckets()
            .expect("filesystem storage lists buckets")
    );
    assert_eq!(support::TEST_SUPPORT_MARKER, "offline-deterministic-tests");
}

#[test]
fn placeholder_constants_match_phase1_structure_contract() {
    assert_eq!(PHASE1_SERVER_SCOPE, "path-style-local-s3");
    assert_eq!(STORAGE_ROOT_DIR, "buckets");
    assert_eq!(XML_CONTENT_TYPE, "application/xml");
    assert!(raw_keys_are_not_filesystem_paths());
}

#[test]
fn placeholder_enums_expose_expected_initial_variants() {
    assert_eq!(RouteScope::PathStyle, RouteScope::PathStyle);
    assert_eq!(S3Operation::ListBuckets, S3Operation::ListBuckets);
    assert_eq!(S3ErrorCode::NotImplemented, S3ErrorCode::NotImplemented);
}
