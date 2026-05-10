// SPDX-License-Identifier: Apache-2.0

mod support;

use hyper::body::Bytes;
use hyper::http::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED};
use hyper::http::{Method, Response, StatusCode};
use s3lab::s3::bucket::BucketName;
use s3lab::s3::object::ObjectKey;
use s3lab::server::state::ServerState;
use s3lab::storage::fs::{FilesystemStorage, StorageClock};
use s3lab::storage::STORAGE_ROOT_DIR;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use support::{request, response_bytes, response_text, TestServer, TEST_SUPPORT_MARKER};
use tempfile::TempDir;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

#[tokio::test]
async fn bucket_lifecycle_over_real_http() {
    let server = TestServer::start().await;
    assert_eq!(TEST_SUPPORT_MARKER, "offline-deterministic-tests");
    assert!(server.base_url().starts_with("http://127.0.0.1:"));

    let create = request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
        .await
        .expect("create bucket over HTTP");
    assert_eq!(create.status(), StatusCode::OK);
    assert!(response_bytes(create)
        .await
        .expect("create body")
        .is_empty());

    let list = request(Method::GET, &server.url("/"), Bytes::new(), &[])
        .await
        .expect("list buckets over HTTP");
    assert_eq!(list.status(), StatusCode::OK);
    assert_eq!(
        list.headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/xml")
    );
    assert_eq!(
        response_text(list).await.expect("list body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult><Buckets><Bucket><Name>bucket-a</Name></Bucket></Buckets></ListAllMyBucketsResult>"
    );

    let delete = request(Method::DELETE, &server.url("/bucket-a"), Bytes::new(), &[])
        .await
        .expect("delete bucket over HTTP");
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    assert!(response_bytes(delete)
        .await
        .expect("delete body")
        .is_empty());

    let empty_list = request(Method::GET, &server.url("/"), Bytes::new(), &[])
        .await
        .expect("list empty buckets over HTTP");
    assert_eq!(empty_list.status(), StatusCode::OK);
    assert_eq!(
        response_text(empty_list).await.expect("empty list body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn object_lifecycle_over_real_http() {
    let server = TestServer::start().await;

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );

    let put = request(
        Method::PUT,
        &server.url("/bucket-a/object.txt"),
        Bytes::from_static(b"hello"),
        &[("content-type", "text/plain"), ("x-amz-meta-case", "value")],
    )
    .await
    .expect("put object over HTTP");
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_object_headers(&put);
    assert!(response_bytes(put).await.expect("put body").is_empty());

    let get = request(
        Method::GET,
        &server.url("/bucket-a/object.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("get object over HTTP");
    assert_eq!(get.status(), StatusCode::OK);
    assert_object_headers(&get);
    assert_eq!(
        response_bytes(get).await.expect("get body"),
        Bytes::from_static(b"hello")
    );

    let head = request(
        Method::HEAD,
        &server.url("/bucket-a/object.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("head object over HTTP");
    assert_eq!(head.status(), StatusCode::OK);
    assert_object_headers(&head);
    assert!(response_bytes(head).await.expect("head body").is_empty());

    let delete = request(
        Method::DELETE,
        &server.url("/bucket-a/object.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("delete object over HTTP");
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    assert!(response_bytes(delete)
        .await
        .expect("delete body")
        .is_empty());

    let missing = request(
        Method::GET,
        &server.url("/bucket-a/object.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("get deleted object over HTTP");
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response_text(missing).await.expect("missing object body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Resource>/bucket-a/object.txt</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn put_object_larger_than_axum_default_body_limit_round_trips_over_real_http() {
    let server = TestServer::start().await;

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );

    let object_body = deterministic_bytes((2 * 1024 * 1024) + 1);
    let put = request(
        Method::PUT,
        &server.url("/bucket-a/large-object.bin"),
        object_body.clone(),
        &[("content-type", "application/octet-stream")],
    )
    .await
    .expect("put object larger than Axum default body limit");
    assert_eq!(put.status(), StatusCode::OK);
    assert!(response_bytes(put).await.expect("put body").is_empty());

    let get = request(
        Method::GET,
        &server.url("/bucket-a/large-object.bin"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("get large object over HTTP");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("2097153")
    );
    assert_eq!(
        response_bytes(get).await.expect("large object body"),
        object_body
    );

    server.shutdown().await;
}

#[tokio::test]
async fn list_objects_prefix_over_real_http() {
    let server = TestServer::start().await;

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );

    for (key, body) in [
        ("logs/z.txt", "z"),
        ("images/a.txt", "excluded"),
        ("logs/a.txt", "aa"),
    ] {
        assert_eq!(
            request(
                Method::PUT,
                &server.url(&format!("/bucket-a/{key}")),
                Bytes::from(body.as_bytes().to_vec()),
                &[],
            )
            .await
            .expect("put object")
            .status(),
            StatusCode::OK
        );
    }

    let list = request(
        Method::GET,
        &server.url("/bucket-a?list-type=2&prefix=logs%2F"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list prefixed objects over HTTP");
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = response_text(list).await.expect("list prefix body");
    assert_ordered_contains(
        &list_body,
        &[
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>bucket-a</Name><Prefix>logs/</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
            "<Contents><Key>logs/a.txt</Key><LastModified>",
            "</LastModified><ETag>&quot;4124bc0a9335c27f086f24ba207a4912&quot;</ETag><Size>2</Size><StorageClass>STANDARD</StorageClass></Contents>",
            "<Contents><Key>logs/z.txt</Key><LastModified>",
            "</LastModified><ETag>&quot;fbade9e36a3f36d3d676c1b808451dd7&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>",
        ],
    );

    server.shutdown().await;
}

#[tokio::test]
async fn list_objects_v2_uses_fixed_clock_through_real_http_harness() {
    let temp_dir = TempDir::new().expect("create fixed-clock data dir");
    let data_dir = temp_dir.path().to_path_buf();
    let server = TestServer::start_with_state(
        ServerState::from_storage(FilesystemStorage::with_clock(
            data_dir.clone(),
            FixedClock(fixed_last_modified()),
        )),
        Some(temp_dir),
        Some(data_dir),
    )
    .await;

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );
    for (key, body) in [("z.txt", "z"), ("a.txt", "aa")] {
        assert_eq!(
            request(
                Method::PUT,
                &server.url(&format!("/bucket-a/{key}")),
                Bytes::from(body.as_bytes().to_vec()),
                &[],
            )
            .await
            .expect("put object")
            .status(),
            StatusCode::OK
        );
    }

    let list = request(
        Method::GET,
        &server.url("/bucket-a?list-type=2"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list objects over fixed-clock HTTP harness");
    assert_eq!(list.status(), StatusCode::OK);
    assert_eq!(
        response_text(list).await.expect("list body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>bucket-a</Name><Prefix></Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>a.txt</Key><LastModified>2026-05-10T12:34:56.000Z</LastModified><ETag>&quot;4124bc0a9335c27f086f24ba207a4912&quot;</ETag><Size>2</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>z.txt</Key><LastModified>2026-05-10T12:34:56.000Z</LastModified><ETag>&quot;fbade9e36a3f36d3d676c1b808451dd7&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn corrupted_persisted_response_metadata_returns_internal_error_over_real_http() {
    let server = TestServer::start().await;

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(
            Method::PUT,
            &server.url("/bucket-a/object.txt"),
            Bytes::from_static(b"hello"),
            &[("content-type", "text/plain")],
        )
        .await
        .expect("put object")
        .status(),
        StatusCode::OK
    );
    rewrite_object_metadata_field(
        server.data_dir().expect("server has data dir"),
        &BucketName::new("bucket-a"),
        &ObjectKey::new("object.txt"),
        "content_type",
        serde_json::json!("text/plain\nbad"),
    );

    let response = request(
        Method::GET,
        &server.url("/bucket-a/object.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("get object with corrupted metadata");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response_text(response).await.expect("internal error body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message><Resource>/bucket-a/object.txt</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn server_recreation_preserves_bucket_and_object_state_over_real_http() {
    let data_dir = TempDir::new().expect("create externally owned data dir");
    let first_server = TestServer::start_with_data_dir(data_dir.path().to_path_buf()).await;

    for bucket in ["bucket-b", "bucket-a"] {
        assert_eq!(
            request(
                Method::PUT,
                &first_server.url(&format!("/{bucket}")),
                Bytes::new(),
                &[]
            )
            .await
            .expect("create bucket")
            .status(),
            StatusCode::OK
        );
    }

    let put = request(
        Method::PUT,
        &first_server.url("/bucket-a/prefix/object.txt"),
        Bytes::from_static(b"hello"),
        &[("content-type", "text/plain"), ("x-amz-meta-case", "value")],
    )
    .await
    .expect("put object before recreation");
    assert_eq!(put.status(), StatusCode::OK);
    assert!(response_bytes(put).await.expect("put body").is_empty());

    first_server.shutdown().await;

    let second_server = TestServer::start_with_data_dir(data_dir.path().to_path_buf()).await;

    let list_buckets = request(Method::GET, &second_server.url("/"), Bytes::new(), &[])
        .await
        .expect("list buckets after recreation");
    assert_eq!(list_buckets.status(), StatusCode::OK);
    assert_eq!(
        response_text(list_buckets).await.expect("list buckets body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult><Buckets><Bucket><Name>bucket-a</Name></Bucket><Bucket><Name>bucket-b</Name></Bucket></Buckets></ListAllMyBucketsResult>"
    );

    let head = request(
        Method::HEAD,
        &second_server.url("/bucket-a/prefix/object.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("head object after recreation");
    assert_eq!(head.status(), StatusCode::OK);
    assert_object_headers(&head);
    assert!(response_bytes(head).await.expect("head body").is_empty());

    let get = request(
        Method::GET,
        &second_server.url("/bucket-a/prefix/object.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("get object after recreation");
    assert_eq!(get.status(), StatusCode::OK);
    assert_object_headers(&get);
    assert_eq!(
        response_bytes(get).await.expect("get body"),
        Bytes::from_static(b"hello")
    );

    let list_objects = request(
        Method::GET,
        &second_server.url("/bucket-a?list-type=2&prefix=prefix%2F"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list objects after recreation");
    assert_eq!(list_objects.status(), StatusCode::OK);
    let list_objects_body = response_text(list_objects)
        .await
        .expect("list objects body");
    assert_ordered_contains(
        &list_objects_body,
        &[
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>bucket-a</Name><Prefix>prefix/</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
            "<Contents><Key>prefix/object.txt</Key><LastModified>",
            "</LastModified><ETag>&quot;5d41402abc4b2a76b9719d911017c592&quot;</ETag><Size>5</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>",
        ],
    );

    second_server.shutdown().await;
}

#[tokio::test]
async fn missing_errors_over_real_http() {
    let server = TestServer::start().await;

    let missing_bucket_list = request(
        Method::GET,
        &server.url("/missing-bucket?list-type=2"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list missing bucket over HTTP");
    assert_eq!(missing_bucket_list.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response_text(missing_bucket_list)
            .await
            .expect("missing bucket list body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist.</Message><Resource>/missing-bucket</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );

    let missing_key_get = request(
        Method::GET,
        &server.url("/bucket-a/missing.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("get missing key over HTTP");
    assert_eq!(missing_key_get.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response_text(missing_key_get)
            .await
            .expect("missing key get body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Resource>/bucket-a/missing.txt</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );

    let missing_key_head = request(
        Method::HEAD,
        &server.url("/bucket-a/missing.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("head missing key over HTTP");
    assert_eq!(missing_key_head.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        missing_key_head
            .headers()
            .get("x-amz-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    assert!(response_bytes(missing_key_head)
        .await
        .expect("missing key head body")
        .is_empty());

    let missing_bucket_delete = request(
        Method::DELETE,
        &server.url("/missing-bucket/object.txt"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("delete object from missing bucket over HTTP");
    assert_eq!(missing_bucket_delete.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response_text(missing_bucket_delete)
            .await
            .expect("missing bucket delete body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist.</Message><Resource>/missing-bucket/object.txt</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn unsupported_operation_returns_s3_xml_over_real_http() {
    let server = TestServer::start().await;

    let unsupported = request(Method::GET, &server.url("/bucket-a?acl"), Bytes::new(), &[])
        .await
        .expect("unsupported operation over HTTP");
    assert_eq!(unsupported.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        unsupported
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/xml")
    );
    assert_eq!(
        unsupported
            .headers()
            .get("x-amz-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    assert_eq!(
        response_text(unsupported)
            .await
            .expect("unsupported operation body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NotImplemented</Code><Message>A header you provided implies functionality that is not implemented.</Message><Resource>/bucket-a</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );

    server.shutdown().await;
}

fn assert_object_headers(response: &Response<hyper::body::Incoming>) {
    assert_eq!(
        response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("5")
    );
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/plain")
    );
    assert_eq!(
        response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok()),
        Some("\"5d41402abc4b2a76b9719d911017c592\"")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-case")
            .and_then(|value| value.to_str().ok()),
        Some("value")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("s3lab-test-request-id")
    );
}

fn assert_put_object_headers(response: &Response<hyper::body::Incoming>) {
    assert!(matches!(
        response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        None | Some("0")
    ));
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        None
    );
    assert_eq!(
        response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok()),
        None
    );
    assert_eq!(
        response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok()),
        Some("\"5d41402abc4b2a76b9719d911017c592\"")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-case")
            .and_then(|value| value.to_str().ok()),
        None
    );
}

fn assert_ordered_contains(haystack: &str, needles: &[&str]) {
    let mut search_start = 0;
    for needle in needles {
        let Some(relative_index) = haystack[search_start..].find(needle) else {
            panic!("missing ordered XML fragment: {needle}");
        };
        search_start += relative_index + needle.len();
    }
}

fn deterministic_bytes(len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct FixedClock(OffsetDateTime);

impl StorageClock for FixedClock {
    fn now_utc(&self) -> OffsetDateTime {
        self.0
    }
}

fn fixed_last_modified() -> OffsetDateTime {
    PrimitiveDateTime::new(
        Date::from_calendar_date(2026, Month::May, 10).expect("valid test date"),
        Time::from_hms(12, 34, 56).expect("valid test time"),
    )
    .assume_utc()
}

fn rewrite_object_metadata_field(
    root: &Path,
    bucket: &BucketName,
    key: &ObjectKey,
    field: &str,
    value: serde_json::Value,
) {
    let metadata_path = object_metadata_path(root, bucket, key);
    let mut metadata = serde_json::from_slice::<serde_json::Value>(
        &fs::read(&metadata_path).expect("read object metadata"),
    )
    .expect("parse object metadata");
    metadata
        .as_object_mut()
        .expect("object metadata is a json object")
        .insert(field.to_owned(), value);
    fs::write(
        metadata_path,
        serde_json::to_vec_pretty(&metadata).expect("serialize object metadata"),
    )
    .expect("write object metadata");
}

fn object_metadata_path(root: &Path, bucket: &BucketName, key: &ObjectKey) -> PathBuf {
    root.join(STORAGE_ROOT_DIR)
        .join(encoded_bucket_path_component(bucket))
        .join("objects")
        .join(object_key_shard(key))
        .join(encoded_object_key_path_component(key))
        .join("metadata.json")
}

fn encoded_bucket_path_component(bucket: &BucketName) -> String {
    format!("bucket-{}", sha256_lower_hex(bucket.as_str()))
}

fn encoded_object_key_path_component(key: &ObjectKey) -> String {
    format!("key-{}", sha256_lower_hex(key.as_str()))
}

fn object_key_shard(key: &ObjectKey) -> String {
    sha256_lower_hex(key.as_str())[..2].to_owned()
}

fn sha256_lower_hex(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}
