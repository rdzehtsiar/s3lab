// SPDX-License-Identifier: Apache-2.0

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED};
use axum::http::{HeaderValue, Method, Request, StatusCode, Uri};
use s3lab::s3::bucket::BucketName;
use s3lab::s3::error::S3ErrorCode;
use s3lab::s3::object::ObjectKey;
use s3lab::s3::operation::S3Operation;
use s3lab::server::router;
use s3lab::server::routes::resolve_operation;
use s3lab::server::state::ServerState;
use s3lab::storage::{
    BucketSummary, ListObjectsOptions, ObjectListing, PutObjectRequest, Storage, StorageError,
    StoredObject, StoredObjectMetadata,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use time::OffsetDateTime;
use tower::ServiceExt;

#[test]
fn supported_routes_resolve_to_explicit_operations() {
    let cases = [
        (Method::GET, "/", S3Operation::ListBuckets),
        (
            Method::PUT,
            "/example-bucket",
            S3Operation::CreateBucket {
                bucket: "example-bucket".to_owned(),
            },
        ),
        (
            Method::HEAD,
            "/example-bucket",
            S3Operation::HeadBucket {
                bucket: "example-bucket".to_owned(),
            },
        ),
        (
            Method::DELETE,
            "/example-bucket",
            S3Operation::DeleteBucket {
                bucket: "example-bucket".to_owned(),
            },
        ),
        (
            Method::PUT,
            "/example-bucket/object.txt",
            S3Operation::PutObject {
                bucket: "example-bucket".to_owned(),
                key: "object.txt".to_owned(),
            },
        ),
        (
            Method::GET,
            "/example-bucket/object.txt",
            S3Operation::GetObject {
                bucket: "example-bucket".to_owned(),
                key: "object.txt".to_owned(),
            },
        ),
        (
            Method::HEAD,
            "/example-bucket/object.txt",
            S3Operation::HeadObject {
                bucket: "example-bucket".to_owned(),
                key: "object.txt".to_owned(),
            },
        ),
        (
            Method::DELETE,
            "/example-bucket/object.txt",
            S3Operation::DeleteObject {
                bucket: "example-bucket".to_owned(),
                key: "object.txt".to_owned(),
            },
        ),
        (
            Method::GET,
            "/example-bucket?list-type=2",
            S3Operation::ListObjectsV2 {
                bucket: "example-bucket".to_owned(),
                prefix: None,
                continuation_token: None,
                max_keys: 1000,
            },
        ),
        (
            Method::GET,
            "/example-bucket?list-type=2&prefix=images%2F&continuation-token=page%2F2&max-keys=25",
            S3Operation::ListObjectsV2 {
                bucket: "example-bucket".to_owned(),
                prefix: Some("images/".to_owned()),
                continuation_token: Some("page/2".to_owned()),
                max_keys: 25,
            },
        ),
        (
            Method::GET,
            "/example-bucket?list-type=2&max-keys=0",
            S3Operation::ListObjectsV2 {
                bucket: "example-bucket".to_owned(),
                prefix: None,
                continuation_token: None,
                max_keys: 0,
            },
        ),
    ];

    for (method, uri, expected_operation) in cases {
        let route = resolve_operation(&method, &uri.parse::<Uri>().expect("valid URI"))
            .expect("supported route resolves");

        assert_eq!(route.operation, expected_operation);
    }
}

#[test]
fn object_keys_preserve_paths_spaces_encoded_slashes_and_literal_plus() {
    let cases = [
        (
            "/example-bucket/nested/path/object.txt",
            "nested/path/object.txt",
        ),
        (
            "/example-bucket/spaces%20in%20name.txt",
            "spaces in name.txt",
        ),
        ("/example-bucket/encoded%2Fslash.txt", "encoded/slash.txt"),
        ("/example-bucket/literal+plus.txt", "literal+plus.txt"),
    ];

    for (uri, expected_key) in cases {
        let route = resolve_operation(&Method::GET, &uri.parse::<Uri>().expect("valid URI"))
            .expect("object route resolves");

        assert_eq!(
            route.operation,
            S3Operation::GetObject {
                bucket: "example-bucket".to_owned(),
                key: expected_key.to_owned(),
            }
        );
    }
}

#[test]
fn malformed_percent_encoding_is_invalid_argument() {
    for uri in [
        "/example-bucket/bad%ZZ",
        "/example-bucket/%FF",
        "/example-bucket?list-type=2&prefix=bad%ZZ",
    ] {
        let rejection = resolve_operation(&Method::GET, &uri.parse::<Uri>().expect("valid URI"))
            .expect_err("malformed percent encoding is rejected");

        assert_eq!(rejection.code, S3ErrorCode::InvalidArgument);
    }
}

#[test]
fn unsupported_and_malformed_bucket_routes_are_clear_rejections() {
    let empty_bucket =
        resolve_operation(&Method::GET, &Uri::from_static("//key")).expect_err("empty bucket");
    let empty_key =
        resolve_operation(&Method::GET, &Uri::from_static("/bucket/")).expect_err("empty key");
    let method =
        resolve_operation(&Method::POST, &Uri::from_static("/bucket/key")).expect_err("bad method");
    let list_v1 =
        resolve_operation(&Method::GET, &Uri::from_static("/bucket")).expect_err("list v1");
    let bad_list_type = resolve_operation(&Method::GET, &Uri::from_static("/bucket?list-type=1"))
        .expect_err("bad list type");

    assert_eq!(empty_bucket.code, S3ErrorCode::InvalidBucketName);
    assert_eq!(empty_key.code, S3ErrorCode::InvalidArgument);
    assert_eq!(method.code, S3ErrorCode::MethodNotAllowed);
    assert_eq!(list_v1.code, S3ErrorCode::NotImplemented);
    assert_eq!(bad_list_type.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn duplicate_and_unknown_query_params_are_invalid_argument() {
    for uri in [
        "/bucket?list-type=2&list-type=2",
        "/bucket?list-type=2&prefix=a&prefix=b",
        "/bucket?list-type=2&unknown=value",
        "/bucket/key?unknown=value",
    ] {
        let rejection = resolve_operation(&Method::GET, &Uri::from_static(uri))
            .expect_err("query must be rejected");

        assert_eq!(rejection.code, S3ErrorCode::InvalidArgument);
    }
}

#[test]
fn known_s3_subresources_are_not_implemented() {
    for subresource in [
        "acl",
        "tagging",
        "uploads",
        "uploadId",
        "partNumber",
        "versionId",
        "versions",
        "policy",
        "location",
        "cors",
        "website",
        "lifecycle",
        "notification",
        "replication",
        "encryption",
        "retention",
        "legal-hold",
        "object-lock",
    ] {
        let uri = format!("/bucket?{subresource}=value");
        let rejection = resolve_operation(&Method::GET, &uri.parse::<Uri>().expect("valid URI"))
            .expect_err("subresource is unsupported");

        assert_eq!(rejection.code, S3ErrorCode::NotImplemented);
    }
}

#[tokio::test]
async fn unsupported_subresources_return_501_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(ServerState::from_storage(storage));

    let response = app
        .oneshot(request(Method::GET, "/bucket?acl", Body::empty()))
        .await
        .expect("route response");

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(calls.lock().expect("calls lock").as_slice(), &[] as &[&str]);
}

#[tokio::test]
async fn list_objects_v2_continuation_token_calls_storage() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(ServerState::from_storage(storage));

    let response = app
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=2&continuation-token=page-2",
            Body::empty(),
        ))
        .await
        .expect("route response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        calls.lock().expect("calls lock").as_slice(),
        ["list_objects"]
    );
    let body = String::from_utf8(body_bytes(response).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<ListBucketResult>"));
    assert!(body.contains("<IsTruncated>false</IsTruncated>"));
}

#[tokio::test]
async fn list_objects_v2_max_keys_two_returns_paged_xml() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    assert_eq!(
        app.clone()
            .oneshot(request(Method::PUT, "/bucket", Body::empty()))
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );
    for (key, body) in [("z.txt", "z"), ("a.txt", "a"), ("c.txt", "cc")] {
        assert_eq!(
            app.clone()
                .oneshot(request(
                    Method::PUT,
                    &format!("/bucket/{key}"),
                    Body::from(body),
                ))
                .await
                .expect("put object")
                .status(),
            StatusCode::OK
        );
    }

    let first = app
        .clone()
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=2&max-keys=2",
            Body::empty(),
        ))
        .await
        .expect("first list page");
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = String::from_utf8(body_bytes(first).await.to_vec()).expect("utf-8 XML body");
    assert_eq!(
        first_body,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>a.txt</Key><Size>1</Size></Contents><Contents><Key>c.txt</Key><Size>2</Size></Contents><NextContinuationToken>s3lab-v1:7a2e747874</NextContinuationToken></ListBucketResult>"
    );

    let second = app
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=2&max-keys=2&continuation-token=s3lab-v1%3A7a2e747874",
            Body::empty(),
        ))
        .await
        .expect("second list page");
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = String::from_utf8(body_bytes(second).await.to_vec()).expect("utf-8 XML body");
    assert_eq!(
        second_body,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><KeyCount>1</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>z.txt</Key><Size>1</Size></Contents></ListBucketResult>"
    );
}

#[tokio::test]
async fn malformed_list_objects_v2_max_keys_values_are_invalid_argument() {
    for uri in [
        "/bucket?list-type=2&max-keys=",
        "/bucket?list-type=2&max-keys=-1",
        "/bucket?list-type=2&max-keys=%2B1",
        "/bucket?list-type=2&max-keys=%201",
        "/bucket?list-type=2&max-keys=1.5",
        "/bucket?list-type=2&max-keys=abc",
        "/bucket?list-type=2&max-keys=1001",
        "/bucket?list-type=2&max-keys=999999999999999999999999999999",
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(ServerState::from_storage(storage));

        let response = app
            .oneshot(request(Method::GET, uri, Body::empty()))
            .await
            .expect("route response");

        assert_invalid_argument_xml(response, uri).await;
        assert_eq!(calls.lock().expect("calls lock").as_slice(), &[] as &[&str]);
    }
}

#[tokio::test]
async fn list_objects_v2_delimiter_is_invalid_argument() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(ServerState::from_storage(storage));

    let response = app
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=2&delimiter=%2F",
            Body::empty(),
        ))
        .await
        .expect("route response");

    assert_invalid_argument_xml(response, "/bucket?list-type=2&delimiter=%2F").await;
    assert_eq!(calls.lock().expect("calls lock").as_slice(), &[] as &[&str]);
}

#[tokio::test]
async fn route_invalid_argument_error_xml_escapes_query_resource_exactly() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(ServerState::from_storage(storage));

    let response = app
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=1&marker=a%26b%3Ctag%3E",
            Body::empty(),
        ))
        .await
        .expect("route response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/xml")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    let body = String::from_utf8(body_bytes(response).await.to_vec()).expect("utf-8 XML body");
    assert_eq!(
        body,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidArgument</Code><Message>Invalid argument.</Message><Resource>/bucket?list-type=1&amp;marker=a%26b%3Ctag%3E</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );
    assert_eq!(calls.lock().expect("calls lock").as_slice(), &[] as &[&str]);
}

#[tokio::test]
async fn head_success_and_error_responses_have_empty_bodies() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::OK);

    let put = app
        .clone()
        .oneshot(request(
            Method::PUT,
            "/bucket/object.txt",
            Body::from("hello"),
        ))
        .await
        .expect("put response");
    assert_eq!(put.status(), StatusCode::OK);

    let head = app
        .clone()
        .oneshot(request(Method::HEAD, "/bucket/object.txt", Body::empty()))
        .await
        .expect("head response");
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()),
        Some("5")
    );
    assert!(body_bytes(head).await.is_empty());

    let missing = app
        .oneshot(request(Method::HEAD, "/bucket/missing.txt", Body::empty()))
        .await
        .expect("missing head response");
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert!(body_bytes(missing).await.is_empty());
}

#[tokio::test]
async fn head_object_includes_available_metadata_headers() {
    let mut metadata = metadata_with_content_type("bucket", "object.txt", 5, Some("text/plain"));
    metadata.user_metadata = BTreeMap::from([
        ("a-key".to_owned(), "first".to_owned()),
        ("z-key".to_owned(), "last".to_owned()),
    ]);
    let storage = RecordingStorage {
        metadata,
        ..RecordingStorage::default()
    };
    let app = router(ServerState::from_storage(storage));

    let head = app
        .oneshot(request(Method::HEAD, "/bucket/object.txt", Body::empty()))
        .await
        .expect("head response");

    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()),
        Some("5")
    );
    assert_eq!(
        head.headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/plain")
    );
    assert_eq!(
        head.headers().get(ETAG).and_then(|v| v.to_str().ok()),
        Some("\"d41d8cd98f00b204e9800998ecf8427e\"")
    );
    assert_eq!(
        head.headers()
            .get(LAST_MODIFIED)
            .and_then(|v| v.to_str().ok()),
        Some("Thu, 01 Jan 1970 00:00:00 GMT")
    );
    assert_eq!(
        head.headers()
            .get("x-amz-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    assert_eq!(
        head.headers()
            .get("x-amz-meta-z-key")
            .and_then(|v| v.to_str().ok()),
        Some("last")
    );
    assert_eq!(
        head.headers()
            .get("x-amz-meta-a-key")
            .and_then(|v| v.to_str().ok()),
        Some("first")
    );
    assert!(body_bytes(head).await.is_empty());
}

#[tokio::test]
async fn storage_errors_return_s3_xml_with_status_and_request_id() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let response = app
        .oneshot(request(
            Method::GET,
            "/missing-bucket?list-type=2",
            Body::empty(),
        ))
        .await
        .expect("list response");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/xml")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    let body = String::from_utf8(body_bytes(response).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>NoSuchBucket</Code>"));
    assert!(body.contains("<Resource>/missing-bucket</Resource>"));
}

#[tokio::test]
async fn bucket_lifecycle_for_empty_bucket_returns_s3_statuses_and_empty_bodies() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);
    assert!(body_bytes(create).await.is_empty());

    let head = app
        .clone()
        .oneshot(request(Method::HEAD, "/bucket", Body::empty()))
        .await
        .expect("head bucket");
    assert_eq!(head.status(), StatusCode::OK);
    assert!(body_bytes(head).await.is_empty());

    let list = app
        .clone()
        .oneshot(request(Method::GET, "/", Body::empty()))
        .await
        .expect("list buckets");
    assert_eq!(list.status(), StatusCode::OK);
    assert_eq!(
        list.headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/xml")
    );
    let body = String::from_utf8(body_bytes(list).await.to_vec()).expect("utf-8 XML body");
    assert_eq!(
        body,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult><Buckets><Bucket><Name>bucket</Name></Bucket></Buckets></ListAllMyBucketsResult>"
    );

    let delete = app
        .oneshot(request(Method::DELETE, "/bucket", Body::empty()))
        .await
        .expect("delete bucket");
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    assert!(body_bytes(delete).await.is_empty());
}

#[tokio::test]
async fn duplicate_bucket_create_returns_409_bucket_already_owned_by_you() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let duplicate = app
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("duplicate create bucket");
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);
    let body = String::from_utf8(body_bytes(duplicate).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>BucketAlreadyOwnedByYou</Code>"));
    assert!(body.contains("<Resource>/bucket</Resource>"));
}

#[tokio::test]
async fn missing_bucket_head_and_delete_return_expected_errors() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let head = app
        .clone()
        .oneshot(request(Method::HEAD, "/missing-bucket", Body::empty()))
        .await
        .expect("head missing bucket");
    assert_eq!(head.status(), StatusCode::NOT_FOUND);
    assert!(body_bytes(head).await.is_empty());

    let delete = app
        .oneshot(request(Method::DELETE, "/missing-bucket", Body::empty()))
        .await
        .expect("delete missing bucket");
    assert_eq!(delete.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(delete).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>NoSuchBucket</Code>"));
    assert!(body.contains("<Resource>/missing-bucket</Resource>"));
}

#[tokio::test]
async fn delete_non_empty_bucket_returns_409_bucket_not_empty() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let put = app
        .clone()
        .oneshot(request(
            Method::PUT,
            "/bucket/object.txt",
            Body::from("hello"),
        ))
        .await
        .expect("put object");
    assert_eq!(put.status(), StatusCode::OK);

    let delete = app
        .clone()
        .oneshot(request(Method::DELETE, "/bucket", Body::empty()))
        .await
        .expect("delete non-empty bucket");
    assert_eq!(delete.status(), StatusCode::CONFLICT);
    let body = String::from_utf8(body_bytes(delete).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>BucketNotEmpty</Code>"));
    assert!(body.contains("<Resource>/bucket</Resource>"));

    let get = app
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get object after failed bucket delete");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn filesystem_backed_router_supports_basic_bucket_and_object_flow() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);
    assert!(body_bytes(create).await.is_empty());

    let put = app
        .clone()
        .oneshot(request(
            Method::PUT,
            "/bucket/nested%2Fobject.txt",
            Body::from("hello"),
        ))
        .await
        .expect("put object");
    assert_eq!(put.status(), StatusCode::OK);
    assert!(body_bytes(put).await.is_empty());

    let get = app
        .clone()
        .oneshot(request(
            Method::GET,
            "/bucket/nested%2Fobject.txt",
            Body::empty(),
        ))
        .await
        .expect("get object");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, Bytes::from_static(b"hello"));

    let head = app
        .clone()
        .oneshot(request(
            Method::HEAD,
            "/bucket/nested%2Fobject.txt",
            Body::empty(),
        ))
        .await
        .expect("head object");
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()),
        Some("5")
    );
    assert!(body_bytes(head).await.is_empty());

    let delete = app
        .oneshot(request(
            Method::DELETE,
            "/bucket/nested%2Fobject.txt",
            Body::empty(),
        ))
        .await
        .expect("delete object");
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    assert!(body_bytes(delete).await.is_empty());
}

#[tokio::test]
async fn object_put_get_and_head_preserve_metadata_headers() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let put = app
        .clone()
        .oneshot(request_with_headers(
            Method::PUT,
            "/bucket/object.txt",
            &[
                ("content-type", "text/plain"),
                ("x-amz-meta-Z-Key", "last"),
                ("x-amz-meta-a-key", "first"),
            ],
            Body::from("hello"),
        ))
        .await
        .expect("put object");
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_object_metadata_headers(&put, Some("text/plain"));
    assert_eq!(
        put.headers().get(ETAG).and_then(|v| v.to_str().ok()),
        Some("\"5d41402abc4b2a76b9719d911017c592\"")
    );
    assert!(body_bytes(put).await.is_empty());

    let get = app
        .clone()
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get object");
    assert_eq!(get.status(), StatusCode::OK);
    assert_object_metadata_headers(&get, "5", Some("text/plain"));
    assert_eq!(body_bytes(get).await, Bytes::from_static(b"hello"));

    let head = app
        .oneshot(request(Method::HEAD, "/bucket/object.txt", Body::empty()))
        .await
        .expect("head object");
    assert_eq!(head.status(), StatusCode::OK);
    assert_object_metadata_headers(&head, "5", Some("text/plain"));
    assert!(body_bytes(head).await.is_empty());
}

#[tokio::test]
async fn object_lifecycle_put_overwrite_get_head_delete_contract() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let put = app
        .clone()
        .oneshot(request(
            Method::PUT,
            "/bucket/object.bin",
            Body::from(vec![0, 1, 2, 255]),
        ))
        .await
        .expect("put object");
    assert_eq!(put.status(), StatusCode::OK);
    assert!(body_bytes(put).await.is_empty());

    let get = app
        .clone()
        .oneshot(request(Method::GET, "/bucket/object.bin", Body::empty()))
        .await
        .expect("get object");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        body_bytes(get).await,
        Bytes::from_static(b"\x00\x01\x02\xff")
    );

    let head = app
        .clone()
        .oneshot(request(Method::HEAD, "/bucket/object.bin", Body::empty()))
        .await
        .expect("head object");
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("4")
    );
    assert!(body_bytes(head).await.is_empty());

    let overwrite = app
        .clone()
        .oneshot(request(
            Method::PUT,
            "/bucket/object.bin",
            Body::from(vec![9, 8, 7]),
        ))
        .await
        .expect("overwrite object");
    assert_eq!(overwrite.status(), StatusCode::OK);
    assert!(body_bytes(overwrite).await.is_empty());

    let overwritten_get = app
        .clone()
        .oneshot(request(Method::GET, "/bucket/object.bin", Body::empty()))
        .await
        .expect("get overwritten object");
    assert_eq!(overwritten_get.status(), StatusCode::OK);
    assert_eq!(
        body_bytes(overwritten_get).await,
        Bytes::from_static(b"\x09\x08\x07")
    );

    let overwritten_head = app
        .clone()
        .oneshot(request(Method::HEAD, "/bucket/object.bin", Body::empty()))
        .await
        .expect("head overwritten object");
    assert_eq!(overwritten_head.status(), StatusCode::OK);
    assert_eq!(
        overwritten_head
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("3")
    );
    assert!(body_bytes(overwritten_head).await.is_empty());

    let delete = app
        .clone()
        .oneshot(request(Method::DELETE, "/bucket/object.bin", Body::empty()))
        .await
        .expect("delete object");
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    assert!(body_bytes(delete).await.is_empty());

    let missing_get = app
        .clone()
        .oneshot(request(Method::GET, "/bucket/object.bin", Body::empty()))
        .await
        .expect("get deleted object");
    assert_eq!(missing_get.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(missing_get).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>NoSuchKey</Code>"));
    assert!(body.contains("<Resource>/bucket/object.bin</Resource>"));

    let repeated_delete = app
        .oneshot(request(Method::DELETE, "/bucket/object.bin", Body::empty()))
        .await
        .expect("delete already missing object");
    assert_eq!(repeated_delete.status(), StatusCode::NO_CONTENT);
    assert!(body_bytes(repeated_delete).await.is_empty());
}

#[tokio::test]
async fn put_object_rejects_empty_user_metadata_suffix() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let response = app
        .oneshot(request_with_headers(
            Method::PUT,
            "/bucket/object.txt",
            &[("x-amz-meta-", "value")],
            Body::from("hello"),
        ))
        .await
        .expect("put object");

    assert_invalid_argument_xml(response, "/bucket/object.txt").await;
}

#[tokio::test]
async fn put_object_rejects_non_utf8_user_metadata_value() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let request = Request::builder()
        .method(Method::PUT)
        .uri("/bucket/object.txt")
        .header(
            "x-amz-meta-bad",
            HeaderValue::from_bytes(&[0xff]).expect("valid opaque header value"),
        )
        .body(Body::from("hello"))
        .expect("valid request");

    let response = app.oneshot(request).await.expect("put object");

    assert_invalid_argument_xml(response, "/bucket/object.txt").await;
}

#[tokio::test]
async fn put_object_rejects_duplicate_normalized_user_metadata_keys() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let response = app
        .oneshot(request_with_headers(
            Method::PUT,
            "/bucket/object.txt",
            &[("x-amz-meta-Z-Key", "last"), ("x-amz-meta-z-key", "again")],
            Body::from("hello"),
        ))
        .await
        .expect("put object");

    assert_invalid_argument_xml(response, "/bucket/object.txt").await;
}

#[tokio::test]
async fn object_lifecycle_missing_bucket_and_key_errors_are_s3_errors() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let missing_bucket_put = app
        .clone()
        .oneshot(request(
            Method::PUT,
            "/missing-bucket/object.txt",
            Body::from("body"),
        ))
        .await
        .expect("put missing bucket object");
    assert_eq!(missing_bucket_put.status(), StatusCode::NOT_FOUND);
    let body =
        String::from_utf8(body_bytes(missing_bucket_put).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>NoSuchBucket</Code>"));

    let missing_bucket_get = app
        .clone()
        .oneshot(request(
            Method::GET,
            "/missing-bucket/object.txt",
            Body::empty(),
        ))
        .await
        .expect("get missing bucket object");
    assert_eq!(missing_bucket_get.status(), StatusCode::NOT_FOUND);
    let body =
        String::from_utf8(body_bytes(missing_bucket_get).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>NoSuchBucket</Code>"));

    let missing_bucket_head = app
        .clone()
        .oneshot(request(
            Method::HEAD,
            "/missing-bucket/object.txt",
            Body::empty(),
        ))
        .await
        .expect("head missing bucket object");
    assert_eq!(missing_bucket_head.status(), StatusCode::NOT_FOUND);
    assert!(body_bytes(missing_bucket_head).await.is_empty());

    let missing_bucket_delete = app
        .clone()
        .oneshot(request(
            Method::DELETE,
            "/missing-bucket/object.txt",
            Body::empty(),
        ))
        .await
        .expect("delete missing bucket object");
    assert_eq!(missing_bucket_delete.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(missing_bucket_delete).await.to_vec())
        .expect("utf-8 XML body");
    assert!(body.contains("<Code>NoSuchBucket</Code>"));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let missing_key_get = app
        .clone()
        .oneshot(request(Method::GET, "/bucket/missing.txt", Body::empty()))
        .await
        .expect("get missing key");
    assert_eq!(missing_key_get.status(), StatusCode::NOT_FOUND);
    let body =
        String::from_utf8(body_bytes(missing_key_get).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>NoSuchKey</Code>"));

    let missing_key_head = app
        .oneshot(request(Method::HEAD, "/bucket/missing.txt", Body::empty()))
        .await
        .expect("head missing key");
    assert_eq!(missing_key_head.status(), StatusCode::NOT_FOUND);
    assert!(body_bytes(missing_key_head).await.is_empty());
}

#[tokio::test]
async fn delete_bucket_and_object_successes_return_204_no_content() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let put = app
        .clone()
        .oneshot(request(
            Method::PUT,
            "/bucket/object.txt",
            Body::from("hello"),
        ))
        .await
        .expect("put object");
    assert_eq!(put.status(), StatusCode::OK);

    let delete_object = app
        .clone()
        .oneshot(request(Method::DELETE, "/bucket/object.txt", Body::empty()))
        .await
        .expect("delete object");
    assert_eq!(delete_object.status(), StatusCode::NO_CONTENT);
    assert!(body_bytes(delete_object).await.is_empty());

    let delete_bucket = app
        .oneshot(request(Method::DELETE, "/bucket", Body::empty()))
        .await
        .expect("delete bucket");
    assert_eq!(delete_bucket.status(), StatusCode::NO_CONTENT);
    assert!(body_bytes(delete_bucket).await.is_empty());
}

#[tokio::test]
async fn delete_object_is_idempotent_for_existing_bucket_only() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::filesystem(temp_dir.path()));

    let create = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create.status(), StatusCode::OK);

    let missing_object = app
        .clone()
        .oneshot(request(
            Method::DELETE,
            "/bucket/missing.txt",
            Body::empty(),
        ))
        .await
        .expect("delete missing object");
    assert_eq!(missing_object.status(), StatusCode::NO_CONTENT);
    assert!(body_bytes(missing_object).await.is_empty());

    let missing_bucket = app
        .oneshot(request(
            Method::DELETE,
            "/missing-bucket/missing.txt",
            Body::empty(),
        ))
        .await
        .expect("delete object from missing bucket");
    assert_eq!(missing_bucket.status(), StatusCode::NOT_FOUND);
    let body =
        String::from_utf8(body_bytes(missing_bucket).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>NoSuchBucket</Code>"));
}

fn request(method: Method, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .expect("valid request")
}

fn request_with_headers(
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(
            *name,
            HeaderValue::from_str(value).expect("valid test header"),
        );
    }
    builder.body(body).expect("valid request")
}

async fn body_bytes(response: axum::http::Response<Body>) -> Bytes {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body")
}

async fn assert_invalid_argument_xml(response: axum::http::Response<Body>, resource: &str) {
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .headers()
            .get("x-amz-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    let body = String::from_utf8(body_bytes(response).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>InvalidArgument</Code>"));
    assert!(body.contains(&format!("<Resource>{}</Resource>", xml_text(resource))));
}

fn xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn assert_object_metadata_headers(
    response: &axum::http::Response<Body>,
    content_length: &str,
    content_type: Option<&str>,
) {
    assert_eq!(
        response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()),
        Some(content_length)
    );
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        content_type
    );
    assert!(response
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .is_some());
    assert_eq!(
        response
            .headers()
            .get("x-amz-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-z-key")
            .and_then(|v| v.to_str().ok()),
        Some("last")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-a-key")
            .and_then(|v| v.to_str().ok()),
        Some("first")
    );
}

fn assert_put_object_metadata_headers(
    response: &axum::http::Response<Body>,
    content_type: Option<&str>,
) {
    assert!(matches!(
        response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()),
        None | Some("0")
    ));
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        content_type
    );
    assert!(response
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .is_some());
    assert_eq!(
        response
            .headers()
            .get("x-amz-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-z-key")
            .and_then(|v| v.to_str().ok()),
        Some("last")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-a-key")
            .and_then(|v| v.to_str().ok()),
        Some("first")
    );
}

#[derive(Clone)]
struct RecordingStorage {
    calls: Arc<Mutex<Vec<&'static str>>>,
    metadata: StoredObjectMetadata,
    last_put_user_metadata: Arc<Mutex<Option<BTreeMap<String, String>>>>,
}

impl RecordingStorage {
    fn record(&self, call: &'static str) {
        self.calls.lock().expect("calls lock").push(call);
    }
}

impl Storage for RecordingStorage {
    fn create_bucket(&self, _bucket: &BucketName) -> Result<(), StorageError> {
        self.record("create_bucket");
        Ok(())
    }

    fn list_buckets(&self) -> Result<Vec<BucketSummary>, StorageError> {
        self.record("list_buckets");
        Ok(Vec::new())
    }

    fn bucket_exists(&self, _bucket: &BucketName) -> Result<bool, StorageError> {
        self.record("bucket_exists");
        Ok(true)
    }

    fn delete_bucket(&self, _bucket: &BucketName) -> Result<(), StorageError> {
        self.record("delete_bucket");
        Ok(())
    }

    fn put_object(&self, request: PutObjectRequest) -> Result<StoredObjectMetadata, StorageError> {
        self.record("put_object");
        *self
            .last_put_user_metadata
            .lock()
            .expect("last put metadata lock") = Some(request.user_metadata);
        Ok(self.metadata.clone())
    }

    fn get_object(
        &self,
        _bucket: &BucketName,
        _key: &ObjectKey,
    ) -> Result<StoredObject, StorageError> {
        self.record("get_object");
        Ok(StoredObject {
            metadata: self.metadata.clone(),
            bytes: Vec::new(),
        })
    }

    fn get_object_metadata(
        &self,
        _bucket: &BucketName,
        _key: &ObjectKey,
    ) -> Result<StoredObjectMetadata, StorageError> {
        self.record("get_object_metadata");
        Ok(self.metadata.clone())
    }

    fn get_object_bytes(
        &self,
        _bucket: &BucketName,
        _key: &ObjectKey,
    ) -> Result<Vec<u8>, StorageError> {
        self.record("get_object_bytes");
        Ok(Vec::new())
    }

    fn list_objects(
        &self,
        bucket: &BucketName,
        options: ListObjectsOptions,
    ) -> Result<ObjectListing, StorageError> {
        self.record("list_objects");
        Ok(ObjectListing {
            bucket: bucket.clone(),
            objects: Vec::new(),
            max_keys: options.max_keys,
            is_truncated: false,
            next_continuation_token: None,
        })
    }

    fn delete_object(&self, _bucket: &BucketName, _key: &ObjectKey) -> Result<(), StorageError> {
        self.record("delete_object");
        Ok(())
    }
}

fn metadata(bucket: &str, key: &str, content_length: u64) -> StoredObjectMetadata {
    metadata_with_content_type(bucket, key, content_length, None)
}

fn metadata_with_content_type(
    bucket: &str,
    key: &str,
    content_length: u64,
    content_type: Option<&str>,
) -> StoredObjectMetadata {
    StoredObjectMetadata {
        bucket: BucketName::new(bucket),
        key: ObjectKey::new(key),
        etag: "\"d41d8cd98f00b204e9800998ecf8427e\"".to_owned(),
        content_length,
        content_type: content_type.map(str::to_owned),
        last_modified: OffsetDateTime::UNIX_EPOCH,
        user_metadata: BTreeMap::new(),
    }
}

impl Default for RecordingStorage {
    fn default() -> Self {
        Self {
            calls: Arc::default(),
            metadata: metadata("bucket", "key", 0),
            last_put_user_metadata: Arc::default(),
        }
    }
}
