// SPDX-License-Identifier: Apache-2.0

mod support;

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED};
use axum::http::{HeaderValue, Method, Request, StatusCode, Uri};
use s3lab::s3::bucket::BucketName;
use s3lab::s3::error::{S3ErrorCode, STATIC_REQUEST_ID};
use s3lab::s3::object::ObjectKey;
use s3lab::s3::operation::{ListObjectsEncoding, S3Operation};
use s3lab::s3::sigv4::{build_canonical_request, build_string_to_sign, parse_authorization_header};
use s3lab::server::router;
use s3lab::server::routes::{resolve_operation, PHASE1_MAX_PUT_OBJECT_BODY_BYTES};
use s3lab::server::state::{FixedRequestIdGenerator, ServerState};
use s3lab::storage::fs::{FilesystemStorage, StorageClock};
use s3lab::storage::{
    BucketSummary, ListObjectsOptions, ObjectListing, PutObjectRequest, Storage, StorageError,
    StoredObject, StoredObjectMetadata,
};
use s3lab::trace::{
    AuthDecision, AuthDecisionTrace, CanonicalRequestBuiltTrace, RequestReceivedTrace,
    ResponseSentTrace, RouteResolvedTrace, SigV4ParsedTrace, StorageMutation,
    StorageMutationOutcome, StorageMutationTrace, TraceEvent, TraceS3Operation, TraceSink,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use support::test_server_state;
use tempfile::TempDir;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};
use tower::ServiceExt;

#[test]
fn supported_routes_resolve_to_explicit_operations() {
    let cases = [
        (Method::GET, "/", S3Operation::ListBuckets),
        (
            Method::PUT,
            "/example-bucket",
            S3Operation::CreateBucket {
                bucket: BucketName::new("example-bucket"),
            },
        ),
        (
            Method::HEAD,
            "/example-bucket",
            S3Operation::HeadBucket {
                bucket: BucketName::new("example-bucket"),
            },
        ),
        (
            Method::DELETE,
            "/example-bucket",
            S3Operation::DeleteBucket {
                bucket: BucketName::new("example-bucket"),
            },
        ),
        (
            Method::PUT,
            "/example-bucket/object.txt",
            S3Operation::PutObject {
                bucket: BucketName::new("example-bucket"),
                key: ObjectKey::new("object.txt"),
            },
        ),
        (
            Method::GET,
            "/example-bucket/object.txt",
            S3Operation::GetObject {
                bucket: BucketName::new("example-bucket"),
                key: ObjectKey::new("object.txt"),
            },
        ),
        (
            Method::HEAD,
            "/example-bucket/object.txt",
            S3Operation::HeadObject {
                bucket: BucketName::new("example-bucket"),
                key: ObjectKey::new("object.txt"),
            },
        ),
        (
            Method::DELETE,
            "/example-bucket/object.txt",
            S3Operation::DeleteObject {
                bucket: BucketName::new("example-bucket"),
                key: ObjectKey::new("object.txt"),
            },
        ),
        (
            Method::GET,
            "/example-bucket?list-type=2",
            S3Operation::ListObjectsV2 {
                bucket: BucketName::new("example-bucket"),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                encoding: None,
            },
        ),
        (
            Method::GET,
            "/example-bucket?list-type=2&encoding-type=url",
            S3Operation::ListObjectsV2 {
                bucket: BucketName::new("example-bucket"),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                encoding: Some(ListObjectsEncoding::Url),
            },
        ),
        (
            Method::GET,
            "/example-bucket?list-type=2&prefix=images%2F&continuation-token=page%2F2&max-keys=25",
            S3Operation::ListObjectsV2 {
                bucket: BucketName::new("example-bucket"),
                prefix: Some(ObjectKey::new("images/")),
                delimiter: None,
                continuation_token: Some("page/2".to_owned()),
                max_keys: 25,
                encoding: None,
            },
        ),
        (
            Method::GET,
            "/example-bucket?list-type=2&max-keys=0",
            S3Operation::ListObjectsV2 {
                bucket: BucketName::new("example-bucket"),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 0,
                encoding: None,
            },
        ),
        (
            Method::GET,
            "/example-bucket?list-type=2&delimiter=%2F",
            S3Operation::ListObjectsV2 {
                bucket: BucketName::new("example-bucket"),
                prefix: None,
                delimiter: Some("/".to_owned()),
                continuation_token: None,
                max_keys: 1000,
                encoding: None,
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
                bucket: BucketName::new("example-bucket"),
                key: ObjectKey::new(expected_key),
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
fn invalid_bucket_names_are_rejected_before_route_resolution() {
    for uri in [
        "/ab",
        "/Uppercase",
        "/bad_bucket",
        "/bucket..name",
        "/bucket.-name",
        "/bucket-.name",
        "/192.168.0.1",
        "/bucket%2Fname",
        "/xn--bucket",
        "/sthree-bucket",
        "/amzn-s3-demo-bucket",
        "/bucket-s3alias",
        "/bucket--ol-s3",
        "/bucket.mrap",
        "/bucket--x-s3",
        "/bucket--table-s3",
    ] {
        let rejection = resolve_operation(&Method::PUT, &Uri::from_static(uri))
            .expect_err("invalid bucket name is rejected");

        assert_eq!(rejection.code, S3ErrorCode::InvalidBucketName);
    }
}

#[test]
fn object_key_routes_accept_utf8_byte_limit_and_reject_longer_keys() {
    let valid_key = "nested/".to_owned() + &"a".repeat(1024 - "nested/".len());
    let valid_uri = format!("/example-bucket/{valid_key}");
    let route = resolve_operation(&Method::PUT, &valid_uri.parse::<Uri>().expect("valid URI"))
        .expect("key at byte limit resolves");

    assert_eq!(
        route.operation,
        S3Operation::PutObject {
            bucket: BucketName::new("example-bucket"),
            key: ObjectKey::new(valid_key),
        }
    );

    let too_long_uri = format!("/example-bucket/{}", "a".repeat(1025));
    let rejection = resolve_operation(
        &Method::PUT,
        &too_long_uri.parse::<Uri>().expect("valid URI"),
    )
    .expect_err("oversized key is rejected");

    assert_eq!(rejection.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn object_key_routes_reject_xml_invalid_control_characters() {
    for uri in [
        "/example-bucket/prefix/%00object.txt",
        "/example-bucket/prefix/%1Fobject.txt",
    ] {
        let rejection = resolve_operation(&Method::PUT, &uri.parse::<Uri>().expect("valid URI"))
            .expect_err("XML-invalid object key is rejected");

        assert_eq!(rejection.code, S3ErrorCode::InvalidArgument);
    }
}

#[test]
fn object_key_routes_accept_carriage_return() {
    let route = resolve_operation(
        &Method::PUT,
        &"/example-bucket/prefix/%0Dobject.txt"
            .parse::<Uri>()
            .expect("valid URI"),
    )
    .expect("CR object key resolves");

    assert_eq!(
        route.operation,
        S3Operation::PutObject {
            bucket: BucketName::new("example-bucket"),
            key: ObjectKey::new("prefix/\robject.txt"),
        }
    );
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
fn x_id_query_param_is_ignored_when_resolving_routes() {
    let cases = [
        (
            Method::GET,
            "/bucket?list-type=2&x-id=ListObjectsV2",
            S3Operation::ListObjectsV2 {
                bucket: BucketName::new("bucket"),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                encoding: None,
            },
        ),
        (
            Method::GET,
            "/bucket/object.txt?x-id=GetObject",
            S3Operation::GetObject {
                bucket: BucketName::new("bucket"),
                key: ObjectKey::new("object.txt"),
            },
        ),
        (
            Method::PUT,
            "/bucket/object.txt?x-id=PutObject&x-id=Retry",
            S3Operation::PutObject {
                bucket: BucketName::new("bucket"),
                key: ObjectKey::new("object.txt"),
            },
        ),
    ];

    for (method, uri, expected_operation) in cases {
        let route = resolve_operation(&method, &uri.parse::<Uri>().expect("valid URI"))
            .expect("x-id should not affect route resolution");

        assert_eq!(route.operation, expected_operation);
    }
}

#[test]
fn empty_query_pairs_are_skipped_when_resolving_routes() {
    let route = resolve_operation(
        &Method::GET,
        &Uri::from_static("/bucket?&&list-type=2&&prefix=logs%2F&&"),
    )
    .expect("empty query pairs should be skipped");

    assert_eq!(
        route.operation,
        S3Operation::ListObjectsV2 {
            bucket: BucketName::new("bucket"),
            prefix: Some(ObjectKey::new("logs/")),
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            encoding: None,
        }
    );
}

#[test]
fn known_s3_subresources_are_not_implemented() {
    for subresource in [
        "acl",
        "delete",
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

#[test]
fn list_objects_v2_prefix_rejects_xml_invalid_control_characters() {
    for uri in [
        "/example-bucket?list-type=2&prefix=logs%00",
        "/example-bucket?list-type=2&prefix=logs%1F",
    ] {
        let rejection = resolve_operation(&Method::GET, &Uri::from_static(uri))
            .expect_err("XML-invalid prefix is rejected");

        assert_eq!(rejection.code, S3ErrorCode::InvalidArgument);
    }
}

#[test]
fn list_objects_v2_prefix_accepts_carriage_return() {
    let route = resolve_operation(
        &Method::GET,
        &Uri::from_static("/example-bucket?list-type=2&prefix=logs%0D"),
    )
    .expect("CR prefix resolves");

    assert_eq!(
        route.operation,
        S3Operation::ListObjectsV2 {
            bucket: BucketName::new("example-bucket"),
            prefix: Some(ObjectKey::new("logs\r")),
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            encoding: None,
        }
    );
}

#[tokio::test]
async fn invalid_bucket_names_return_invalid_bucket_name_without_storage_call() {
    for (method, uri) in [
        (Method::PUT, "/BadBucket"),
        (Method::HEAD, "/bad_bucket"),
        (Method::GET, "/bucket..name?list-type=2"),
        (Method::DELETE, "/bucket-.name"),
        (Method::PUT, "/ab/object.txt"),
        (Method::GET, "/192.168.0.1/object.txt"),
        (Method::HEAD, "/bucket%2Fname/object.txt"),
        (Method::DELETE, "/bucket.-name/object.txt"),
        (Method::PUT, "/xn--bucket/object.txt"),
        (Method::GET, "/sthree-bucket/object.txt"),
        (Method::HEAD, "/amzn-s3-demo-bucket/object.txt"),
        (Method::DELETE, "/bucket--table-s3/object.txt"),
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(method.clone(), uri, Body::empty()))
            .await
            .expect("route response");

        if method == Method::HEAD {
            assert_head_error(response, StatusCode::BAD_REQUEST).await;
        } else {
            assert_s3_error_xml(
                response,
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                path_only(uri),
            )
            .await;
        }
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn oversized_object_key_routes_return_invalid_argument_without_storage_call() {
    for method in [Method::PUT, Method::GET, Method::HEAD, Method::DELETE] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));
        let uri = format!("/bucket/{}", "a".repeat(1025));

        let response = app
            .oneshot(request(method.clone(), &uri, Body::from("body")))
            .await
            .expect("route response");

        if method == Method::HEAD {
            assert_head_error(response, StatusCode::BAD_REQUEST).await;
        } else {
            assert_invalid_argument_xml(response, &uri).await;
        }
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn xml_invalid_object_key_routes_return_invalid_argument_without_storage_call() {
    for method in [Method::PUT, Method::GET, Method::HEAD, Method::DELETE] {
        for uri in [
            "/bucket/prefix/%00object.txt",
            "/bucket/prefix/%1Fobject.txt",
        ] {
            let storage = RecordingStorage::default();
            let calls = Arc::clone(&storage.calls);
            let app = router(test_server_state(storage));

            let response = app
                .oneshot(request(method.clone(), uri, Body::from("body")))
                .await
                .expect("route response");

            if method == Method::HEAD {
                assert_head_error(response, StatusCode::BAD_REQUEST).await;
            } else {
                assert_invalid_argument_xml(response, uri).await;
            }
            assert_no_storage_calls(&calls);
        }
    }
}

#[tokio::test]
async fn method_not_allowed_route_errors_return_s3_xml_without_storage_call() {
    for (method, uri) in [
        (Method::POST, "/"),
        (Method::POST, "/bucket"),
        (Method::PATCH, "/bucket/key"),
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(method, uri, Body::empty()))
            .await
            .expect("route response");

        assert_s3_error_xml(
            response,
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            uri,
        )
        .await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn unsupported_subresources_return_501_without_storage_call() {
    for (method, uri) in [
        (Method::GET, "/bucket?acl"),
        (Method::POST, "/bucket?delete"),
        (Method::PUT, "/bucket/key?tagging"),
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(method, uri, Body::empty()))
            .await
            .expect("route response");

        assert_s3_error_xml(
            response,
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            path_only(uri),
        )
        .await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn head_route_errors_have_request_id_and_no_body() {
    for (uri, status) in [
        ("/bucket?acl", StatusCode::NOT_IMPLEMENTED),
        ("/bucket?unknown=value", StatusCode::BAD_REQUEST),
        ("/bucket/key?unknown=value", StatusCode::BAD_REQUEST),
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(Method::HEAD, uri, Body::empty()))
            .await
            .expect("route response");

        assert_head_error(response, status).await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn unsigned_requests_remain_compatible_and_can_mutate_storage() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

    let response = app
        .oneshot(request(
            Method::PUT,
            "/bucket/object.txt",
            Body::from("hello"),
        ))
        .await
        .expect("unsigned put object");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(calls.lock().expect("calls lock").as_slice(), ["put_object"]);
}

#[tokio::test]
async fn valid_signed_request_is_verified_before_storage_mutation() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));
    let body = b"hello";
    let headers = signed_request_headers(Method::PUT, "/bucket/object.txt", body);

    let response = app
        .oneshot(request_with_owned_headers(
            Method::PUT,
            "/bucket/object.txt",
            &headers,
            Body::from(body.as_slice().to_vec()),
        ))
        .await
        .expect("signed put object");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(calls.lock().expect("calls lock").as_slice(), ["put_object"]);
}

#[tokio::test]
async fn signed_put_rejects_literal_payload_hash_mismatch_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));
    let signed_body = b"signed-body";
    let sent_body = b"sent-secret-body";
    let headers = signed_request_headers(Method::PUT, "/bucket/object.txt", signed_body);

    let response = app
        .oneshot(request_with_owned_headers(
            Method::PUT,
            "/bucket/object.txt",
            &headers,
            Body::from(sent_body.as_slice().to_vec()),
        ))
        .await
        .expect("payload hash mismatch response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/xml")
    );
    let body = String::from_utf8(body_bytes(response).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>XAmzContentSHA256Mismatch</Code>"));
    assert!(body.contains("<Resource>/bucket/object.txt</Resource>"));
    assert!(!body.contains("signed-body"));
    assert!(!body.contains("sent-secret-body"));
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn signed_put_rejects_missing_payload_hash_with_body_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));
    let sent_body = b"sent-secret-body";
    let headers = signed_request_headers_without_payload_hash(Method::PUT, "/bucket/object.txt");

    let response = app
        .oneshot(request_with_owned_headers(
            Method::PUT,
            "/bucket/object.txt",
            &headers,
            Body::from(sent_body.as_slice().to_vec()),
        ))
        .await
        .expect("missing payload hash response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/xml")
    );
    let body = String::from_utf8(body_bytes(response).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>XAmzContentSHA256Mismatch</Code>"));
    assert!(body.contains("<Resource>/bucket/object.txt</Resource>"));
    assert!(!body.contains("sent-secret-body"));
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn malformed_authorization_returns_s3_error_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

    let response = app
        .oneshot(request_with_headers(
            Method::PUT,
            "/bucket/object.txt",
            &[("authorization", "Bearer secret-value")],
            Body::from("hello"),
        ))
        .await
        .expect("malformed auth response");

    assert_s3_error_xml(
        response,
        StatusCode::BAD_REQUEST,
        "AuthorizationHeaderMalformed",
        "/bucket/object.txt",
    )
    .await;
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn head_authorization_errors_preserve_empty_body_behavior() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

    let response = app
        .oneshot(request_with_headers(
            Method::HEAD,
            "/bucket/object.txt",
            &[("authorization", "Bearer secret-value")],
            Body::empty(),
        ))
        .await
        .expect("head malformed auth response");

    assert_head_error(response, StatusCode::BAD_REQUEST).await;
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn wrong_access_key_returns_s3_error_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

    let response = app
        .oneshot(request_with_headers(
            Method::PUT,
            "/bucket/object.txt",
            &[(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=wrong-key/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000",
            )],
            Body::from("hello"),
        ))
        .await
        .expect("wrong access key response");

    assert_s3_error_xml(
        response,
        StatusCode::FORBIDDEN,
        "InvalidAccessKeyId",
        "/bucket/object.txt",
    )
    .await;
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn missing_signed_header_returns_s3_error_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

    let response = app
        .oneshot(request_with_headers(
            Method::PUT,
            "/bucket/object.txt",
            &[
                ("x-amz-date", "20260512T010203Z"),
                (
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=s3lab/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000",
                ),
            ],
            Body::from("hello"),
        ))
        .await
        .expect("missing signed header response");

    assert_s3_error_xml(
        response,
        StatusCode::FORBIDDEN,
        "AccessDenied",
        "/bucket/object.txt",
    )
    .await;
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn signature_mismatch_returns_s3_error_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));
    let body = b"hello";
    let mut headers = signed_request_headers(Method::PUT, "/bucket/object.txt", body);
    replace_authorization_signature(
        &mut headers,
        "0000000000000000000000000000000000000000000000000000000000000000",
    );

    let response = app
        .oneshot(request_with_owned_headers(
            Method::PUT,
            "/bucket/object.txt",
            &headers,
            Body::from(body.as_slice().to_vec()),
        ))
        .await
        .expect("signature mismatch response");

    assert_s3_error_xml(
        response,
        StatusCode::FORBIDDEN,
        "SignatureDoesNotMatch",
        "/bucket/object.txt",
    )
    .await;
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn signed_request_trace_records_safe_auth_events() {
    let storage = RecordingStorage::default();
    let sink = TestTraceSink::default();
    let recorded = sink.clone();
    let state = ServerState::with_request_id_generator_and_trace_sink(
        storage,
        FixedRequestIdGenerator::new(STATIC_REQUEST_ID),
        sink,
    );
    let app = router(state);
    let body = b"hello";
    let headers = signed_request_headers(Method::PUT, "/bucket/object.txt", body);
    let authorization = header_value(&headers, "authorization").to_owned();

    let response = app
        .oneshot(request_with_owned_headers(
            Method::PUT,
            "/bucket/object.txt",
            &headers,
            Body::from(body.as_slice().to_vec()),
        ))
        .await
        .expect("signed put object");

    assert_eq!(response.status(), StatusCode::OK);
    let events = recorded.events();
    assert!(
        events.contains(&TraceEvent::RequestReceived(RequestReceivedTrace::new(
            STATIC_REQUEST_ID,
            "PUT",
            "/bucket/object.txt",
            [
                "authorization",
                "host",
                "x-amz-content-sha256",
                "x-amz-date"
            ],
        )))
    );
    assert!(
        events.contains(&TraceEvent::RouteResolved(RouteResolvedTrace::new(
            STATIC_REQUEST_ID,
            "PUT",
            "/bucket/object.txt",
            TraceS3Operation::PutObject,
        )))
    );
    assert!(events
        .iter()
        .any(|event| matches!(event, TraceEvent::SigV4Parsed(SigV4ParsedTrace { .. }))));
    assert!(events.iter().any(|event| matches!(
        event,
        TraceEvent::CanonicalRequestBuilt(CanonicalRequestBuiltTrace { .. })
    )));
    assert!(
        events.contains(&TraceEvent::AuthDecision(AuthDecisionTrace::new(
            STATIC_REQUEST_ID,
            AuthDecision::Accepted,
        )))
    );
    assert!(
        events.contains(&TraceEvent::StorageMutation(StorageMutationTrace::new(
            STATIC_REQUEST_ID,
            StorageMutation::PutObject,
            Some("bucket"),
            Some("object.txt"),
            StorageMutationOutcome::Applied,
        )))
    );
    assert!(
        events.contains(&TraceEvent::ResponseSent(ResponseSentTrace::new(
            STATIC_REQUEST_ID,
            200,
            None::<String>,
        )))
    );

    let debug = format!("{events:?}");
    assert!(!debug.contains(&authorization));
    assert!(!debug.contains(signature_from_authorization(&authorization)));
    assert!(!debug.contains("s3lab-secret"));
}

#[tokio::test]
async fn bucket_list_query_without_list_type_remains_not_implemented() {
    for uri in ["/bucket?prefix=a", "/bucket?max-keys=10"] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(Method::GET, uri, Body::empty()))
            .await
            .expect("route response");

        assert_s3_error_xml(
            response,
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            path_only(uri),
        )
        .await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn malformed_query_names_are_invalid_argument() {
    for uri in ["/bucket?=value", "/bucket?bad%ZZ=value"] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(Method::GET, uri, Body::empty()))
            .await
            .expect("route response");

        assert_s3_error_xml(
            response,
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            path_only(uri),
        )
        .await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn list_objects_v2_continuation_token_calls_storage() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

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
async fn list_objects_v2_slash_delimiter_calls_storage() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

    let response = app
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=2&delimiter=%2F",
            Body::empty(),
        ))
        .await
        .expect("route response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        calls.lock().expect("calls lock").as_slice(),
        ["list_objects"]
    );
}

#[tokio::test]
async fn list_objects_v2_encoding_type_url_returns_encoded_xml() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::with_clock(
        temp_dir.path().to_path_buf(),
        FixedClock(fixed_last_modified()),
    )));

    assert_eq!(
        app.clone()
            .oneshot(request(Method::PUT, "/bucket", Body::empty()))
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                Method::PUT,
                "/bucket/folder/a%20b%281%29.txt",
                Body::from("hello"),
            ))
            .await
            .expect("put object")
            .status(),
        StatusCode::OK
    );

    let response = app
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=2&encoding-type=url&prefix=folder%2F&delimiter=%2F",
            Body::empty(),
        ))
        .await
        .expect("list with URL encoding");
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(response).await.to_vec()).expect("utf-8 XML body");

    assert!(body.contains("<EncodingType>url</EncodingType>"));
    assert!(body.contains("<Prefix>folder%2F</Prefix>"));
    assert!(body.contains("<Delimiter>%2F</Delimiter>"));
    assert!(body.contains("<Key>folder%2Fa%20b%281%29.txt</Key>"));
}

#[tokio::test]
async fn list_objects_v2_max_keys_two_returns_paged_xml() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::with_clock(
        temp_dir.path().to_path_buf(),
        FixedClock(fixed_last_modified()),
    )));

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
        expected_list_objects_v2_xml(ListObjectsV2XmlExpectation {
            key_count: 2,
            max_keys: 2,
            is_truncated: true,
            contents: &[
                list_object_xml("a.txt", "0cc175b9c0f1b6a831c399e269772661", 1),
                list_object_xml("c.txt", "e0323a9039add2978bf5b49550572c7c", 2),
            ],
            next_continuation_token: Some("s3lab-v1:7a2e747874"),
            ..ListObjectsV2XmlExpectation::default()
        })
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
        expected_list_objects_v2_xml(ListObjectsV2XmlExpectation {
            key_count: 1,
            max_keys: 2,
            continuation_token: Some("s3lab-v1:7a2e747874"),
            contents: &[list_object_xml(
                "z.txt",
                "fbade9e36a3f36d3d676c1b808451dd7",
                1,
            )],
            ..ListObjectsV2XmlExpectation::default()
        })
    );
}

#[tokio::test]
async fn list_objects_v2_max_keys_zero_returns_truncated_xml_when_objects_match() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::with_clock(
        temp_dir.path().to_path_buf(),
        FixedClock(fixed_last_modified()),
    )));

    assert_eq!(
        app.clone()
            .oneshot(request(Method::PUT, "/bucket", Body::empty()))
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );
    for key in ["b.txt", "a.txt"] {
        assert_eq!(
            app.clone()
                .oneshot(request(
                    Method::PUT,
                    &format!("/bucket/{key}"),
                    Body::from(key),
                ))
                .await
                .expect("put object")
                .status(),
            StatusCode::OK
        );
    }

    let response = app
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=2&max-keys=0",
            Body::empty(),
        ))
        .await
        .expect("list max-keys zero");

    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(response).await.to_vec()).expect("utf-8 XML body");
    assert_eq!(
        body,
        expected_list_objects_v2_xml(ListObjectsV2XmlExpectation {
            max_keys: 0,
            is_truncated: true,
            next_continuation_token: Some("s3lab-v1:612e747874"),
            ..ListObjectsV2XmlExpectation::default()
        })
    );
}

#[tokio::test]
async fn list_objects_v2_max_keys_zero_reused_token_advances() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::with_clock(
        temp_dir.path().to_path_buf(),
        FixedClock(fixed_last_modified()),
    )));

    assert_eq!(
        app.clone()
            .oneshot(request(Method::PUT, "/bucket", Body::empty()))
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );
    for key in ["c.txt", "a.txt", "b.txt"] {
        assert_eq!(
            app.clone()
                .oneshot(request(
                    Method::PUT,
                    &format!("/bucket/{key}"),
                    Body::from(key),
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
            "/bucket?list-type=2&max-keys=0",
            Body::empty(),
        ))
        .await
        .expect("list first zero max keys page");
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = String::from_utf8(body_bytes(first).await.to_vec()).expect("utf-8 XML body");
    assert_eq!(
        first_body,
        expected_list_objects_v2_xml(ListObjectsV2XmlExpectation {
            max_keys: 0,
            is_truncated: true,
            next_continuation_token: Some("s3lab-v1:612e747874"),
            ..ListObjectsV2XmlExpectation::default()
        })
    );

    let second = app
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=2&max-keys=0&continuation-token=s3lab-v1%3A612e747874",
            Body::empty(),
        ))
        .await
        .expect("list second zero max keys page");
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = String::from_utf8(body_bytes(second).await.to_vec()).expect("utf-8 XML body");
    assert_eq!(
        second_body,
        expected_list_objects_v2_xml(ListObjectsV2XmlExpectation {
            max_keys: 0,
            continuation_token: Some("s3lab-v1:612e747874"),
            is_truncated: true,
            next_continuation_token: Some("s3lab-v1:622e747874"),
            ..ListObjectsV2XmlExpectation::default()
        })
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
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(Method::GET, uri, Body::empty()))
            .await
            .expect("route response");

        assert_invalid_argument_xml(response, path_only(uri)).await;
        assert_eq!(calls.lock().expect("calls lock").as_slice(), &[] as &[&str]);
    }
}

#[tokio::test]
async fn list_objects_v2_non_slash_delimiter_is_invalid_argument_without_storage_call() {
    for uri in [
        "/bucket?list-type=2&delimiter=.",
        "/bucket?list-type=2&delimiter=%7C",
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(Method::GET, uri, Body::empty()))
            .await
            .expect("route response");

        assert_invalid_argument_xml(response, "/bucket").await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn list_objects_v2_xml_invalid_prefix_returns_invalid_argument_without_storage_call() {
    for uri in [
        "/bucket?list-type=2&prefix=logs%00",
        "/bucket?list-type=2&prefix=logs%1F",
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(Method::GET, uri, Body::empty()))
            .await
            .expect("route response");

        assert_invalid_argument_xml(response, "/bucket").await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn list_objects_v2_unsupported_params_are_not_implemented_without_storage_call() {
    for uri in [
        "/bucket?list-type=2&start-after=a.txt",
        "/bucket?list-type=2&fetch-owner=true",
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(Method::GET, uri, Body::empty()))
            .await
            .expect("route response");

        assert_s3_error_xml(
            response,
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "/bucket",
        )
        .await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn list_objects_v2_unsupported_encoding_type_is_invalid_argument_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

    let response = app
        .oneshot(request(
            Method::GET,
            "/bucket?list-type=2&encoding-type=xml",
            Body::empty(),
        ))
        .await
        .expect("route response");

    assert_s3_error_xml(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "/bucket",
    )
    .await;
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn route_invalid_argument_error_xml_reports_path_only_resource() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

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
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidArgument</Code><Message>Invalid argument.</Message><Resource>/bucket</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );
    assert_eq!(calls.lock().expect("calls lock").as_slice(), &[] as &[&str]);
}

#[tokio::test]
async fn runtime_error_request_ids_are_sequential_unique_and_match_xml() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(ServerState::from_storage(FilesystemStorage::new(
        temp_dir.path(),
    )));

    let first = app
        .clone()
        .oneshot(request(Method::GET, "/bad_bucket", Body::empty()))
        .await
        .expect("first route response");
    assert_eq!(first.status(), StatusCode::BAD_REQUEST);
    let first_request_id = first
        .headers()
        .get("x-amz-request-id")
        .and_then(|value| value.to_str().ok())
        .expect("first request id")
        .to_owned();
    assert_eq!(first_request_id, "s3lab-0000000000000001");
    let first_body = String::from_utf8(body_bytes(first).await.to_vec()).expect("utf-8 XML body");
    assert!(first_body.contains(&format!("<RequestId>{first_request_id}</RequestId>")));

    let second = app
        .oneshot(request(Method::GET, "/also_bad_bucket", Body::empty()))
        .await
        .expect("second route response");
    assert_eq!(second.status(), StatusCode::BAD_REQUEST);
    let second_request_id = second
        .headers()
        .get("x-amz-request-id")
        .and_then(|value| value.to_str().ok())
        .expect("second request id")
        .to_owned();
    assert_eq!(second_request_id, "s3lab-0000000000000002");
    assert_ne!(first_request_id, second_request_id);
    let second_body = String::from_utf8(body_bytes(second).await.to_vec()).expect("utf-8 XML body");
    assert!(second_body.contains(&format!("<RequestId>{second_request_id}</RequestId>")));
}

#[tokio::test]
async fn route_error_xml_does_not_echo_presigned_url_query_credentials() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

    let response = app
        .oneshot(request(
            Method::GET,
            "/bucket?X-Amz-Credential=AKIA%2F20260510%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=abcdef123456&X-Amz-Security-Token=session-token",
            Body::empty(),
        ))
        .await
        .expect("route response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = String::from_utf8(body_bytes(response).await.to_vec()).expect("utf-8 XML body");
    assert!(body.contains("<Code>InvalidArgument</Code>"));
    assert!(body.contains("<Resource>/bucket</Resource>"));
    assert!(!body.contains("X-Amz-Credential"));
    assert!(!body.contains("AKIA"));
    assert!(!body.contains("X-Amz-Signature"));
    assert!(!body.contains("abcdef123456"));
    assert!(!body.contains("X-Amz-Security-Token"));
    assert!(!body.contains("session-token"));
    assert_eq!(calls.lock().expect("calls lock").as_slice(), &[] as &[&str]);
}

#[tokio::test]
async fn head_success_and_error_responses_have_empty_bodies() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(storage));

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
async fn get_object_returns_internal_error_for_invalid_response_metadata_headers() {
    for metadata in [
        {
            let mut metadata =
                metadata_with_content_type("bucket", "object.txt", 5, Some("text/plain\nbad"));
            metadata.etag = "\"d41d8cd98f00b204e9800998ecf8427e\"".to_owned();
            metadata
        },
        {
            let mut metadata = metadata_with_content_type("bucket", "object.txt", 5, None);
            metadata.etag = "\"bad\netag\"".to_owned();
            metadata
        },
        {
            let mut metadata = metadata_with_content_type("bucket", "object.txt", 5, None);
            metadata
                .user_metadata
                .insert("bad key".to_owned(), "value".to_owned());
            metadata
        },
        {
            let mut metadata = metadata_with_content_type("bucket", "object.txt", 5, None);
            metadata
                .user_metadata
                .insert("valid-key".to_owned(), "bad\nvalue".to_owned());
            metadata
        },
    ] {
        let storage = RecordingStorage {
            metadata,
            ..RecordingStorage::default()
        };
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
            .await
            .expect("get response");

        assert_s3_error_xml(
            response,
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "/bucket/object.txt",
        )
        .await;
    }
}

#[tokio::test]
async fn head_object_returns_internal_error_for_invalid_response_metadata_headers() {
    let mut metadata = metadata_with_content_type("bucket", "object.txt", 5, None);
    metadata.etag = "\"bad\netag\"".to_owned();
    let storage = RecordingStorage {
        metadata,
        ..RecordingStorage::default()
    };
    let app = router(test_server_state(storage));

    let response = app
        .oneshot(request(Method::HEAD, "/bucket/object.txt", Body::empty()))
        .await
        .expect("head response");

    assert_head_error(response, StatusCode::INTERNAL_SERVER_ERROR).await;
}

#[tokio::test]
async fn storage_errors_return_s3_xml_with_status_and_request_id() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
async fn successful_s3_responses_include_request_id() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

    let create_bucket = app
        .clone()
        .oneshot(request(Method::PUT, "/bucket", Body::empty()))
        .await
        .expect("create bucket");
    assert_eq!(create_bucket.status(), StatusCode::OK);
    assert_success_request_id(&create_bucket);

    let head_bucket = app
        .clone()
        .oneshot(request(Method::HEAD, "/bucket", Body::empty()))
        .await
        .expect("head bucket");
    assert_eq!(head_bucket.status(), StatusCode::OK);
    assert_success_request_id(&head_bucket);

    let list_buckets = app
        .clone()
        .oneshot(request(Method::GET, "/", Body::empty()))
        .await
        .expect("list buckets");
    assert_eq!(list_buckets.status(), StatusCode::OK);
    assert_success_request_id(&list_buckets);

    let put_object = app
        .clone()
        .oneshot(request(
            Method::PUT,
            "/bucket/object.txt",
            Body::from("hello"),
        ))
        .await
        .expect("put object");
    assert_eq!(put_object.status(), StatusCode::OK);
    assert_success_request_id(&put_object);

    let list_objects = app
        .clone()
        .oneshot(request(Method::GET, "/bucket?list-type=2", Body::empty()))
        .await
        .expect("list objects");
    assert_eq!(list_objects.status(), StatusCode::OK);
    assert_success_request_id(&list_objects);

    let delete_object = app
        .clone()
        .oneshot(request(Method::DELETE, "/bucket/object.txt", Body::empty()))
        .await
        .expect("delete object");
    assert_eq!(delete_object.status(), StatusCode::NO_CONTENT);
    assert_success_request_id(&delete_object);

    let delete_bucket = app
        .oneshot(request(Method::DELETE, "/bucket", Body::empty()))
        .await
        .expect("delete bucket");
    assert_eq!(delete_bucket.status(), StatusCode::NO_CONTENT);
    assert_success_request_id(&delete_bucket);
}

#[tokio::test]
async fn bucket_lifecycle_for_empty_bucket_returns_s3_statuses_and_empty_bodies() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    assert_put_object_success_headers(&put);
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
async fn put_object_accepts_aws_cli_crc32_checksum_headers() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
                ("x-amz-sdk-checksum-algorithm", "CRC32"),
                ("x-amz-checksum-crc32", "NhCmhg=="),
            ],
            Body::from("hello"),
        ))
        .await
        .expect("put object with AWS CLI checksum headers");
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_object_success_headers(&put);
    assert!(body_bytes(put).await.is_empty());

    let get = app
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get checksummed object");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn put_object_accepts_aws_cli_crc64nvme_checksum_headers() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
                ("x-amz-sdk-checksum-algorithm", "CRC64NVME"),
                ("x-amz-checksum-crc64nvme", "M3eFcAZSQlc="),
            ],
            Body::from("hello"),
        ))
        .await
        .expect("put object with AWS CLI CRC64NVME checksum headers");
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_object_success_headers(&put);
    assert!(body_bytes(put).await.is_empty());

    let get = app
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get checksummed object");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn put_object_accepts_aws_cli_aws_chunked_crc32_trailer() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "5"),
                ("x-amz-sdk-checksum-algorithm", "CRC32"),
                ("x-amz-trailer", "x-amz-checksum-crc32"),
            ],
            Body::from(
                "5;chunk-signature=signature\r\nhello\r\n0;chunk-signature=signature\r\nx-amz-checksum-crc32:NhCmhg==\r\n\r\n",
            ),
        ))
        .await
        .expect("put aws-chunked object with checksum trailer");
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_object_success_headers(&put);
    assert!(body_bytes(put).await.is_empty());

    let get = app
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get aws-chunked object");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn put_object_accepts_aws_cli_aws_chunked_crc64nvme_trailer() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "5"),
                ("x-amz-sdk-checksum-algorithm", "CRC64NVME"),
                ("x-amz-trailer", "x-amz-checksum-crc64nvme"),
            ],
            Body::from(
                "5;chunk-signature=signature\r\nhello\r\n0;chunk-signature=signature\r\nx-amz-checksum-crc64nvme:M3eFcAZSQlc=\r\n\r\n",
            ),
        ))
        .await
        .expect("put aws-chunked object with CRC64NVME checksum trailer");
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_object_success_headers(&put);
    assert!(body_bytes(put).await.is_empty());

    let get = app
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get aws-chunked object");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn put_object_rejects_mismatched_crc32_checksum_without_storing_object() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
                ("x-amz-sdk-checksum-algorithm", "CRC32"),
                ("x-amz-checksum-crc32", "AAAAAA=="),
            ],
            Body::from("hello"),
        ))
        .await
        .expect("put object with bad checksum");
    assert_s3_error_xml(
        put,
        StatusCode::BAD_REQUEST,
        "BadDigest",
        "/bucket/object.txt",
    )
    .await;

    let get = app
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get rejected object");
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn put_object_rejects_mismatched_crc64nvme_checksum_without_storing_object() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
                ("x-amz-sdk-checksum-algorithm", "CRC64NVME"),
                ("x-amz-checksum-crc64nvme", "AAAAAAAAAAA="),
            ],
            Body::from("hello"),
        ))
        .await
        .expect("put object with bad CRC64NVME checksum");
    assert_s3_error_xml(
        put,
        StatusCode::BAD_REQUEST,
        "BadDigest",
        "/bucket/object.txt",
    )
    .await;

    let get = app
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get rejected object");
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn put_object_rejects_mismatched_aws_chunked_crc32_trailer_without_storing_object() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "5"),
                ("x-amz-sdk-checksum-algorithm", "CRC32"),
                ("x-amz-trailer", "x-amz-checksum-crc32"),
            ],
            Body::from("5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"),
        ))
        .await
        .expect("put aws-chunked object with bad checksum trailer");
    assert_s3_error_xml(
        put,
        StatusCode::BAD_REQUEST,
        "BadDigest",
        "/bucket/object.txt",
    )
    .await;

    let get = app
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get rejected aws-chunked object");
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn put_object_rejects_aws_chunked_oversized_chunk_size_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));

    let put = app
        .oneshot(request_with_headers(
            Method::PUT,
            "/bucket/object.txt",
            &[
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "5"),
            ],
            Body::from("ffffffffffffffff\r\nhello\r\n"),
        ))
        .await
        .expect("put aws-chunked object with oversized chunk size");

    assert_invalid_argument_xml(put, "/bucket/object.txt").await;
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn put_object_rejects_mismatched_aws_chunked_header_checksum_even_when_trailer_matches() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "5"),
                ("x-amz-sdk-checksum-algorithm", "CRC32"),
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-trailer", "x-amz-checksum-crc32"),
            ],
            Body::from("5\r\nhello\r\n0\r\nx-amz-checksum-crc32:NhCmhg==\r\n\r\n"),
        ))
        .await
        .expect("put aws-chunked object with mismatched header checksum and valid trailer");
    assert_s3_error_xml(
        put,
        StatusCode::BAD_REQUEST,
        "BadDigest",
        "/bucket/object.txt",
    )
    .await;

    let get = app
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get rejected aws-chunked object");
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn object_put_without_content_type_defaults_get_and_head_to_binary_octet_stream() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
            "/bucket/object.bin",
            &[("x-amz-meta-Z-Key", "last"), ("x-amz-meta-a-key", "first")],
            Body::from("hello"),
        ))
        .await
        .expect("put object");
    assert_eq!(put.status(), StatusCode::OK);

    let get = app
        .clone()
        .oneshot(request(Method::GET, "/bucket/object.bin", Body::empty()))
        .await
        .expect("get object");
    assert_eq!(get.status(), StatusCode::OK);
    assert_object_metadata_headers(&get, "5", Some("binary/octet-stream"));
    assert_eq!(body_bytes(get).await, Bytes::from_static(b"hello"));

    let head = app
        .oneshot(request(Method::HEAD, "/bucket/object.bin", Body::empty()))
        .await
        .expect("head object");
    assert_eq!(head.status(), StatusCode::OK);
    assert_object_metadata_headers(&head, "5", Some("binary/octet-stream"));
    assert!(body_bytes(head).await.is_empty());
}

#[tokio::test]
async fn put_object_rejects_known_unsupported_s3_headers_without_storage_call() {
    for (header, value) in [
        ("x-amz-acl", "public-read"),
        ("x-amz-tagging", "project=s3lab"),
        ("x-amz-storage-class", "STANDARD_IA"),
        ("x-amz-server-side-encryption", "AES256"),
        ("x-amz-server-side-encryption-aws-kms-key-id", "key-id"),
        ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        ("x-amz-checksum-algorithm", "SHA256"),
        ("x-amz-checksum-sha256", "checksum"),
        ("x-amz-copy-source", "/source-bucket/source-key"),
        ("x-amz-sdk-checksum-algorithm", "SHA256"),
        (
            "x-amz-grant-read",
            "uri=http://acs.amazonaws.com/groups/global/AllUsers",
        ),
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        ("content-md5", "CY9rzUYh03PK3k6DJie09g=="),
        ("cache-control", "max-age=60"),
        ("content-disposition", "attachment"),
        ("content-encoding", "gzip"),
        ("content-language", "en-US"),
        ("expires", "Sun, 10 May 2026 12:00:00 GMT"),
        ("x-amz-trailer", "x-amz-checksum-sha256"),
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request_with_headers(
                Method::PUT,
                "/bucket/object.txt",
                &[(header, value)],
                Body::from("hello"),
            ))
            .await
            .expect("put object");

        assert_s3_error_xml(
            response,
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "/bucket/object.txt",
        )
        .await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn range_get_and_head_object_are_not_implemented_without_storage_call() {
    for method in [Method::GET, Method::HEAD] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request_with_headers(
                method.clone(),
                "/bucket/object.txt",
                &[("range", "bytes=0-1")],
                Body::empty(),
            ))
            .await
            .expect("range object request");

        if method == Method::HEAD {
            assert_head_error(response, StatusCode::NOT_IMPLEMENTED).await;
        } else {
            assert_s3_error_xml(
                response,
                StatusCode::NOT_IMPLEMENTED,
                "NotImplemented",
                "/bucket/object.txt",
            )
            .await;
        }
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn put_object_rejects_body_over_phase1_limit_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));
    let oversized_body = vec![b'x'; PHASE1_MAX_PUT_OBJECT_BODY_BYTES + 1];

    let response = app
        .oneshot(request(
            Method::PUT,
            "/bucket/object.txt",
            Body::from(oversized_body),
        ))
        .await
        .expect("put oversized object");

    assert_s3_error_xml(
        response,
        StatusCode::BAD_REQUEST,
        "EntityTooLarge",
        "/bucket/object.txt",
    )
    .await;
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn filesystem_object_put_get_and_head_use_fixed_last_modified_header() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::with_clock(
        temp_dir.path().to_path_buf(),
        FixedClock(fixed_last_modified()),
    )));

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
    assert_no_last_modified(&put);
    assert!(body_bytes(put).await.is_empty());

    let get = app
        .clone()
        .oneshot(request(Method::GET, "/bucket/object.txt", Body::empty()))
        .await
        .expect("get object");
    assert_eq!(get.status(), StatusCode::OK);
    assert_last_modified(&get, "Sun, 10 May 2026 12:34:56 GMT");
    assert_eq!(body_bytes(get).await, Bytes::from_static(b"hello"));

    let head = app
        .oneshot(request(Method::HEAD, "/bucket/object.txt", Body::empty()))
        .await
        .expect("head object");
    assert_eq!(head.status(), StatusCode::OK);
    assert_last_modified(&head, "Sun, 10 May 2026 12:34:56 GMT");
    assert!(body_bytes(head).await.is_empty());
}

#[tokio::test]
async fn object_lifecycle_put_overwrite_get_head_delete_contract() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
async fn put_object_rejects_invalid_user_metadata_headers_without_storage_call() {
    for headers in [
        vec![("x-amz-meta-", "value")],
        vec![("x-amz-meta-Z-Key", "last"), ("x-amz-meta-z-key", "again")],
    ] {
        let storage = RecordingStorage::default();
        let calls = Arc::clone(&storage.calls);
        let app = router(test_server_state(storage));

        let response = app
            .oneshot(request_with_headers(
                Method::PUT,
                "/bucket/object.txt",
                &headers,
                Body::from("hello"),
            ))
            .await
            .expect("put object");

        assert_invalid_argument_xml(response, "/bucket/object.txt").await;
        assert_no_storage_calls(&calls);
    }
}

#[tokio::test]
async fn put_object_rejects_non_utf8_user_metadata_value() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
async fn put_object_rejects_non_utf8_content_type_without_storage_call() {
    let storage = RecordingStorage::default();
    let calls = Arc::clone(&storage.calls);
    let app = router(test_server_state(storage));
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/bucket/object.txt")
        .header(
            CONTENT_TYPE,
            HeaderValue::from_bytes(&[0xff]).expect("valid opaque header value"),
        )
        .body(Body::from("hello"))
        .expect("valid request");

    let response = app.oneshot(request).await.expect("put object");

    assert_invalid_argument_xml(response, "/bucket/object.txt").await;
    assert_no_storage_calls(&calls);
}

#[tokio::test]
async fn put_object_rejects_duplicate_normalized_user_metadata_keys() {
    let temp_dir = TempDir::new().expect("temp dir");
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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
    let app = router(test_server_state(FilesystemStorage::new(temp_dir.path())));

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

fn request_with_owned_headers(
    method: Method,
    uri: &str,
    headers: &[(String, String)],
    body: Body,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(
            name,
            HeaderValue::from_str(value).expect("valid test header"),
        );
    }
    builder.body(body).expect("valid request")
}

fn signed_request_headers(method: Method, uri: &str, body: &[u8]) -> Vec<(String, String)> {
    signed_request_headers_with_access_key(method, uri, body, "s3lab")
}

fn signed_request_headers_without_payload_hash(method: Method, uri: &str) -> Vec<(String, String)> {
    let request_datetime = "20260512T010203Z";
    let credential_scope = "20260512/us-east-1/s3/aws4_request";
    let signed_headers = "host;x-amz-date";
    let payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let parsed_uri = uri.parse::<Uri>().expect("valid URI");
    let header_pairs = [("host", "localhost"), ("x-amz-date", request_datetime)];
    let unsigned_authorization = format!(
        "AWS4-HMAC-SHA256 Credential=s3lab/{credential_scope}, SignedHeaders={signed_headers}, Signature=0000000000000000000000000000000000000000000000000000000000000000"
    );
    let authorization =
        parse_authorization_header(&unsigned_authorization).expect("authorization shape");
    let canonical_request = build_canonical_request(
        method.as_str(),
        parsed_uri.path(),
        &[],
        &header_pairs,
        authorization.signed_headers(),
        payload_hash,
    )
    .expect("canonical request");
    let string_to_sign = build_string_to_sign(
        request_datetime,
        authorization.credential().scope(),
        &canonical_request,
    );
    let signature = sigv4_signature(
        "s3lab-secret",
        "20260512",
        "us-east-1",
        "s3",
        &string_to_sign,
    );
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential=s3lab/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    vec![
        ("host".to_owned(), "localhost".to_owned()),
        ("x-amz-date".to_owned(), request_datetime.to_owned()),
        ("authorization".to_owned(), authorization),
    ]
}

fn signed_request_headers_with_access_key(
    method: Method,
    uri: &str,
    body: &[u8],
    access_key: &str,
) -> Vec<(String, String)> {
    let request_datetime = "20260512T010203Z";
    let credential_scope = "20260512/us-east-1/s3/aws4_request";
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let payload_hash = sha256_lower_hex_bytes(body);
    let parsed_uri = uri.parse::<Uri>().expect("valid URI");
    let header_pairs = [
        ("host", "localhost"),
        ("x-amz-content-sha256", payload_hash.as_str()),
        ("x-amz-date", request_datetime),
    ];
    let unsigned_authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature=0000000000000000000000000000000000000000000000000000000000000000"
    );
    let authorization =
        parse_authorization_header(&unsigned_authorization).expect("authorization shape");
    let canonical_request = build_canonical_request(
        method.as_str(),
        parsed_uri.path(),
        &[],
        &header_pairs,
        authorization.signed_headers(),
        &payload_hash,
    )
    .expect("canonical request");
    let string_to_sign = build_string_to_sign(
        request_datetime,
        authorization.credential().scope(),
        &canonical_request,
    );
    let signature = sigv4_signature(
        "s3lab-secret",
        "20260512",
        "us-east-1",
        "s3",
        &string_to_sign,
    );
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    vec![
        ("host".to_owned(), "localhost".to_owned()),
        ("x-amz-date".to_owned(), request_datetime.to_owned()),
        ("x-amz-content-sha256".to_owned(), payload_hash),
        ("authorization".to_owned(), authorization),
    ]
}

fn replace_authorization_signature(headers: &mut [(String, String)], signature: &str) {
    let authorization = headers
        .iter_mut()
        .find(|(name, _value)| name == "authorization")
        .expect("authorization header");
    let (prefix, _old_signature) = authorization
        .1
        .rsplit_once("Signature=")
        .expect("signature parameter");
    authorization.1 = format!("{prefix}Signature={signature}");
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
    headers
        .iter()
        .find(|(header_name, _value)| header_name == name)
        .map(|(_name, value)| value.as_str())
        .expect("header exists")
}

fn signature_from_authorization(authorization: &str) -> &str {
    authorization
        .rsplit_once("Signature=")
        .map(|(_prefix, signature)| signature)
        .expect("signature parameter")
}

fn sigv4_signature(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    let date_key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, service.as_bytes());
    let signing_key = hmac_sha256(&service_key, b"aws4_request");
    hex_encode(&hmac_sha256(&signing_key, string_to_sign.as_bytes()))
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> [u8; 32] {
    const HMAC_SHA256_BLOCK_SIZE: usize = 64;
    let mut normalized_key = [0_u8; HMAC_SHA256_BLOCK_SIZE];
    if key.len() > HMAC_SHA256_BLOCK_SIZE {
        let digest = Sha256::digest(key);
        normalized_key[..digest.len()].copy_from_slice(&digest);
    } else {
        normalized_key[..key.len()].copy_from_slice(key);
    }

    let mut outer_key_pad = [0x5c_u8; HMAC_SHA256_BLOCK_SIZE];
    let mut inner_key_pad = [0x36_u8; HMAC_SHA256_BLOCK_SIZE];
    for index in 0..HMAC_SHA256_BLOCK_SIZE {
        outer_key_pad[index] ^= normalized_key[index];
        inner_key_pad[index] ^= normalized_key[index];
    }

    let mut inner = Sha256::new();
    inner.update(inner_key_pad);
    inner.update(value);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_key_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn sha256_lower_hex_bytes(value: &[u8]) -> String {
    hex_encode(&Sha256::digest(value))
}

fn hex_encode(value: &[u8]) -> String {
    value
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
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
    assert!(body.contains("<Code>InvalidArgument</Code>"));
    assert!(body.contains(&format!("<Resource>{}</Resource>", xml_text(resource))));
}

async fn assert_s3_error_xml(
    response: axum::http::Response<Body>,
    status: StatusCode,
    code: &str,
    resource: &str,
) {
    assert_eq!(response.status(), status);
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
    assert!(body.contains(&format!("<Code>{code}</Code>")));
    assert!(body.contains(&format!("<Resource>{}</Resource>", xml_text(resource))));
}

async fn assert_head_error(response: axum::http::Response<Body>, status: StatusCode) {
    assert_eq!(response.status(), status);
    assert_eq!(
        response
            .headers()
            .get("x-amz-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    assert!(body_bytes(response).await.is_empty());
}

fn assert_no_storage_calls(calls: &Arc<Mutex<Vec<&'static str>>>) {
    assert_eq!(calls.lock().expect("calls lock").as_slice(), &[] as &[&str]);
}

fn assert_success_request_id(response: &axum::http::Response<Body>) {
    assert_eq!(
        response
            .headers()
            .get("x-amz-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("s3lab-test-request-id")
    );
}

fn xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn path_only(uri: &str) -> &str {
    uri.split_once('?').map_or(uri, |(path, _)| path)
}

#[derive(Default)]
struct ListObjectsV2XmlExpectation<'a> {
    key_count: usize,
    max_keys: usize,
    continuation_token: Option<&'a str>,
    is_truncated: bool,
    contents: &'a [String],
    next_continuation_token: Option<&'a str>,
}

fn expected_list_objects_v2_xml(expectation: ListObjectsV2XmlExpectation<'_>) -> String {
    let mut xml = format!(
        "{}<ListBucketResult><Name>bucket</Name><Prefix></Prefix><KeyCount>{}</KeyCount><MaxKeys>{}</MaxKeys>",
        xml_declaration(),
        expectation.key_count,
        expectation.max_keys
    );

    if let Some(continuation_token) = expectation.continuation_token {
        xml.push_str(&format!(
            "<ContinuationToken>{continuation_token}</ContinuationToken>"
        ));
    }
    xml.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        expectation.is_truncated
    ));
    for content in expectation.contents {
        xml.push_str(content);
    }
    if let Some(next_continuation_token) = expectation.next_continuation_token {
        xml.push_str(&format!(
            "<NextContinuationToken>{next_continuation_token}</NextContinuationToken>"
        ));
    }
    xml.push_str("</ListBucketResult>");
    xml
}

fn list_object_xml(key: &str, etag: &str, size: u64) -> String {
    format!(
        "<Contents><Key>{key}</Key><LastModified>2026-05-10T12:34:56.000Z</LastModified><ETag>&quot;{etag}&quot;</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
    )
}

fn xml_declaration() -> &'static str {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
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

fn assert_last_modified(response: &axum::http::Response<Body>, expected: &str) {
    assert_eq!(
        response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|v| v.to_str().ok()),
        Some(expected)
    );
}

fn assert_no_last_modified(response: &axum::http::Response<Body>) {
    assert_eq!(
        response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|v| v.to_str().ok()),
        None
    );
}

fn assert_put_object_success_headers(response: &axum::http::Response<Body>) {
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
        None
    );
    assert_eq!(
        response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|v| v.to_str().ok()),
        None
    );
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
        None
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-a-key")
            .and_then(|v| v.to_str().ok()),
        None
    );
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

#[derive(Debug, Clone, Default)]
struct TestTraceSink {
    events: Arc<Mutex<Vec<TraceEvent>>>,
}

impl TestTraceSink {
    fn events(&self) -> Vec<TraceEvent> {
        self.events.lock().expect("trace events lock").clone()
    }
}

impl TraceSink for TestTraceSink {
    fn record(&self, event: TraceEvent) {
        self.events.lock().expect("trace events lock").push(event);
    }
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
            entries: Vec::new(),
            objects: Vec::new(),
            common_prefixes: Vec::new(),
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
