// SPDX-License-Identifier: Apache-2.0

mod support;

use hyper::body::Bytes;
use hyper::http::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG};
use hyper::http::{Method, Response, StatusCode};
use support::{request, response_bytes, response_text, TestServer, TEST_SUPPORT_MARKER};

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
    assert_eq!(
        put.headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok()),
        Some("\"5d41402abc4b2a76b9719d911017c592\"")
    );
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
    assert_eq!(
        response_text(list).await.expect("list prefix body"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>bucket-a</Name><Prefix>logs/</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>logs/a.txt</Key><Size>2</Size></Contents><Contents><Key>logs/z.txt</Key><Size>1</Size></Contents></ListBucketResult>"
    );

    server.shutdown().await;
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
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NotImplemented</Code><Message>A header you provided implies functionality that is not implemented.</Message><Resource>/bucket-a?acl</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
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
