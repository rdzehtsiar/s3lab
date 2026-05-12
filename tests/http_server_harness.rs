// SPDX-License-Identifier: Apache-2.0

mod support;

use hyper::body::Bytes;
use hyper::http::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED};
use hyper::http::{Method, Response, StatusCode};
use md5::Md5;
use s3lab::s3::bucket::BucketName;
use s3lab::s3::object::ObjectKey;
use s3lab::s3::sigv4::{
    build_canonical_request, build_string_to_sign, parse_authorization_header,
    parse_query_authorization, SIGV4_UNSIGNED_PAYLOAD,
};
use s3lab::storage::fs::{FilesystemStorage, StorageClock};
use s3lab::storage::STORAGE_ROOT_DIR;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use support::{
    request, response_bytes, response_text, test_server_state, TestServer, TEST_SUPPORT_MARKER,
};
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
async fn presigned_object_get_and_put_round_trip_over_real_http() {
    let server = TestServer::start().await;
    let host = server
        .base_url()
        .strip_prefix("http://")
        .expect("loopback HTTP base URL");

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );

    let put_path = presigned_object_path_query(Method::PUT, "/bucket-a/object.txt", host);
    let put = request(
        Method::PUT,
        &server.url(&put_path),
        Bytes::from_static(b"hello from real presigned http"),
        &[
            ("host", host),
            ("content-type", "text/plain"),
            ("x-amz-meta-case", "value"),
        ],
    )
    .await
    .expect("presigned put object over HTTP");
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(
        put.headers()
            .get("x-amz-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    assert!(response_bytes(put).await.expect("put body").is_empty());

    let get_path = presigned_object_path_query(Method::GET, "/bucket-a/object.txt", host);
    let get = request(
        Method::GET,
        &server.url(&get_path),
        Bytes::new(),
        &[("host", host)],
    )
    .await
    .expect("presigned get object over HTTP");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/plain")
    );
    assert_eq!(
        get.headers()
            .get("x-amz-meta-case")
            .and_then(|value| value.to_str().ok()),
        Some("value")
    );
    assert_eq!(
        get.headers()
            .get("x-amz-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("s3lab-test-request-id")
    );
    assert_eq!(
        response_bytes(get).await.expect("get body"),
        Bytes::from_static(b"hello from real presigned http")
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
async fn list_objects_delimiter_over_real_http() {
    let server = TestServer::start().await;

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );

    for (key, body) in [
        ("logs/a.txt", "aa"),
        ("logs/archive/1.txt", "archive"),
        ("logs/z.txt", "z"),
        ("images/a.txt", "excluded"),
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
        &server.url("/bucket-a?list-type=2&prefix=logs%2F&delimiter=%2F"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list delimiter objects over HTTP");
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = response_text(list).await.expect("list delimiter body");
    assert_ordered_contains(
        &list_body,
        &[
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>bucket-a</Name><Prefix>logs/</Prefix><Delimiter>/</Delimiter><KeyCount>3</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
            "<Contents><Key>logs/a.txt</Key><LastModified>",
            "</LastModified><ETag>&quot;4124bc0a9335c27f086f24ba207a4912&quot;</ETag><Size>2</Size><StorageClass>STANDARD</StorageClass></Contents>",
            "<CommonPrefixes><Prefix>logs/archive/</Prefix></CommonPrefixes>",
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
        test_server_state(FilesystemStorage::with_clock(
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
async fn multipart_lifecycle_over_real_http() {
    let server = TestServer::start().await;

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );

    let create = request(
        Method::POST,
        &server.url("/bucket-a/path/object.bin?uploads"),
        Bytes::new(),
        &[
            ("content-type", "application/octet-stream"),
            ("x-amz-meta-case", "value"),
        ],
    )
    .await
    .expect("create multipart upload over HTTP");
    assert_eq!(create.status(), StatusCode::OK);
    assert_eq!(
        create
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/xml")
    );
    let create_body = response_text(create).await.expect("create multipart body");
    assert!(create_body.contains("<Bucket>bucket-a</Bucket>"));
    assert!(create_body.contains("<Key>path/object.bin</Key>"));
    let upload_id = xml_element_text(&create_body, "UploadId");

    let invisible = request(
        Method::GET,
        &server.url("/bucket-a?list-type=2&prefix=path%2F"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list objects before multipart completion");
    assert_eq!(invisible.status(), StatusCode::OK);
    let invisible_body = response_text(invisible).await.expect("invisible list body");
    assert!(invisible_body.contains("<KeyCount>0</KeyCount>"));
    assert!(!invisible_body.contains("<Key>path/object.bin</Key>"));

    let part_two = Bytes::from_static(b"world");
    let upload_two = request(
        Method::PUT,
        &server.url(&format!(
            "/bucket-a/path/object.bin?partNumber=2&uploadId={upload_id}"
        )),
        part_two.clone(),
        &[],
    )
    .await
    .expect("upload second part over HTTP");
    assert_eq!(upload_two.status(), StatusCode::OK);
    assert_eq!(
        upload_two
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(md5_etag(part_two.as_ref()).as_str())
    );

    let part_one = Bytes::from_static(b"hello ");
    let upload_one = request(
        Method::PUT,
        &server.url(&format!(
            "/bucket-a/path/object.bin?partNumber=1&uploadId={upload_id}"
        )),
        part_one.clone(),
        &[],
    )
    .await
    .expect("upload first part over HTTP");
    assert_eq!(upload_one.status(), StatusCode::OK);
    assert_eq!(
        upload_one
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(md5_etag(part_one.as_ref()).as_str())
    );

    let list_parts = request(
        Method::GET,
        &server.url(&format!("/bucket-a/path/object.bin?uploadId={upload_id}")),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list multipart parts over HTTP");
    assert_eq!(list_parts.status(), StatusCode::OK);
    let list_parts_body = response_text(list_parts).await.expect("list parts body");
    assert_ordered_contains(
        &list_parts_body,
        &[
            "<ListPartsResult><Bucket>bucket-a</Bucket><Key>path/object.bin</Key>",
            &format!("<UploadId>{upload_id}</UploadId>"),
            "<PartNumber>1</PartNumber>",
            &format!(
                "<ETag>{}</ETag><Size>6</Size>",
                md5_etag_xml(part_one.as_ref())
            ),
            "<PartNumber>2</PartNumber>",
            &format!(
                "<ETag>{}</ETag><Size>5</Size>",
                md5_etag_xml(part_two.as_ref())
            ),
        ],
    );

    let complete_etag = multipart_etag_for(&[part_one.as_ref(), part_two.as_ref()]);
    let complete = request(
        Method::POST,
        &server.url(&format!("/bucket-a/path/object.bin?uploadId={upload_id}")),
        Bytes::from(complete_multipart_xml(&[
            (1, &md5_etag(part_one.as_ref())),
            (2, &md5_etag(part_two.as_ref())),
        ])),
        &[],
    )
    .await
    .expect("complete multipart upload over HTTP");
    assert_eq!(complete.status(), StatusCode::OK);
    let complete_body = response_text(complete).await.expect("complete body");
    assert_eq!(
        complete_body,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult><Location>/bucket-a/path/object.bin</Location><Bucket>bucket-a</Bucket><Key>path/object.bin</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
            xml_escape_text(&complete_etag)
        )
    );

    let get = request(
        Method::GET,
        &server.url("/bucket-a/path/object.bin"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("get completed multipart object over HTTP");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("11")
    );
    assert_eq!(
        get.headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/octet-stream")
    );
    assert_eq!(
        get.headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(complete_etag.as_str())
    );
    assert_eq!(
        get.headers()
            .get("x-amz-meta-case")
            .and_then(|value| value.to_str().ok()),
        Some("value")
    );
    assert_eq!(
        response_bytes(get).await.expect("completed object body"),
        Bytes::from_static(b"hello world")
    );

    let visible = request(
        Method::GET,
        &server.url("/bucket-a?list-type=2&prefix=path%2F"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list completed multipart object");
    assert_eq!(visible.status(), StatusCode::OK);
    let visible_body = response_text(visible).await.expect("visible list body");
    assert_ordered_contains(
        &visible_body,
        &[
            "<KeyCount>1</KeyCount>",
            "<Contents><Key>path/object.bin</Key><LastModified>",
            &format!(
                "</LastModified><ETag>{}</ETag><Size>11</Size>",
                xml_escape_text(&complete_etag)
            ),
        ],
    );

    let completed_upload = request(
        Method::GET,
        &server.url(&format!("/bucket-a/path/object.bin?uploadId={upload_id}")),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list completed upload parts");
    assert_http_s3_error(
        completed_upload,
        StatusCode::NOT_FOUND,
        "NoSuchUpload",
        "/bucket-a/path/object.bin",
    )
    .await;

    let abort_create = request(
        Method::POST,
        &server.url("/bucket-a/path/aborted.bin?uploads"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("create upload to abort");
    assert_eq!(abort_create.status(), StatusCode::OK);
    let abort_upload_id = xml_element_text(
        &response_text(abort_create)
            .await
            .expect("abort create body"),
        "UploadId",
    );
    assert_eq!(
        request(
            Method::PUT,
            &server.url(&format!(
                "/bucket-a/path/aborted.bin?partNumber=1&uploadId={abort_upload_id}"
            )),
            Bytes::from_static(b"aborted"),
            &[],
        )
        .await
        .expect("upload part before abort")
        .status(),
        StatusCode::OK
    );
    let abort = request(
        Method::DELETE,
        &server.url(&format!(
            "/bucket-a/path/aborted.bin?uploadId={abort_upload_id}"
        )),
        Bytes::new(),
        &[],
    )
    .await
    .expect("abort multipart upload over HTTP");
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);
    assert!(response_bytes(abort).await.expect("abort body").is_empty());

    let aborted_object = request(
        Method::GET,
        &server.url("/bucket-a/path/aborted.bin"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("get aborted multipart object");
    assert_http_s3_error(
        aborted_object,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "/bucket-a/path/aborted.bin",
    )
    .await;

    let aborted_upload = request(
        Method::GET,
        &server.url(&format!(
            "/bucket-a/path/aborted.bin?uploadId={abort_upload_id}"
        )),
        Bytes::new(),
        &[],
    )
    .await
    .expect("list aborted multipart upload");
    assert_http_s3_error(
        aborted_upload,
        StatusCode::NOT_FOUND,
        "NoSuchUpload",
        "/bucket-a/path/aborted.bin",
    )
    .await;

    server.shutdown().await;
}

#[tokio::test]
async fn multipart_negative_cases_over_real_http() {
    let server = TestServer::start().await;

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );

    let missing_upload = request(
        Method::PUT,
        &server.url("/bucket-a/object.bin?partNumber=1&uploadId=missing-upload"),
        Bytes::from_static(b"part"),
        &[],
    )
    .await
    .expect("upload part to missing upload");
    assert_http_s3_error(
        missing_upload,
        StatusCode::NOT_FOUND,
        "NoSuchUpload",
        "/bucket-a/object.bin",
    )
    .await;

    let upload_id = create_multipart_upload(&server, "/bucket-a/object.bin").await;
    let part_one = Bytes::from_static(b"one");
    let part_two = Bytes::from_static(b"two");
    upload_part(
        &server,
        "/bucket-a/object.bin",
        &upload_id,
        1,
        part_one.clone(),
    )
    .await;
    upload_part(
        &server,
        "/bucket-a/object.bin",
        &upload_id,
        2,
        part_two.clone(),
    )
    .await;

    let invalid_order = request(
        Method::POST,
        &server.url(&format!("/bucket-a/object.bin?uploadId={upload_id}")),
        Bytes::from(complete_multipart_xml(&[
            (2, &md5_etag(part_two.as_ref())),
            (1, &md5_etag(part_one.as_ref())),
        ])),
        &[],
    )
    .await
    .expect("complete multipart with invalid part order");
    assert_http_s3_error(
        invalid_order,
        StatusCode::BAD_REQUEST,
        "InvalidPartOrder",
        "/bucket-a/object.bin",
    )
    .await;

    let etag_mismatch = request(
        Method::POST,
        &server.url(&format!("/bucket-a/object.bin?uploadId={upload_id}")),
        Bytes::from(complete_multipart_xml(&[(1, "\"wrong-etag\"")])),
        &[],
    )
    .await
    .expect("complete multipart with ETag mismatch");
    assert_http_s3_error(
        etag_mismatch,
        StatusCode::BAD_REQUEST,
        "InvalidPart",
        "/bucket-a/object.bin",
    )
    .await;

    let malformed_xml = request(
        Method::POST,
        &server.url(&format!("/bucket-a/object.bin?uploadId={upload_id}")),
        Bytes::from_static(b"<CompleteMultipartUpload><Part><ETag>\"etag\"</ETag></Part>"),
        &[],
    )
    .await
    .expect("complete multipart with malformed XML");
    assert_http_s3_error(
        malformed_xml,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "/bucket-a/object.bin",
    )
    .await;

    let abort = request(
        Method::DELETE,
        &server.url(&format!("/bucket-a/object.bin?uploadId={upload_id}")),
        Bytes::new(),
        &[],
    )
    .await
    .expect("abort upload after negative cases");
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);

    let complete_aborted = request(
        Method::POST,
        &server.url(&format!("/bucket-a/object.bin?uploadId={upload_id}")),
        Bytes::from(complete_multipart_xml(&[(1, &md5_etag(part_one.as_ref()))])),
        &[],
    )
    .await
    .expect("complete aborted multipart upload");
    assert_http_s3_error(
        complete_aborted,
        StatusCode::NOT_FOUND,
        "NoSuchUpload",
        "/bucket-a/object.bin",
    )
    .await;

    let aborted_object = request(
        Method::GET,
        &server.url("/bucket-a/object.bin"),
        Bytes::new(),
        &[],
    )
    .await
    .expect("get aborted multipart object");
    assert_http_s3_error(
        aborted_object,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "/bucket-a/object.bin",
    )
    .await;

    server.shutdown().await;
}

#[tokio::test]
async fn upload_part_integrity_failures_over_real_http() {
    let server = TestServer::start().await;
    let host = server
        .base_url()
        .strip_prefix("http://")
        .expect("loopback HTTP base URL");

    assert_eq!(
        request(Method::PUT, &server.url("/bucket-a"), Bytes::new(), &[])
            .await
            .expect("create bucket")
            .status(),
        StatusCode::OK
    );

    let checksum_upload_id = create_multipart_upload(&server, "/bucket-a/checksum.bin").await;
    let checksum_mismatch = request(
        Method::PUT,
        &server.url(&format!(
            "/bucket-a/checksum.bin?partNumber=1&uploadId={checksum_upload_id}"
        )),
        Bytes::from_static(b"checksum-body"),
        &[("x-amz-checksum-crc32", "AAAAAA==")],
    )
    .await
    .expect("upload part with checksum mismatch");
    assert_http_s3_error(
        checksum_mismatch,
        StatusCode::BAD_REQUEST,
        "BadDigest",
        "/bucket-a/checksum.bin",
    )
    .await;

    let signed_upload_id = create_multipart_upload(&server, "/bucket-a/signed.bin").await;
    let signed_body = b"signed-body";
    let sent_body = Bytes::from_static(b"sent-secret-body");
    let path_and_query = format!("/bucket-a/signed.bin?partNumber=1&uploadId={signed_upload_id}");
    let signed_headers = signed_request_headers(
        Method::PUT,
        &path_and_query,
        host,
        &sha256_lower_hex_bytes(signed_body),
    );
    let header_refs = owned_header_refs(&signed_headers);
    let hash_mismatch = request(
        Method::PUT,
        &server.url(&path_and_query),
        sent_body,
        &header_refs,
    )
    .await
    .expect("upload part with signed literal hash mismatch");
    assert_http_s3_error(
        hash_mismatch,
        StatusCode::BAD_REQUEST,
        "XAmzContentSHA256Mismatch",
        "/bucket-a/signed.bin",
    )
    .await;

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

async fn create_multipart_upload(server: &TestServer, path: &str) -> String {
    let response = request(
        Method::POST,
        &server.url(&format!("{path}?uploads")),
        Bytes::new(),
        &[],
    )
    .await
    .expect("create multipart upload");
    assert_eq!(response.status(), StatusCode::OK);
    xml_element_text(
        &response_text(response).await.expect("create upload body"),
        "UploadId",
    )
}

async fn upload_part(
    server: &TestServer,
    path: &str,
    upload_id: &str,
    part_number: u32,
    body: Bytes,
) {
    let response = request(
        Method::PUT,
        &server.url(&format!(
            "{path}?partNumber={part_number}&uploadId={upload_id}"
        )),
        body,
        &[],
    )
    .await
    .expect("upload multipart part");
    assert_eq!(response.status(), StatusCode::OK);
}

async fn assert_http_s3_error(
    response: Response<hyper::body::Incoming>,
    status: StatusCode,
    code: &str,
    resource: &str,
) {
    assert_eq!(response.status(), status);
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
    let body = response_text(response).await.expect("S3 error body");
    assert!(body.contains(&format!("<Code>{code}</Code>")));
    assert!(body.contains(&format!(
        "<Resource>{}</Resource>",
        xml_escape_text(resource)
    )));
}

fn complete_multipart_xml(parts: &[(u32, &str)]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (part_number, etag) in parts {
        xml.push_str(&format!(
            "<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag></Part>"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

fn xml_element_text(xml: &str, element: &str) -> String {
    let start_tag = format!("<{element}>");
    let end_tag = format!("</{element}>");
    let start = xml
        .find(&start_tag)
        .map(|index| index + start_tag.len())
        .expect("XML start element");
    let end = xml[start..]
        .find(&end_tag)
        .map(|index| start + index)
        .expect("XML end element");

    xml[start..end].to_owned()
}

fn md5_etag(bytes: &[u8]) -> String {
    format!("\"{}\"", hex_encode(&Md5::digest(bytes)))
}

fn md5_etag_xml(bytes: &[u8]) -> String {
    xml_escape_text(&md5_etag(bytes))
}

fn multipart_etag_for(parts: &[&[u8]]) -> String {
    let mut joined_digests = Vec::new();
    for part in parts {
        joined_digests.extend_from_slice(&Md5::digest(part));
    }

    format!(
        "\"{}-{}\"",
        hex_encode(&Md5::digest(joined_digests)),
        parts.len()
    )
}

fn xml_escape_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn presigned_object_path_query(method: Method, path: &str, host: &str) -> String {
    let request_datetime = "20260512T010203Z";
    let credential_scope = "20260512/us-east-1/s3/aws4_request";
    let mut query = vec![
        ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
        (
            "X-Amz-Credential".to_owned(),
            format!("s3lab/{credential_scope}"),
        ),
        ("X-Amz-Date".to_owned(), request_datetime.to_owned()),
        ("X-Amz-Expires".to_owned(), "60".to_owned()),
        ("X-Amz-SignedHeaders".to_owned(), "host".to_owned()),
    ];
    let unsigned_query_refs = query_refs(&query);
    let mut query_with_placeholder = query.clone();
    query_with_placeholder.push((
        "X-Amz-Signature".to_owned(),
        "0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
    ));
    let placeholder_refs = query_refs(&query_with_placeholder);
    let authorization =
        parse_query_authorization(&placeholder_refs).expect("presigned query shape");
    let canonical_request = build_canonical_request(
        method.as_str(),
        path,
        &unsigned_query_refs,
        &[("host", host)],
        authorization.signed_headers(),
        SIGV4_UNSIGNED_PAYLOAD,
    )
    .expect("canonical presigned request");
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
    query.push(("X-Amz-Signature".to_owned(), signature));

    format!("{path}?{}", encoded_query(&query))
}

fn signed_request_headers(
    method: Method,
    path_and_query: &str,
    host: &str,
    payload_hash: &str,
) -> Vec<(String, String)> {
    let request_datetime = "20260512T010203Z";
    let credential_scope = "20260512/us-east-1/s3/aws4_request";
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let path = path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _query)| path);
    let query = query_pairs(path_and_query);
    let query_refs = query_refs(&query);
    let header_pairs = [
        ("host", host),
        ("x-amz-content-sha256", payload_hash),
        ("x-amz-date", request_datetime),
    ];
    let unsigned_authorization = format!(
        "AWS4-HMAC-SHA256 Credential=s3lab/{credential_scope}, SignedHeaders={signed_headers}, Signature=0000000000000000000000000000000000000000000000000000000000000000"
    );
    let authorization =
        parse_authorization_header(&unsigned_authorization).expect("authorization shape");
    let canonical_request = build_canonical_request(
        method.as_str(),
        path,
        &query_refs,
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
        ("host".to_owned(), host.to_owned()),
        ("x-amz-date".to_owned(), request_datetime.to_owned()),
        ("x-amz-content-sha256".to_owned(), payload_hash.to_owned()),
        ("authorization".to_owned(), authorization),
    ]
}

fn query_pairs(path_and_query: &str) -> Vec<(String, String)> {
    let Some((_path, query)) = path_and_query.split_once('?') else {
        return Vec::new();
    };

    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            pair.split_once('=')
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .unwrap_or_else(|| (pair.to_owned(), String::new()))
        })
        .collect()
}

fn owned_header_refs(headers: &[(String, String)]) -> Vec<(&str, &str)> {
    headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect()
}

fn query_refs(query: &[(String, String)]) -> Vec<(&str, &str)> {
    query
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect()
}

fn encoded_query(query: &[(String, String)]) -> String {
    query
        .iter()
        .map(|(name, value)| format!("{}={}", aws_query_encode(name), aws_query_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn aws_query_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
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

fn hex_encode(value: &[u8]) -> String {
    value
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
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
    sha256_lower_hex_bytes(value.as_bytes())
}

fn sha256_lower_hex_bytes(value: &[u8]) -> String {
    hex_encode(&Sha256::digest(value))
}
