// SPDX-License-Identifier: Apache-2.0

use s3lab::s3::bucket::BucketName;
use s3lab::s3::error::{S3Error, S3ErrorCode, S3RequestId, TEST_REQUEST_ID};
use s3lab::s3::object::ObjectKey;
use s3lab::s3::xml::error_response_xml;
use s3lab::storage::StorageError;
use std::path::PathBuf;

#[test]
fn no_such_bucket_error_xml_has_stable_field_order() {
    let error = S3Error::new(
        S3ErrorCode::NoSuchBucket,
        "/missing-bucket",
        S3RequestId::new(TEST_REQUEST_ID),
    );

    assert_eq!(
        error_response_xml(&error),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist.</Message><Resource>/missing-bucket</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );
}

#[test]
fn error_xml_escapes_resource_and_custom_message_text() {
    let error = S3Error::with_message(
        S3ErrorCode::InvalidArgument,
        "bad & unsupported <header> value > limit",
        "/bucket/key?a=1&b=<value>",
        S3RequestId::new(TEST_REQUEST_ID),
    );

    assert_eq!(
        error_response_xml(&error),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidArgument</Code><Message>bad &amp; unsupported &lt;header&gt; value &gt; limit</Message><Resource>/bucket/key?a=1&amp;b=&lt;value&gt;</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );
}

#[test]
fn selected_error_codes_have_s3_default_messages() {
    let cases = [
        (S3ErrorCode::NoSuchKey, "The specified key does not exist."),
        (
            S3ErrorCode::BucketNotEmpty,
            "The bucket you tried to delete is not empty.",
        ),
        (
            S3ErrorCode::NotImplemented,
            "A header you provided implies functionality that is not implemented.",
        ),
        (
            S3ErrorCode::MethodNotAllowed,
            "The specified method is not allowed against this resource.",
        ),
    ];

    for (code, expected_message) in cases {
        assert_eq!(code.default_message(), expected_message);
    }
}

#[test]
fn storage_errors_convert_to_s3_errors_with_resource_and_request_id() {
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("missing.txt");

    let cases = [
        (
            StorageError::NoSuchBucket {
                bucket: bucket.clone(),
            },
            S3ErrorCode::NoSuchBucket,
        ),
        (
            StorageError::NoSuchKey {
                bucket: bucket.clone(),
                key: key.clone(),
            },
            S3ErrorCode::NoSuchKey,
        ),
        (
            StorageError::BucketNotEmpty {
                bucket: bucket.clone(),
            },
            S3ErrorCode::BucketNotEmpty,
        ),
        (
            StorageError::InvalidObjectKey { key: String::new() },
            S3ErrorCode::InvalidArgument,
        ),
    ];

    for (storage_error, expected_code) in cases {
        let error = S3Error::from_storage_error(
            &storage_error,
            "/example-bucket/missing.txt",
            S3RequestId::new(TEST_REQUEST_ID),
        );

        assert_eq!(error.code, expected_code);
        assert_eq!(error.resource, "/example-bucket/missing.txt");
        assert_eq!(error.request_id.as_str(), TEST_REQUEST_ID);
    }
}

#[test]
fn invalid_storage_argument_conversion_keeps_actionable_message() {
    let error = S3Error::from_storage_error(
        &StorageError::InvalidObjectKey { key: String::new() },
        "/example-bucket/",
        S3RequestId::new(TEST_REQUEST_ID),
    );

    assert_eq!(error.code, S3ErrorCode::InvalidArgument);
    assert_eq!(error.message, "invalid object key: ");
}

#[test]
fn internal_storage_error_conversion_uses_generic_message() {
    let error = S3Error::from_storage_error(
        &StorageError::Io {
            path: PathBuf::from("C:\\private\\bucket\\metadata.json"),
            source: std::io::Error::other("disk failure at private path"),
        },
        "/example-bucket",
        S3RequestId::new(TEST_REQUEST_ID),
    );

    assert_eq!(error.code, S3ErrorCode::InternalError);
    assert_eq!(
        error.message,
        "We encountered an internal error. Please try again."
    );
    assert!(!error.message.contains("C:\\private"));
    assert!(!error.message.contains("disk failure"));
    assert_eq!(error.resource, "/example-bucket");
    assert_eq!(error.request_id.as_str(), TEST_REQUEST_ID);
}

#[test]
fn method_not_allowed_code_string_is_available() {
    assert_eq!(S3ErrorCode::MethodNotAllowed.as_str(), "MethodNotAllowed");
}
