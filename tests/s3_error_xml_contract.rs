// SPDX-License-Identifier: Apache-2.0

use s3lab::s3::error::{S3Error, S3ErrorCode, S3RequestId, STATIC_REQUEST_ID};
use s3lab::s3::xml::{
    error_response_xml, list_buckets_response_xml, list_objects_v2_response_xml, ListBucketXml,
    ListBucketsXml, ListObjectXml, ListObjectsV2Xml, ListObjectsV2XmlEntry,
};
use time::OffsetDateTime;

#[test]
fn no_such_bucket_error_xml_has_stable_field_order() {
    let error = S3Error::new(
        S3ErrorCode::NoSuchBucket,
        "/missing-bucket",
        S3RequestId::new(STATIC_REQUEST_ID),
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
        S3RequestId::new(STATIC_REQUEST_ID),
    );

    assert_eq!(
        error_response_xml(&error),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidArgument</Code><Message>bad &amp; unsupported &lt;header&gt; value &gt; limit</Message><Resource>/bucket/key?a=1&amp;b=&lt;value&gt;</Resource><RequestId>s3lab-test-request-id</RequestId></Error>"
    );
}

#[test]
fn list_buckets_xml_has_stable_order_and_escapes_bucket_names() {
    let listing = ListBucketsXml {
        buckets: vec![
            ListBucketXml {
                name: "a&b".to_owned(),
            },
            ListBucketXml {
                name: "z<bucket>".to_owned(),
            },
        ],
    };

    assert_eq!(
        list_buckets_response_xml(&listing),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult><Buckets><Bucket><Name>a&amp;b</Name></Bucket><Bucket><Name>z&lt;bucket&gt;</Name></Bucket></Buckets></ListAllMyBucketsResult>"
    );
}

#[test]
fn list_objects_v2_xml_represents_empty_prefixed_listing() {
    let listing = ListObjectsV2Xml {
        bucket: "empty-bucket".to_owned(),
        entries: Vec::new(),
        max_keys: 1000,
        is_truncated: false,
        next_continuation_token: None,
    };

    assert_eq!(
        list_objects_v2_response_xml(&listing, Some("logs/&<today>"), None, None, None),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>empty-bucket</Name><Prefix>logs/&amp;&lt;today&gt;</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>"
    );
}

#[test]
fn list_objects_v2_xml_preserves_object_order_sizes_and_truncated_token() {
    let listing = ListObjectsV2Xml {
        bucket: "example-bucket".to_owned(),
        entries: vec![
            ListObjectsV2XmlEntry::Object(object_xml("photos/a&b.txt", 11)),
            ListObjectsV2XmlEntry::Object(object_xml("photos/z<last>.txt", 42)),
        ],
        max_keys: 2,
        is_truncated: true,
        next_continuation_token: Some("next&page<2>".to_owned()),
    };

    assert_eq!(
        list_objects_v2_response_xml(&listing, None, None, None, None),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>example-bucket</Name><Prefix></Prefix><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>photos/a&amp;b.txt</Key><LastModified>1970-01-01T00:00:00.000Z</LastModified><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>11</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>photos/z&lt;last&gt;.txt</Key><LastModified>1970-01-01T00:00:00.000Z</LastModified><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>42</Size><StorageClass>STANDARD</StorageClass></Contents><NextContinuationToken>next&amp;page&lt;2&gt;</NextContinuationToken></ListBucketResult>"
    );
}

#[test]
fn list_objects_v2_xml_echoes_request_continuation_token() {
    let listing = ListObjectsV2Xml {
        bucket: "example-bucket".to_owned(),
        entries: vec![ListObjectsV2XmlEntry::Object(object_xml(
            "photos/a.txt",
            11,
        ))],
        max_keys: 1000,
        is_truncated: false,
        next_continuation_token: Some("ignored-token".to_owned()),
    };

    assert_eq!(
        list_objects_v2_response_xml(&listing, None, None, Some("page&2<now>"), None),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>example-bucket</Name><Prefix></Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><ContinuationToken>page&amp;2&lt;now&gt;</ContinuationToken><IsTruncated>false</IsTruncated><Contents><Key>photos/a.txt</Key><LastModified>1970-01-01T00:00:00.000Z</LastModified><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>11</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"
    );
}

#[test]
fn list_objects_v2_xml_escapes_carriage_return_as_character_reference() {
    let listing = ListObjectsV2Xml {
        bucket: "example-bucket".to_owned(),
        entries: vec![ListObjectsV2XmlEntry::Object(object_xml(
            "logs/\robject.txt",
            4,
        ))],
        max_keys: 1000,
        is_truncated: false,
        next_continuation_token: None,
    };

    assert_eq!(
        list_objects_v2_response_xml(&listing, Some("logs/\r"), None, None, None),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>example-bucket</Name><Prefix>logs/&#13;</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>logs/&#13;object.txt</Key><LastModified>1970-01-01T00:00:00.000Z</LastModified><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>4</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"
    );
}

#[test]
fn list_objects_v2_xml_writes_delimiter_common_prefixes_and_key_count() {
    let listing = ListObjectsV2Xml {
        bucket: "example-bucket".to_owned(),
        entries: vec![
            ListObjectsV2XmlEntry::Object(object_xml("a.txt", 1)),
            ListObjectsV2XmlEntry::CommonPrefix("photos/&<raw>/".to_owned()),
        ],
        max_keys: 2,
        is_truncated: false,
        next_continuation_token: None,
    };

    assert_eq!(
        list_objects_v2_response_xml(&listing, None, Some("/"), None, None),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>example-bucket</Name><Prefix></Prefix><Delimiter>/</Delimiter><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>a.txt</Key><LastModified>1970-01-01T00:00:00.000Z</LastModified><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents><CommonPrefixes><Prefix>photos/&amp;&lt;raw&gt;/</Prefix></CommonPrefixes></ListBucketResult>"
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
fn method_not_allowed_code_string_is_available() {
    assert_eq!(S3ErrorCode::MethodNotAllowed.as_str(), "MethodNotAllowed");
}

fn object_xml(key: &str, content_length: u64) -> ListObjectXml {
    ListObjectXml {
        key: key.to_owned(),
        etag: "\"d41d8cd98f00b204e9800998ecf8427e\"".to_owned(),
        content_length,
        last_modified: OffsetDateTime::UNIX_EPOCH,
    }
}
