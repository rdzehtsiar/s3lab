// SPDX-License-Identifier: Apache-2.0

pub mod fs;
pub mod key;

use crate::s3::bucket::BucketName;
use crate::s3::error::S3ErrorCode;
use crate::s3::object::ObjectKey;
use std::collections::BTreeMap;
use thiserror::Error;
use time::OffsetDateTime;

pub const STORAGE_ROOT_DIR: &str = "buckets";

pub trait Storage {
    fn create_bucket(&self, bucket: &BucketName) -> Result<(), StorageError>;

    fn list_buckets(&self) -> Result<Vec<BucketSummary>, StorageError>;

    fn bucket_exists(&self, bucket: &BucketName) -> Result<bool, StorageError>;

    fn delete_bucket(&self, bucket: &BucketName) -> Result<(), StorageError>;

    fn put_object(&self, request: PutObjectRequest) -> Result<StoredObjectMetadata, StorageError>;

    fn get_object_metadata(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObjectMetadata, StorageError>;

    fn get_object_bytes(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<u8>, StorageError>;

    fn list_objects(
        &self,
        bucket: &BucketName,
        options: ListObjectsOptions,
    ) -> Result<ObjectListing, StorageError>;

    fn delete_object(&self, bucket: &BucketName, key: &ObjectKey) -> Result<(), StorageError>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BucketSummary {
    pub name: BucketName,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PutObjectRequest {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub user_metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StoredObjectMetadata {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub etag: String,
    pub content_length: u64,
    pub content_type: Option<String>,
    pub last_modified: OffsetDateTime,
    pub user_metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ObjectListing {
    pub bucket: BucketName,
    pub objects: Vec<StoredObjectMetadata>,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ListObjectsOptions {
    pub prefix: Option<ObjectKey>,
    pub continuation_token: Option<String>,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("bucket already exists: {bucket}")]
    BucketAlreadyExists { bucket: BucketName },

    #[error("bucket is not empty: {bucket}")]
    BucketNotEmpty { bucket: BucketName },

    #[error("bucket does not exist: {bucket}")]
    NoSuchBucket { bucket: BucketName },

    #[error("object key does not exist in bucket {bucket}: {key}")]
    NoSuchKey { bucket: BucketName, key: ObjectKey },

    #[error("invalid bucket name: {bucket}")]
    InvalidBucketName { bucket: String },

    #[error("invalid object key: {key}")]
    InvalidObjectKey { key: String },

    #[error("invalid storage argument: {message}")]
    InvalidArgument { message: String },

    #[error("storage I/O error at {path}: {source}")]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl From<&StorageError> for S3ErrorCode {
    fn from(error: &StorageError) -> Self {
        match error {
            StorageError::BucketAlreadyExists { .. } => Self::BucketAlreadyOwnedByYou,
            StorageError::BucketNotEmpty { .. } => Self::BucketNotEmpty,
            StorageError::NoSuchBucket { .. } => Self::NoSuchBucket,
            StorageError::NoSuchKey { .. } => Self::NoSuchKey,
            StorageError::InvalidBucketName { .. } => Self::InvalidBucketName,
            StorageError::InvalidObjectKey { .. } | StorageError::InvalidArgument { .. } => {
                Self::InvalidArgument
            }
            StorageError::Io { .. } => Self::InternalError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ListObjectsOptions, ObjectListing, PutObjectRequest, StorageError, StoredObjectMetadata,
    };
    use crate::s3::bucket::BucketName;
    use crate::s3::error::S3ErrorCode;
    use crate::s3::object::ObjectKey;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

    #[test]
    fn list_objects_options_default_has_no_filters_or_token() {
        assert_eq!(
            ListObjectsOptions::default(),
            ListObjectsOptions {
                prefix: None,
                continuation_token: None,
            }
        );
    }

    #[test]
    fn object_metadata_preserves_contract_fields_and_sorted_user_metadata() {
        let mut user_metadata = BTreeMap::new();
        user_metadata.insert("z-key".to_owned(), "last".to_owned());
        user_metadata.insert("a-key".to_owned(), "first".to_owned());

        let metadata = StoredObjectMetadata {
            bucket: BucketName::new("example-bucket"),
            key: ObjectKey::new("prefix/example.txt"),
            etag: "\"5d41402abc4b2a76b9719d911017c592\"".to_owned(),
            content_length: 11,
            content_type: Some("text/plain".to_owned()),
            last_modified: fixed_last_modified(),
            user_metadata,
        };

        assert_eq!(metadata.bucket.as_str(), "example-bucket");
        assert_eq!(metadata.key.as_str(), "prefix/example.txt");
        assert_eq!(metadata.etag, "\"5d41402abc4b2a76b9719d911017c592\"");
        assert_eq!(metadata.content_length, 11);
        assert_eq!(metadata.content_type.as_deref(), Some("text/plain"));
        assert_eq!(
            metadata.user_metadata.keys().collect::<Vec<_>>(),
            vec!["a-key", "z-key"]
        );
    }

    #[test]
    fn put_object_request_preserves_bytes_and_metadata() {
        let request = PutObjectRequest {
            bucket: BucketName::new("example-bucket"),
            key: ObjectKey::new("example.txt"),
            bytes: b"hello".to_vec(),
            content_type: Some("text/plain".to_owned()),
            user_metadata: BTreeMap::from([("owner".to_owned(), "local".to_owned())]),
        };

        assert_eq!(request.bucket.as_str(), "example-bucket");
        assert_eq!(request.key.as_str(), "example.txt");
        assert_eq!(request.bytes, b"hello");
        assert_eq!(request.content_type.as_deref(), Some("text/plain"));
        assert_eq!(request.user_metadata["owner"], "local");
    }

    #[test]
    fn object_listing_preserves_bucket_objects_and_continuation_token() {
        let listing = ObjectListing {
            bucket: BucketName::new("example-bucket"),
            objects: Vec::new(),
            next_continuation_token: Some("token-1".to_owned()),
        };

        assert_eq!(listing.bucket.as_str(), "example-bucket");
        assert!(listing.objects.is_empty());
        assert_eq!(listing.next_continuation_token.as_deref(), Some("token-1"));
    }

    #[test]
    fn storage_error_messages_include_actionable_names() {
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("missing.txt");

        let bucket_error = StorageError::NoSuchBucket {
            bucket: bucket.clone(),
        };
        let key_error = StorageError::NoSuchKey { bucket, key };

        assert!(bucket_error.to_string().contains("example-bucket"));
        assert!(key_error.to_string().contains("example-bucket"));
        assert!(key_error.to_string().contains("missing.txt"));
    }

    #[test]
    fn storage_errors_map_to_s3_error_codes() {
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("missing.txt");

        let cases = [
            (
                StorageError::BucketAlreadyExists {
                    bucket: bucket.clone(),
                },
                S3ErrorCode::BucketAlreadyOwnedByYou,
            ),
            (
                StorageError::BucketNotEmpty {
                    bucket: bucket.clone(),
                },
                S3ErrorCode::BucketNotEmpty,
            ),
            (
                StorageError::NoSuchBucket {
                    bucket: bucket.clone(),
                },
                S3ErrorCode::NoSuchBucket,
            ),
            (
                StorageError::NoSuchKey {
                    bucket: bucket.clone(),
                    key,
                },
                S3ErrorCode::NoSuchKey,
            ),
            (
                StorageError::InvalidBucketName {
                    bucket: "bad_bucket".to_owned(),
                },
                S3ErrorCode::InvalidBucketName,
            ),
            (
                StorageError::InvalidObjectKey { key: String::new() },
                S3ErrorCode::InvalidArgument,
            ),
            (
                StorageError::InvalidArgument {
                    message: "bad continuation token".to_owned(),
                },
                S3ErrorCode::InvalidArgument,
            ),
            (
                StorageError::Io {
                    path: PathBuf::from("metadata.json"),
                    source: std::io::Error::new(std::io::ErrorKind::Other, "disk error"),
                },
                S3ErrorCode::InternalError,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(S3ErrorCode::from(&error), expected);
        }
    }

    fn fixed_last_modified() -> OffsetDateTime {
        PrimitiveDateTime::new(
            Date::from_calendar_date(2026, Month::May, 10).expect("valid test date"),
            Time::MIDNIGHT,
        )
        .assume_utc()
    }
}
