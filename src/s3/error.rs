// SPDX-License-Identifier: Apache-2.0

use crate::storage::StorageError;

pub const TEST_REQUEST_ID: &str = "s3lab-test-request-id";

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum S3ErrorCode {
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    InternalError,
    InvalidArgument,
    InvalidBucketName,
    MethodNotAllowed,
    NoSuchBucket,
    NoSuchKey,
    NotImplemented,
}

impl S3ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::InternalError => "InternalError",
            Self::InvalidArgument => "InvalidArgument",
            Self::InvalidBucketName => "InvalidBucketName",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::NotImplemented => "NotImplemented",
        }
    }

    pub fn default_message(&self) -> &'static str {
        match self {
            Self::NoSuchBucket => "The specified bucket does not exist.",
            Self::NoSuchKey => "The specified key does not exist.",
            Self::BucketAlreadyOwnedByYou => {
                "Your previous request to create the named bucket succeeded and you already own it."
            }
            Self::BucketNotEmpty => "The bucket you tried to delete is not empty.",
            Self::InvalidBucketName => "The specified bucket is not valid.",
            Self::InvalidArgument => "Invalid argument.",
            Self::MethodNotAllowed => "The specified method is not allowed against this resource.",
            Self::NotImplemented => {
                "A header you provided implies functionality that is not implemented."
            }
            Self::InternalError => "We encountered an internal error. Please try again.",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct S3RequestId(String);

impl S3RequestId {
    pub fn new(request_id: impl Into<String>) -> Self {
        Self(request_id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct S3Error {
    pub code: S3ErrorCode,
    pub message: String,
    pub resource: String,
    pub request_id: S3RequestId,
}

impl S3Error {
    pub fn new(code: S3ErrorCode, resource: impl Into<String>, request_id: S3RequestId) -> Self {
        let message = code.default_message().to_owned();

        Self {
            code,
            message,
            resource: resource.into(),
            request_id,
        }
    }

    pub fn with_message(
        code: S3ErrorCode,
        message: impl Into<String>,
        resource: impl Into<String>,
        request_id: S3RequestId,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            resource: resource.into(),
            request_id,
        }
    }

    pub fn from_storage_error(
        error: &StorageError,
        resource: impl Into<String>,
        request_id: S3RequestId,
    ) -> Self {
        let code = S3ErrorCode::from(error);
        let resource = resource.into();

        match code {
            S3ErrorCode::InternalError => Self::new(code, resource, request_id),
            S3ErrorCode::InvalidArgument => {
                Self::with_message(code, error.to_string(), resource, request_id)
            }
            _ => Self::new(code, resource, request_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{S3Error, S3ErrorCode, S3RequestId, TEST_REQUEST_ID};

    #[test]
    fn not_implemented_error_code_is_comparable() {
        assert_eq!(S3ErrorCode::NotImplemented, S3ErrorCode::NotImplemented);
    }

    #[test]
    fn not_implemented_error_code_can_be_cloned() {
        let code = S3ErrorCode::NotImplemented;

        assert_eq!(code.clone(), code);
    }

    #[test]
    fn debug_output_names_error_code() {
        assert_eq!(
            format!("{:?}", S3ErrorCode::NotImplemented),
            "NotImplemented"
        );
    }

    #[test]
    fn storage_related_error_codes_are_available() {
        assert_eq!(
            [
                S3ErrorCode::BucketAlreadyOwnedByYou,
                S3ErrorCode::BucketNotEmpty,
                S3ErrorCode::InternalError,
                S3ErrorCode::InvalidArgument,
                S3ErrorCode::InvalidBucketName,
                S3ErrorCode::MethodNotAllowed,
                S3ErrorCode::NoSuchBucket,
                S3ErrorCode::NoSuchKey,
            ]
            .len(),
            8
        );
    }

    #[test]
    fn error_code_strings_match_s3_names() {
        assert_eq!(S3ErrorCode::MethodNotAllowed.as_str(), "MethodNotAllowed");
    }

    #[test]
    fn new_error_uses_default_message() {
        let error = S3Error::new(
            S3ErrorCode::NoSuchBucket,
            "/missing-bucket",
            S3RequestId::new(TEST_REQUEST_ID),
        );

        assert_eq!(error.code, S3ErrorCode::NoSuchBucket);
        assert_eq!(error.message, "The specified bucket does not exist.");
        assert_eq!(error.resource, "/missing-bucket");
        assert_eq!(error.request_id.as_str(), TEST_REQUEST_ID);
    }
}
