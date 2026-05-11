// SPDX-License-Identifier: Apache-2.0

pub const STATIC_REQUEST_ID: &str = "s3lab-test-request-id";

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum S3ErrorCode {
    AccessDenied,
    AuthorizationHeaderMalformed,
    BadDigest,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    EntityTooLarge,
    InvalidAccessKeyId,
    InternalError,
    InvalidArgument,
    InvalidBucketName,
    MethodNotAllowed,
    NoSuchBucket,
    NoSuchKey,
    NotImplemented,
    SignatureDoesNotMatch,
    XAmzContentSHA256Mismatch,
}

impl S3ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::AuthorizationHeaderMalformed => "AuthorizationHeaderMalformed",
            Self::BadDigest => "BadDigest",
            Self::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::EntityTooLarge => "EntityTooLarge",
            Self::InvalidAccessKeyId => "InvalidAccessKeyId",
            Self::InternalError => "InternalError",
            Self::InvalidArgument => "InvalidArgument",
            Self::InvalidBucketName => "InvalidBucketName",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::NotImplemented => "NotImplemented",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::XAmzContentSHA256Mismatch => "XAmzContentSHA256Mismatch",
        }
    }

    pub fn default_message(&self) -> &'static str {
        match self {
            Self::AccessDenied => "Access denied.",
            Self::AuthorizationHeaderMalformed => "The authorization header is malformed.",
            Self::BadDigest => "The Content-MD5 you specified did not match what we received.",
            Self::NoSuchBucket => "The specified bucket does not exist.",
            Self::NoSuchKey => "The specified key does not exist.",
            Self::BucketAlreadyOwnedByYou => {
                "Your previous request to create the named bucket succeeded and you already own it."
            }
            Self::BucketNotEmpty => "The bucket you tried to delete is not empty.",
            Self::EntityTooLarge => "Your proposed upload exceeds the maximum allowed object size.",
            Self::InvalidAccessKeyId => {
                "The AWS access key ID you provided does not exist in S3Lab."
            }
            Self::InvalidBucketName => "The specified bucket is not valid.",
            Self::InvalidArgument => "Invalid argument.",
            Self::MethodNotAllowed => "The specified method is not allowed against this resource.",
            Self::NotImplemented => {
                "A header you provided implies functionality that is not implemented."
            }
            Self::SignatureDoesNotMatch => {
                "The request signature we calculated does not match the signature you provided."
            }
            Self::XAmzContentSHA256Mismatch => {
                "The x-amz-content-sha256 header value did not match the request body."
            }
            Self::InternalError => "We encountered an internal error. Please try again.",
        }
    }

    pub fn http_status_code(&self) -> u16 {
        match self {
            Self::AccessDenied | Self::InvalidAccessKeyId | Self::SignatureDoesNotMatch => 403,
            Self::AuthorizationHeaderMalformed => 400,
            Self::BadDigest => 400,
            Self::NoSuchBucket | Self::NoSuchKey => 404,
            Self::BucketAlreadyOwnedByYou | Self::BucketNotEmpty => 409,
            Self::EntityTooLarge => 400,
            Self::InvalidBucketName | Self::InvalidArgument => 400,
            Self::MethodNotAllowed => 405,
            Self::NotImplemented => 501,
            Self::XAmzContentSHA256Mismatch => 400,
            Self::InternalError => 500,
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
}

#[cfg(test)]
mod tests {
    use super::{S3Error, S3ErrorCode, S3RequestId, STATIC_REQUEST_ID};

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
                S3ErrorCode::AccessDenied,
                S3ErrorCode::AuthorizationHeaderMalformed,
                S3ErrorCode::BucketAlreadyOwnedByYou,
                S3ErrorCode::BadDigest,
                S3ErrorCode::BucketNotEmpty,
                S3ErrorCode::EntityTooLarge,
                S3ErrorCode::InvalidAccessKeyId,
                S3ErrorCode::InternalError,
                S3ErrorCode::InvalidArgument,
                S3ErrorCode::InvalidBucketName,
                S3ErrorCode::MethodNotAllowed,
                S3ErrorCode::NoSuchBucket,
                S3ErrorCode::NoSuchKey,
                S3ErrorCode::SignatureDoesNotMatch,
                S3ErrorCode::XAmzContentSHA256Mismatch,
            ]
            .len(),
            15
        );
    }

    #[test]
    fn error_code_strings_match_s3_names() {
        assert_eq!(S3ErrorCode::MethodNotAllowed.as_str(), "MethodNotAllowed");
    }

    #[test]
    fn error_codes_map_to_http_status_codes() {
        let cases = [
            (S3ErrorCode::AccessDenied, 403),
            (S3ErrorCode::AuthorizationHeaderMalformed, 400),
            (S3ErrorCode::NoSuchBucket, 404),
            (S3ErrorCode::NoSuchKey, 404),
            (S3ErrorCode::BadDigest, 400),
            (S3ErrorCode::BucketAlreadyOwnedByYou, 409),
            (S3ErrorCode::BucketNotEmpty, 409),
            (S3ErrorCode::EntityTooLarge, 400),
            (S3ErrorCode::InvalidAccessKeyId, 403),
            (S3ErrorCode::InvalidBucketName, 400),
            (S3ErrorCode::InvalidArgument, 400),
            (S3ErrorCode::MethodNotAllowed, 405),
            (S3ErrorCode::NotImplemented, 501),
            (S3ErrorCode::SignatureDoesNotMatch, 403),
            (S3ErrorCode::XAmzContentSHA256Mismatch, 400),
            (S3ErrorCode::InternalError, 500),
        ];

        for (code, expected) in cases {
            assert_eq!(code.http_status_code(), expected);
        }
    }

    #[test]
    fn new_error_uses_default_message() {
        let error = S3Error::new(
            S3ErrorCode::NoSuchBucket,
            "/missing-bucket",
            S3RequestId::new(STATIC_REQUEST_ID),
        );

        assert_eq!(error.code, S3ErrorCode::NoSuchBucket);
        assert_eq!(error.message, "The specified bucket does not exist.");
        assert_eq!(error.resource, "/missing-bucket");
        assert_eq!(error.request_id.as_str(), STATIC_REQUEST_ID);
    }
}
