// SPDX-License-Identifier: Apache-2.0

use crate::s3::bucket::BucketName;
use crate::s3::object::ObjectKey;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ListObjectsEncoding {
    Url,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum S3Operation {
    ListBuckets,
    CreateBucket {
        bucket: BucketName,
    },
    HeadBucket {
        bucket: BucketName,
    },
    DeleteBucket {
        bucket: BucketName,
    },
    PutObject {
        bucket: BucketName,
        key: ObjectKey,
    },
    GetObject {
        bucket: BucketName,
        key: ObjectKey,
    },
    HeadObject {
        bucket: BucketName,
        key: ObjectKey,
    },
    DeleteObject {
        bucket: BucketName,
        key: ObjectKey,
    },
    CreateMultipartUpload {
        bucket: BucketName,
        key: ObjectKey,
    },
    UploadPart {
        bucket: BucketName,
        key: ObjectKey,
        upload_id: String,
        part_number: u32,
    },
    ListParts {
        bucket: BucketName,
        key: ObjectKey,
        upload_id: String,
    },
    CompleteMultipartUpload {
        bucket: BucketName,
        key: ObjectKey,
        upload_id: String,
    },
    AbortMultipartUpload {
        bucket: BucketName,
        key: ObjectKey,
        upload_id: String,
    },
    ListObjectsV2 {
        bucket: BucketName,
        prefix: Option<ObjectKey>,
        delimiter: Option<String>,
        continuation_token: Option<String>,
        max_keys: usize,
        encoding: Option<ListObjectsEncoding>,
    },
}

#[cfg(test)]
mod tests {
    use super::S3Operation;
    use crate::s3::bucket::BucketName;
    use crate::s3::object::ObjectKey;

    #[test]
    fn object_operations_preserve_owned_route_values() {
        let operation = S3Operation::GetObject {
            bucket: BucketName::new("example-bucket"),
            key: ObjectKey::new("nested/object.txt"),
        };

        assert_eq!(
            operation.clone(),
            S3Operation::GetObject {
                bucket: BucketName::new("example-bucket"),
                key: ObjectKey::new("nested/object.txt"),
            }
        );
    }

    #[test]
    fn debug_output_names_operation() {
        assert_eq!(format!("{:?}", S3Operation::ListBuckets), "ListBuckets");
    }
}
