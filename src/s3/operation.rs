// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum S3Operation {
    ListBuckets,
    CreateBucket {
        bucket: String,
    },
    HeadBucket {
        bucket: String,
    },
    DeleteBucket {
        bucket: String,
    },
    PutObject {
        bucket: String,
        key: String,
    },
    GetObject {
        bucket: String,
        key: String,
    },
    HeadObject {
        bucket: String,
        key: String,
    },
    DeleteObject {
        bucket: String,
        key: String,
    },
    ListObjectsV2 {
        bucket: String,
        prefix: Option<String>,
        continuation_token: Option<String>,
        max_keys: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::S3Operation;

    #[test]
    fn object_operations_preserve_owned_route_values() {
        let operation = S3Operation::GetObject {
            bucket: "example-bucket".to_owned(),
            key: "nested/object.txt".to_owned(),
        };

        assert_eq!(
            operation.clone(),
            S3Operation::GetObject {
                bucket: "example-bucket".to_owned(),
                key: "nested/object.txt".to_owned(),
            }
        );
    }

    #[test]
    fn debug_output_names_operation() {
        assert_eq!(format!("{:?}", S3Operation::ListBuckets), "ListBuckets");
    }
}
