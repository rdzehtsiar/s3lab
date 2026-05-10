// SPDX-License-Identifier: Apache-2.0

use super::StorageError;
use crate::s3::bucket::BucketName;
use crate::s3::object::ObjectKey;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EncodedBucketName {
    path_component: String,
}

impl EncodedBucketName {
    pub fn as_path_component(&self) -> &str {
        &self.path_component
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EncodedObjectKey {
    shard: String,
    path_component: String,
}

impl EncodedObjectKey {
    pub fn shard(&self) -> &str {
        &self.shard
    }

    pub fn as_path_component(&self) -> &str {
        &self.path_component
    }
}

pub fn raw_keys_are_not_filesystem_paths() -> bool {
    true
}

pub fn encode_bucket_name(bucket: &BucketName) -> EncodedBucketName {
    EncodedBucketName {
        path_component: format!("bucket-{}", sha256_lower_hex(bucket.as_str())),
    }
}

pub fn encode_object_key(key: &ObjectKey) -> Result<EncodedObjectKey, StorageError> {
    if key.as_str().is_empty() {
        return Err(StorageError::InvalidObjectKey {
            key: key.as_str().to_owned(),
        });
    }

    let digest = sha256_lower_hex(key.as_str());
    Ok(EncodedObjectKey {
        shard: digest[..2].to_owned(),
        path_component: format!("key-{digest}"),
    })
}

fn sha256_lower_hex(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::{encode_bucket_name, encode_object_key, raw_keys_are_not_filesystem_paths};
    use crate::s3::bucket::BucketName;
    use crate::s3::object::ObjectKey;

    #[test]
    fn storage_key_policy_rejects_raw_path_mapping() {
        assert!(raw_keys_are_not_filesystem_paths());
    }

    #[test]
    fn bucket_encoding_is_deterministic_and_distinguishes_names() {
        let first = encode_bucket_name(&BucketName::new("example-bucket"));
        let second = encode_bucket_name(&BucketName::new("example-bucket"));
        let different = encode_bucket_name(&BucketName::new("other-bucket"));

        assert_eq!(first, second);
        assert_ne!(first, different);
        assert!(first.as_path_component().starts_with("bucket-"));
        assert_eq!(first.as_path_component().len(), "bucket-".len() + 64);
    }

    #[test]
    fn object_encoding_is_deterministic_and_distinguishes_keys() {
        let first = encode_object_key(&ObjectKey::new("prefix/example.txt")).expect("valid key");
        let second = encode_object_key(&ObjectKey::new("prefix/example.txt")).expect("valid key");
        let different = encode_object_key(&ObjectKey::new("prefix/other.txt")).expect("valid key");

        assert_eq!(first, second);
        assert_ne!(first, different);
        assert!(first.as_path_component().starts_with("key-"));
        assert_eq!(first.as_path_component().len(), "key-".len() + 64);
        assert_eq!(first.shard().len(), 2);
    }

    #[test]
    fn encoded_path_components_are_not_raw_or_reserved_names() {
        let bucket = encode_bucket_name(&BucketName::new("con"));
        let object = encode_object_key(&ObjectKey::new("../con/aux/file.txt")).expect("valid key");

        for component in [
            bucket.as_path_component(),
            object.shard(),
            object.as_path_component(),
        ] {
            assert!(!component.contains('/'));
            assert!(!component.contains('\\'));
            assert_ne!(component, ".");
            assert_ne!(component, "..");
            assert_ne!(component, "con");
            assert_ne!(component, "aux");
            assert!(!component.contains("../con/aux/file.txt"));
        }
    }

    #[test]
    fn traversal_looking_keys_do_not_appear_as_path_segments() {
        let encoded =
            encode_object_key(&ObjectKey::new("../nested/../../escape.txt")).expect("valid key");

        assert_ne!(encoded.shard(), "..");
        assert_ne!(encoded.as_path_component(), "..");
        assert!(!encoded.as_path_component().contains("nested"));
        assert!(!encoded.as_path_component().contains("escape.txt"));
    }

    #[test]
    fn empty_object_key_is_rejected_before_path_encoding() {
        let error = encode_object_key(&ObjectKey::new("")).expect_err("empty key is invalid");

        assert!(error.to_string().contains("invalid object key"));
    }
}
