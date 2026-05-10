// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct BucketName(String);

impl BucketName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::BucketName;

    #[test]
    fn new_stores_bucket_name_verbatim() {
        let bucket = BucketName::new("example-bucket");

        assert_eq!(bucket.as_str(), "example-bucket");
    }

    #[test]
    fn bucket_names_sort_lexicographically() {
        let mut buckets = [
            BucketName::new("z-bucket"),
            BucketName::new("a-bucket"),
            BucketName::new("m-bucket"),
        ];

        buckets.sort();

        assert_eq!(
            buckets.map(|bucket| bucket.as_str().to_owned()),
            ["a-bucket", "m-bucket", "z-bucket"]
        );
    }

    #[test]
    fn bucket_name_can_be_cloned_without_changing_value() {
        let bucket = BucketName::new("example-bucket");

        assert_eq!(bucket.clone(), bucket);
    }
}
