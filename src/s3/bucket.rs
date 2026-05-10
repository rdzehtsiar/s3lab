// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct BucketName(String);

impl BucketName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_valid_s3_name(&self) -> bool {
        is_valid_s3_bucket_name(self.as_str())
    }
}

pub fn is_valid_s3_bucket_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if !(3..=63).contains(&bytes.len()) {
        return false;
    }

    if has_reserved_prefix_or_suffix(name) {
        return false;
    }

    if !is_ascii_lowercase_letter_or_digit(bytes[0])
        || !is_ascii_lowercase_letter_or_digit(bytes[bytes.len() - 1])
    {
        return false;
    }

    if !bytes
        .iter()
        .all(|byte| is_ascii_lowercase_letter_or_digit(*byte) || matches!(*byte, b'.' | b'-'))
    {
        return false;
    }

    if bytes
        .windows(2)
        .any(|pair| matches!(pair, b".." | b".-" | b"-."))
    {
        return false;
    }

    !is_ipv4_address_like(name)
}

fn has_reserved_prefix_or_suffix(name: &str) -> bool {
    const RESERVED_PREFIXES: &[&str] = &["xn--", "sthree-", "amzn-s3-demo-"];
    const RESERVED_SUFFIXES: &[&str] = &["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"];

    RESERVED_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
        || RESERVED_SUFFIXES
            .iter()
            .any(|suffix| name.ends_with(suffix))
}

fn is_ascii_lowercase_letter_or_digit(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

fn is_ipv4_address_like(name: &str) -> bool {
    let mut parts = name.split('.');
    let Some(first) = parts.next() else {
        return false;
    };

    let mut count = 1;
    if !is_ipv4_octet_like(first) {
        return false;
    }

    for part in parts {
        count += 1;
        if !is_ipv4_octet_like(part) {
            return false;
        }
    }

    count == 4
}

fn is_ipv4_octet_like(part: &str) -> bool {
    !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()) && part.parse::<u8>().is_ok()
}

impl Display for BucketName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
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
    fn practical_s3_bucket_names_are_valid() {
        for name in [
            "abc",
            "example-bucket",
            "example.bucket",
            "a1-bucket.2",
            "a2345678901234567890123456789012345678901234567890123456789012",
        ] {
            assert!(
                BucketName::new(name).is_valid_s3_name(),
                "bucket should be valid: {name}"
            );
        }
    }

    #[test]
    fn practical_s3_bucket_names_reject_invalid_shapes() {
        for name in [
            "",
            "ab",
            "a234567890123456789012345678901234567890123456789012345678901234",
            "Uppercase",
            "has_underscore",
            "-starts-with-hyphen",
            ".starts-with-dot",
            "ends-with-hyphen-",
            "ends-with-dot.",
            "adjacent..dots",
            "dot.-hyphen",
            "hyphen-.dot",
            "192.168.0.1",
            "slash/name",
        ] {
            assert!(
                !BucketName::new(name).is_valid_s3_name(),
                "bucket should be invalid: {name}"
            );
        }
    }

    #[test]
    fn practical_s3_bucket_names_reject_reserved_prefixes_and_suffixes() {
        for name in [
            "xn--bucket",
            "sthree-bucket",
            "amzn-s3-demo-bucket",
            "bucket-s3alias",
            "bucket--ol-s3",
            "bucket.mrap",
            "bucket--x-s3",
            "bucket--table-s3",
        ] {
            assert!(
                !BucketName::new(name).is_valid_s3_name(),
                "bucket should be invalid: {name}"
            );
        }
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

    #[test]
    fn bucket_name_displays_inner_value() {
        assert_eq!(
            BucketName::new("example-bucket").to_string(),
            "example-bucket"
        );
    }
}
