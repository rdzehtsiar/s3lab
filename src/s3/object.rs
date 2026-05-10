// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectKey(String);

pub const MAX_OBJECT_KEY_UTF8_BYTES: usize = 1024;

impl ObjectKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_valid_s3_key(&self) -> bool {
        is_valid_s3_object_key(self.as_str())
    }
}

pub fn is_valid_s3_object_key(key: &str) -> bool {
    !key.is_empty() && is_valid_s3_object_key_prefix(key)
}

pub fn is_valid_s3_object_key_prefix(prefix: &str) -> bool {
    prefix.len() <= MAX_OBJECT_KEY_UTF8_BYTES && prefix.chars().all(is_xml_compatible_char)
}

fn is_xml_compatible_char(character: char) -> bool {
    matches!(
        character,
        '\u{9}' | '\u{A}' | '\u{D}' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}'
    )
}

impl Display for ObjectKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::{is_valid_s3_object_key_prefix, ObjectKey, MAX_OBJECT_KEY_UTF8_BYTES};

    #[test]
    fn new_stores_object_key_verbatim() {
        let key = ObjectKey::new("prefix/example.txt");

        assert_eq!(key.as_str(), "prefix/example.txt");
    }

    #[test]
    fn object_keys_sort_lexicographically() {
        let mut keys = [
            ObjectKey::new("z.txt"),
            ObjectKey::new("a.txt"),
            ObjectKey::new("nested/m.txt"),
        ];

        keys.sort();

        assert_eq!(
            keys.map(|key| key.as_str().to_owned()),
            ["a.txt", "nested/m.txt", "z.txt"]
        );
    }

    #[test]
    fn object_key_can_be_cloned_without_changing_value() {
        let key = ObjectKey::new("prefix/example.txt");

        assert_eq!(key.clone(), key);
    }

    #[test]
    fn object_key_displays_inner_value() {
        assert_eq!(
            ObjectKey::new("prefix/example.txt").to_string(),
            "prefix/example.txt"
        );
    }

    #[test]
    fn practical_s3_object_keys_reject_empty_values() {
        assert!(!ObjectKey::new("").is_valid_s3_key());
    }

    #[test]
    fn practical_s3_object_keys_accept_path_like_keys_at_utf8_byte_limit() {
        let key = ObjectKey::new(format!(
            "prefix/{}",
            "a".repeat(MAX_OBJECT_KEY_UTF8_BYTES - "prefix/".len())
        ));

        assert!(key.is_valid_s3_key());
    }

    #[test]
    fn practical_s3_object_keys_accept_path_like_safe_control_characters() {
        let key = ObjectKey::new("prefix/\tline\ncarriage\robject.txt");

        assert!(key.is_valid_s3_key());
    }

    #[test]
    fn practical_s3_object_keys_reject_values_over_utf8_byte_limit() {
        for key in [
            ObjectKey::new("a".repeat(MAX_OBJECT_KEY_UTF8_BYTES + 1)),
            ObjectKey::new("é".repeat((MAX_OBJECT_KEY_UTF8_BYTES / 2) + 1)),
        ] {
            assert!(
                !key.is_valid_s3_key(),
                "object key should be invalid: {key}"
            );
        }
    }

    #[test]
    fn practical_s3_object_keys_reject_xml_invalid_control_characters() {
        for key in [
            ObjectKey::new("prefix/\0object.txt"),
            ObjectKey::new("prefix/\u{1F}object.txt"),
        ] {
            assert!(
                !key.is_valid_s3_key(),
                "object key should be XML-invalid: {key:?}"
            );
        }
    }

    #[test]
    fn list_objects_prefixes_allow_empty_and_reject_xml_invalid_control_characters() {
        assert!(is_valid_s3_object_key_prefix(""));
        assert!(is_valid_s3_object_key_prefix("nested/path/"));
        assert!(is_valid_s3_object_key_prefix("nested/\rpath/"));
        assert!(!is_valid_s3_object_key_prefix("nested/\0path/"));
        assert!(!is_valid_s3_object_key_prefix("nested/\u{1F}path/"));
    }
}
