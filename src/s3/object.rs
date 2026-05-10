// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ObjectKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectKey;

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
}
