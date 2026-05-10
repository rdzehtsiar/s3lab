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
