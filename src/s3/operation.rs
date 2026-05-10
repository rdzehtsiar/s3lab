// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum S3Operation {
    ListBuckets,
}

#[cfg(test)]
mod tests {
    use super::S3Operation;

    #[test]
    fn list_buckets_operation_is_copyable_and_comparable() {
        let operation = S3Operation::ListBuckets;
        let copied = operation;

        assert_eq!(copied, S3Operation::ListBuckets);
    }

    #[test]
    fn debug_output_names_operation() {
        assert_eq!(format!("{:?}", S3Operation::ListBuckets), "ListBuckets");
    }
}
