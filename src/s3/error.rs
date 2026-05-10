// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum S3ErrorCode {
    NotImplemented,
}

#[cfg(test)]
mod tests {
    use super::S3ErrorCode;

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
}
