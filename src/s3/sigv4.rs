// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};

const SIGV4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const CREDENTIAL_PARAM: &str = "Credential";
const SIGNED_HEADERS_PARAM: &str = "SignedHeaders";
const SIGNATURE_PARAM: &str = "Signature";
const CREDENTIAL_SCOPE_TERMINATOR: &str = "aws4_request";
const SIGNATURE_HEX_LENGTH: usize = 64;

/// Parsed AWS Signature Version 4 `Authorization` header.
///
/// This parser validates only the header shape and safe structural fields. It
/// does not verify the request signature or integrate with request routing.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SigV4Authorization {
    algorithm: SigV4Algorithm,
    credential: SigV4Credential,
    signed_headers: Vec<SignedHeaderName>,
    signature: String,
}

impl SigV4Authorization {
    /// The SigV4 algorithm declared by the header.
    pub fn algorithm(&self) -> SigV4Algorithm {
        self.algorithm
    }

    /// The parsed credential and credential scope.
    pub fn credential(&self) -> &SigV4Credential {
        &self.credential
    }

    /// Canonical signed header names in the order supplied by the client.
    pub fn signed_headers(&self) -> &[SignedHeaderName] {
        &self.signed_headers
    }

    /// The 64-character hexadecimal signature string.
    pub fn signature(&self) -> &str {
        &self.signature
    }
}

/// Supported SigV4 authorization algorithm.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SigV4Algorithm {
    Aws4HmacSha256,
}

impl SigV4Algorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Aws4HmacSha256 => SIGV4_ALGORITHM,
        }
    }
}

/// SigV4 credential identity and scope.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SigV4Credential {
    access_key_id: String,
    scope: SigV4CredentialScope,
}

impl SigV4Credential {
    /// Access key id from the credential field. This is an identifier, not the
    /// secret access key.
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Date, region, and service scope for signing key derivation.
    pub fn scope(&self) -> &SigV4CredentialScope {
        &self.scope
    }
}

/// Date, region, and service components of a SigV4 credential scope.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SigV4CredentialScope {
    date: String,
    region: String,
    service: String,
}

impl SigV4CredentialScope {
    /// Eight-digit signing date in `YYYYMMDD` form.
    pub fn date(&self) -> &str {
        &self.date
    }

    /// Signing region from the credential scope.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Signing service from the credential scope.
    pub fn service(&self) -> &str {
        &self.service
    }
}

/// Lower-case canonical header name from the SigV4 `SignedHeaders` field.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SignedHeaderName(String);

impl SignedHeaderName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Required SigV4 authorization parameters.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SigV4AuthorizationParameter {
    Credential,
    SignedHeaders,
    Signature,
}

impl SigV4AuthorizationParameter {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Credential => CREDENTIAL_PARAM,
            Self::SignedHeaders => SIGNED_HEADERS_PARAM,
            Self::Signature => SIGNATURE_PARAM,
        }
    }
}

/// Typed diagnostics for rejected SigV4 `Authorization` headers.
///
/// Diagnostic messages intentionally describe what to fix without echoing
/// credential values, signatures, or other caller-supplied secrets.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SigV4ParseDiagnostic {
    MalformedAuthorizationHeader,
    UnsupportedAlgorithm,
    MalformedParameter,
    UnknownParameter,
    MissingParameter {
        parameter: SigV4AuthorizationParameter,
    },
    DuplicateParameter {
        parameter: SigV4AuthorizationParameter,
    },
    InvalidCredentialScope,
    EmptyAccessKey,
    InvalidCredentialDate,
    EmptyRegion,
    EmptyService,
    InvalidCredentialScopeTerminator,
    EmptySignedHeaders,
    InvalidSignedHeaderName,
    DuplicateSignedHeader,
    UnsortedSignedHeaders,
    InvalidSignatureLength,
    InvalidSignatureHex,
}

impl SigV4ParseDiagnostic {
    /// Safe actionable text suitable for logs, tests, and future diagnostics.
    pub fn message(&self) -> &'static str {
        match self {
            Self::MalformedAuthorizationHeader => {
                "Use the SigV4 Authorization format: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=..."
            }
            Self::UnsupportedAlgorithm => {
                "Use AWS4-HMAC-SHA256 as the Authorization algorithm."
            }
            Self::MalformedParameter => {
                "Write each SigV4 Authorization parameter as Name=value."
            }
            Self::UnknownParameter => {
                "Remove unsupported SigV4 Authorization parameters."
            }
            Self::MissingParameter { .. } => {
                "Include the required Credential, SignedHeaders, and Signature parameters."
            }
            Self::DuplicateParameter { .. } => {
                "Include each SigV4 Authorization parameter only once."
            }
            Self::InvalidCredentialScope => {
                "Use a Credential scope with access key, date, region, service, and aws4_request parts."
            }
            Self::EmptyAccessKey => "Include a nonempty access key id in the Credential parameter.",
            Self::InvalidCredentialDate => "Use an eight-digit YYYYMMDD date in the Credential scope.",
            Self::EmptyRegion => "Include a nonempty region in the Credential scope.",
            Self::EmptyService => "Include a nonempty service in the Credential scope.",
            Self::InvalidCredentialScopeTerminator => {
                "End the Credential scope with aws4_request."
            }
            Self::EmptySignedHeaders => "Include at least one lower-case signed header name.",
            Self::InvalidSignedHeaderName => {
                "Use lower-case signed header names separated by semicolons."
            }
            Self::DuplicateSignedHeader => "List each signed header name only once.",
            Self::UnsortedSignedHeaders => "List signed header names in ascending order.",
            Self::InvalidSignatureLength => "Use a 64-character hexadecimal Signature value.",
            Self::InvalidSignatureHex => "Use only hexadecimal characters in the Signature value.",
        }
    }
}

impl Display for SigV4ParseDiagnostic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

/// Parse an AWS SigV4 `Authorization` header value.
///
/// The accepted form is:
/// `AWS4-HMAC-SHA256 Credential=<access>/<date>/<region>/<service>/aws4_request, SignedHeaders=<headers>, Signature=<64 hex>`.
pub fn parse_authorization_header(
    header_value: &str,
) -> Result<SigV4Authorization, SigV4ParseDiagnostic> {
    let (algorithm, parameter_text) = header_value
        .trim()
        .split_once(' ')
        .ok_or(SigV4ParseDiagnostic::MalformedAuthorizationHeader)?;

    if algorithm != SIGV4_ALGORITHM {
        return Err(SigV4ParseDiagnostic::UnsupportedAlgorithm);
    }

    if parameter_text.trim().is_empty() {
        return Err(SigV4ParseDiagnostic::MalformedAuthorizationHeader);
    }

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for parameter in parameter_text.split(',') {
        let (name, value) = parameter
            .trim()
            .split_once('=')
            .ok_or(SigV4ParseDiagnostic::MalformedParameter)?;

        match name.trim() {
            CREDENTIAL_PARAM => set_once(
                &mut credential,
                value.trim(),
                SigV4AuthorizationParameter::Credential,
            )?,
            SIGNED_HEADERS_PARAM => set_once(
                &mut signed_headers,
                value.trim(),
                SigV4AuthorizationParameter::SignedHeaders,
            )?,
            SIGNATURE_PARAM => set_once(
                &mut signature,
                value.trim(),
                SigV4AuthorizationParameter::Signature,
            )?,
            _ => return Err(SigV4ParseDiagnostic::UnknownParameter),
        }
    }

    let credential =
        parse_credential(credential.ok_or(SigV4ParseDiagnostic::MissingParameter {
            parameter: SigV4AuthorizationParameter::Credential,
        })?)?;
    let signed_headers = parse_signed_headers(signed_headers.ok_or(
        SigV4ParseDiagnostic::MissingParameter {
            parameter: SigV4AuthorizationParameter::SignedHeaders,
        },
    )?)?;
    let signature = parse_signature(signature.ok_or(SigV4ParseDiagnostic::MissingParameter {
        parameter: SigV4AuthorizationParameter::Signature,
    })?)?;

    Ok(SigV4Authorization {
        algorithm: SigV4Algorithm::Aws4HmacSha256,
        credential,
        signed_headers,
        signature,
    })
}

fn set_once<'a>(
    target: &mut Option<&'a str>,
    value: &'a str,
    parameter: SigV4AuthorizationParameter,
) -> Result<(), SigV4ParseDiagnostic> {
    if target.is_some() {
        return Err(SigV4ParseDiagnostic::DuplicateParameter { parameter });
    }

    *target = Some(value);
    Ok(())
}

fn parse_credential(value: &str) -> Result<SigV4Credential, SigV4ParseDiagnostic> {
    let parts = value.split('/').collect::<Vec<_>>();
    let [access_key_id, date, region, service, terminal] = parts.as_slice() else {
        return Err(SigV4ParseDiagnostic::InvalidCredentialScope);
    };

    if access_key_id.is_empty() {
        return Err(SigV4ParseDiagnostic::EmptyAccessKey);
    }

    if !is_valid_scope_date(date) {
        return Err(SigV4ParseDiagnostic::InvalidCredentialDate);
    }

    if region.is_empty() {
        return Err(SigV4ParseDiagnostic::EmptyRegion);
    }

    if service.is_empty() {
        return Err(SigV4ParseDiagnostic::EmptyService);
    }

    if *terminal != CREDENTIAL_SCOPE_TERMINATOR {
        return Err(SigV4ParseDiagnostic::InvalidCredentialScopeTerminator);
    }

    Ok(SigV4Credential {
        access_key_id: (*access_key_id).to_owned(),
        scope: SigV4CredentialScope {
            date: (*date).to_owned(),
            region: (*region).to_owned(),
            service: (*service).to_owned(),
        },
    })
}

fn parse_signed_headers(value: &str) -> Result<Vec<SignedHeaderName>, SigV4ParseDiagnostic> {
    if value.is_empty() {
        return Err(SigV4ParseDiagnostic::EmptySignedHeaders);
    }

    let mut names = Vec::new();
    let mut previous_name: Option<&str> = None;

    for name in value.split(';') {
        if !is_valid_signed_header_name(name) {
            return Err(SigV4ParseDiagnostic::InvalidSignedHeaderName);
        }

        if let Some(previous) = previous_name {
            if name == previous {
                return Err(SigV4ParseDiagnostic::DuplicateSignedHeader);
            }

            if name < previous {
                return Err(SigV4ParseDiagnostic::UnsortedSignedHeaders);
            }
        }

        previous_name = Some(name);
        names.push(SignedHeaderName(name.to_owned()));
    }

    Ok(names)
}

fn parse_signature(value: &str) -> Result<String, SigV4ParseDiagnostic> {
    if value.len() != SIGNATURE_HEX_LENGTH {
        return Err(SigV4ParseDiagnostic::InvalidSignatureLength);
    }

    if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SigV4ParseDiagnostic::InvalidSignatureHex);
    }

    Ok(value.to_owned())
}

fn is_valid_scope_date(value: &str) -> bool {
    value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_valid_signed_header_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-'))
}

#[cfg(test)]
mod tests {
    use super::{
        parse_authorization_header, SigV4Algorithm, SigV4AuthorizationParameter,
        SigV4ParseDiagnostic,
    };

    const VALID_AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF";

    #[test]
    fn parses_valid_authorization_header() {
        let authorization =
            parse_authorization_header(VALID_AUTHORIZATION).expect("valid authorization header");

        assert_eq!(authorization.algorithm(), SigV4Algorithm::Aws4HmacSha256);
        assert_eq!(
            authorization.credential().access_key_id(),
            "AKIAIOSFODNN7EXAMPLE"
        );
        assert_eq!(authorization.credential().scope().date(), "20260512");
        assert_eq!(authorization.credential().scope().region(), "us-east-1");
        assert_eq!(authorization.credential().scope().service(), "s3");
        assert_eq!(
            authorization
                .signed_headers()
                .iter()
                .map(|header| header.as_str())
                .collect::<Vec<_>>(),
            ["host", "x-amz-date"]
        );
        assert_eq!(
            authorization.signature(),
            "0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF"
        );
    }

    #[test]
    fn rejects_unsupported_algorithm() {
        assert_parse_error(
            "AWS4-HMAC-SHA1 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::UnsupportedAlgorithm,
        );
    }

    #[test]
    fn rejects_missing_required_parameter() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::MissingParameter {
                parameter: SigV4AuthorizationParameter::SignedHeaders,
            },
        );
    }

    #[test]
    fn rejects_duplicate_parameter() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::DuplicateParameter {
                parameter: SigV4AuthorizationParameter::SignedHeaders,
            },
        );
    }

    #[test]
    fn rejects_malformed_parameter() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::MalformedParameter,
        );
    }

    #[test]
    fn rejects_unknown_parameter() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Extra=value, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::UnknownParameter,
        );
    }

    #[test]
    fn rejects_invalid_credential_scope_part_count() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::InvalidCredentialScope,
        );
    }

    #[test]
    fn rejects_empty_access_key() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::EmptyAccessKey,
        );
    }

    #[test]
    fn rejects_invalid_scope_date() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/2026-05-12/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::InvalidCredentialDate,
        );
    }

    #[test]
    fn rejects_empty_region() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512//s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::EmptyRegion,
        );
    }

    #[test]
    fn rejects_empty_service() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1//aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::EmptyService,
        );
    }

    #[test]
    fn rejects_invalid_scope_terminator() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/not_aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::InvalidCredentialScopeTerminator,
        );
    }

    #[test]
    fn rejects_empty_signed_headers() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::EmptySignedHeaders,
        );
    }

    #[test]
    fn rejects_uppercase_signed_header_name() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=Host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::InvalidSignedHeaderName,
        );
    }

    #[test]
    fn rejects_duplicate_signed_header_name() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;host, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::DuplicateSignedHeader,
        );
    }

    #[test]
    fn rejects_unsorted_signed_header_names() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=x-amz-date;host, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF",
            SigV4ParseDiagnostic::UnsortedSignedHeaders,
        );
    }

    #[test]
    fn rejects_invalid_signature_length() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef",
            SigV4ParseDiagnostic::InvalidSignatureLength,
        );
    }

    #[test]
    fn rejects_invalid_signature_hex() {
        assert_parse_error(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEZ",
            SigV4ParseDiagnostic::InvalidSignatureHex,
        );
    }

    #[test]
    fn diagnostics_do_not_echo_sensitive_input() {
        let diagnostic = parse_authorization_header(
            "AWS4-HMAC-SHA256 Credential=SECRET_ACCESS_KEY/20260512/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=not-a-signature",
        )
        .expect_err("invalid signature length");

        assert!(!diagnostic.message().contains("SECRET_ACCESS_KEY"));
        assert!(!diagnostic.message().contains("not-a-signature"));
    }

    fn assert_parse_error(header: &str, expected: SigV4ParseDiagnostic) {
        assert_eq!(
            parse_authorization_header(header).expect_err("header should fail"),
            expected
        );
    }
}
