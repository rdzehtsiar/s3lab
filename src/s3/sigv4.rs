// SPDX-License-Identifier: Apache-2.0

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};

const SIGV4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const QUERY_ALGORITHM_PARAM: &str = "X-Amz-Algorithm";
const QUERY_CREDENTIAL_PARAM: &str = "X-Amz-Credential";
const QUERY_DATE_PARAM: &str = "X-Amz-Date";
const QUERY_EXPIRES_PARAM: &str = "X-Amz-Expires";
const QUERY_SIGNED_HEADERS_PARAM: &str = "X-Amz-SignedHeaders";
const QUERY_SIGNATURE_PARAM: &str = "X-Amz-Signature";
const QUERY_CONTENT_SHA256_PARAM: &str = "X-Amz-Content-Sha256";
const QUERY_SECURITY_TOKEN_PARAM: &str = "X-Amz-Security-Token";
const CREDENTIAL_PARAM: &str = "Credential";
const SIGNED_HEADERS_PARAM: &str = "SignedHeaders";
const SIGNATURE_PARAM: &str = "Signature";
const CREDENTIAL_SCOPE_TERMINATOR: &str = "aws4_request";
pub const SIGV4_UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const SIGNATURE_HEX_LENGTH: usize = 64;
const HMAC_SHA256_BLOCK_SIZE: usize = 64;

/// Parsed AWS Signature Version 4 `Authorization` header.
///
/// This parser validates only the header shape and safe structural fields. It
/// does not verify the request signature or integrate with request routing.
#[derive(Clone, Eq, PartialEq)]
pub struct SigV4Authorization {
    algorithm: SigV4Algorithm,
    credential: SigV4Credential,
    signed_headers: Vec<SignedHeaderName>,
    signature: String,
}

impl Debug for SigV4Authorization {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigV4Authorization")
            .field("algorithm", &self.algorithm)
            .field("credential", &self.credential)
            .field("signed_headers", &self.signed_headers)
            .field("signature", &RedactedText::new(&self.signature))
            .finish()
    }
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

/// Parsed AWS Signature Version 4 presigned query authorization parameters.
///
/// This parser validates the query-auth shape and safe structural fields. It
/// does not enforce expiration time because that requires a caller-supplied
/// clock policy.
#[derive(Clone, Eq, PartialEq)]
pub struct SigV4QueryAuthorization {
    algorithm: SigV4Algorithm,
    credential: SigV4Credential,
    request_datetime: String,
    expires_seconds: u32,
    signed_headers: Vec<SignedHeaderName>,
    signature: String,
    content_sha256: Option<String>,
}

impl Debug for SigV4QueryAuthorization {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigV4QueryAuthorization")
            .field("algorithm", &self.algorithm)
            .field("credential", &self.credential)
            .field(
                "request_datetime",
                &RedactedText::new(&self.request_datetime),
            )
            .field("expires_seconds", &self.expires_seconds)
            .field("signed_headers", &self.signed_headers)
            .field("signature", &RedactedText::new(&self.signature))
            .field(
                "content_sha256",
                &self.content_sha256.as_deref().map(RedactedText::new),
            )
            .finish()
    }
}

impl SigV4QueryAuthorization {
    /// The SigV4 algorithm declared by `X-Amz-Algorithm`.
    pub fn algorithm(&self) -> SigV4Algorithm {
        self.algorithm
    }

    /// The parsed credential and credential scope.
    pub fn credential(&self) -> &SigV4Credential {
        &self.credential
    }

    /// Request timestamp from `X-Amz-Date`.
    pub fn request_datetime(&self) -> &str {
        &self.request_datetime
    }

    /// Expiration duration from `X-Amz-Expires`.
    pub fn expires_seconds(&self) -> u32 {
        self.expires_seconds
    }

    /// Canonical signed header names in the order supplied by the client.
    pub fn signed_headers(&self) -> &[SignedHeaderName] {
        &self.signed_headers
    }

    /// The 64-character hexadecimal signature string.
    pub fn signature(&self) -> &str {
        &self.signature
    }

    /// Payload hash used for canonical request construction.
    pub fn payload_hash(&self) -> &str {
        self.content_sha256
            .as_deref()
            .unwrap_or(SIGV4_UNSIGNED_PAYLOAD)
    }

    /// Optional payload hash supplied by `X-Amz-Content-Sha256`.
    pub fn content_sha256(&self) -> Option<&str> {
        self.content_sha256.as_deref()
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
#[derive(Clone, Eq, PartialEq)]
pub struct SigV4Credential {
    access_key_id: String,
    scope: SigV4CredentialScope,
}

impl Debug for SigV4Credential {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigV4Credential")
            .field("access_key_id", &RedactedText::new(&self.access_key_id))
            .field("scope", &self.scope)
            .finish()
    }
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

/// SigV4 presigned-query parameters interpreted by the query-auth parser.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SigV4QueryParameter {
    Algorithm,
    Credential,
    Date,
    Expires,
    SignedHeaders,
    Signature,
    ContentSha256,
    SecurityToken,
}

impl SigV4QueryParameter {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Algorithm => QUERY_ALGORITHM_PARAM,
            Self::Credential => QUERY_CREDENTIAL_PARAM,
            Self::Date => QUERY_DATE_PARAM,
            Self::Expires => QUERY_EXPIRES_PARAM,
            Self::SignedHeaders => QUERY_SIGNED_HEADERS_PARAM,
            Self::Signature => QUERY_SIGNATURE_PARAM,
            Self::ContentSha256 => QUERY_CONTENT_SHA256_PARAM,
            Self::SecurityToken => QUERY_SECURITY_TOKEN_PARAM,
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

/// Typed diagnostics for rejected SigV4 presigned query authorization.
///
/// Diagnostic messages intentionally describe what to fix without echoing
/// credential values, signatures, session tokens, or other caller-supplied
/// secrets.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SigV4QueryParseDiagnostic {
    MissingParameter { parameter: SigV4QueryParameter },
    DuplicateParameter { parameter: SigV4QueryParameter },
    UnsupportedAlgorithm,
    UnsupportedSessionToken,
    InvalidCredentialScope,
    EmptyAccessKey,
    InvalidCredentialDate,
    EmptyRegion,
    EmptyService,
    InvalidCredentialScopeTerminator,
    InvalidRequestDate,
    InvalidExpires,
    EmptySignedHeaders,
    InvalidSignedHeaderName,
    DuplicateSignedHeader,
    UnsortedSignedHeaders,
    InvalidSignatureLength,
    InvalidSignatureHex,
    InvalidContentSha256,
}

impl SigV4QueryParseDiagnostic {
    /// Safe actionable text suitable for logs, tests, and future diagnostics.
    pub fn message(&self) -> &'static str {
        match self {
            Self::MissingParameter { .. } => {
                "Include the required X-Amz-Algorithm, X-Amz-Credential, X-Amz-Date, X-Amz-Expires, X-Amz-SignedHeaders, and X-Amz-Signature query parameters."
            }
            Self::DuplicateParameter { .. } => {
                "Include each SigV4 presigned query parameter only once."
            }
            Self::UnsupportedAlgorithm => "Use AWS4-HMAC-SHA256 as X-Amz-Algorithm.",
            Self::UnsupportedSessionToken => {
                "Session-token presigned URLs are not supported; omit X-Amz-Security-Token."
            }
            Self::InvalidCredentialScope => {
                "Use an X-Amz-Credential scope with access key, date, region, service, and aws4_request parts."
            }
            Self::EmptyAccessKey => "Include a nonempty access key id in X-Amz-Credential.",
            Self::InvalidCredentialDate => {
                "Use an eight-digit YYYYMMDD date in the X-Amz-Credential scope."
            }
            Self::EmptyRegion => "Include a nonempty region in the X-Amz-Credential scope.",
            Self::EmptyService => "Include a nonempty service in the X-Amz-Credential scope.",
            Self::InvalidCredentialScopeTerminator => {
                "End the X-Amz-Credential scope with aws4_request."
            }
            Self::InvalidRequestDate => "Use an X-Amz-Date value in YYYYMMDDTHHMMSSZ form.",
            Self::InvalidExpires => "Use decimal seconds in X-Amz-Expires.",
            Self::EmptySignedHeaders => {
                "Include at least one lower-case name in X-Amz-SignedHeaders."
            }
            Self::InvalidSignedHeaderName => {
                "Use lower-case signed header names separated by semicolons in X-Amz-SignedHeaders."
            }
            Self::DuplicateSignedHeader => "List each signed header name only once.",
            Self::UnsortedSignedHeaders => "List signed header names in ascending order.",
            Self::InvalidSignatureLength => {
                "Use a 64-character hexadecimal X-Amz-Signature value."
            }
            Self::InvalidSignatureHex => {
                "Use only hexadecimal characters in X-Amz-Signature."
            }
            Self::InvalidContentSha256 => {
                "Use UNSIGNED-PAYLOAD or a 64-character hexadecimal SHA-256 value in X-Amz-Content-Sha256."
            }
        }
    }
}

impl Display for SigV4QueryParseDiagnostic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

/// Diagnostics for canonical request construction and signature verification.
///
/// Messages intentionally avoid echoing secrets, signatures, or header values.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SigV4VerificationDiagnostic {
    MissingSignedHeader { header_name: String },
    SignatureMismatch,
}

impl SigV4VerificationDiagnostic {
    /// Safe actionable text suitable for logs, tests, and future diagnostics.
    pub fn message(&self) -> &'static str {
        match self {
            Self::MissingSignedHeader { .. } => {
                "A header listed in SignedHeaders was not present in the request; include it before signing or remove it from SignedHeaders."
            }
            Self::SignatureMismatch => {
                "The SigV4 signature did not match the canonical request; verify the method, path, query, signed headers, payload hash, credential scope, timestamp, and secret."
            }
        }
    }
}

impl Display for SigV4VerificationDiagnostic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

/// Opaque SigV4 signing key derived from a secret access key and credential scope.
#[derive(Clone, Eq, PartialEq)]
pub struct SigV4SigningKey {
    bytes: [u8; 32],
}

impl Debug for SigV4SigningKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigV4SigningKey")
            .field("bytes", &RedactedBytes::new(&self.bytes))
            .finish()
    }
}

/// Canonical request and string-to-sign generated during successful verification.
#[derive(Clone, Eq, PartialEq)]
pub struct SigV4Verification {
    canonical_request: String,
    string_to_sign: String,
}

impl Debug for SigV4Verification {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigV4Verification")
            .field(
                "canonical_request",
                &RedactedText::new(&self.canonical_request),
            )
            .field("string_to_sign", &RedactedText::new(&self.string_to_sign))
            .finish()
    }
}

impl SigV4Verification {
    /// Canonical request used for the verification attempt.
    pub fn canonical_request(&self) -> &str {
        &self.canonical_request
    }

    /// SigV4 string-to-sign used for the verification attempt.
    pub fn string_to_sign(&self) -> &str {
        &self.string_to_sign
    }
}

/// Borrowed request components needed for SigV4 signature verification.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SigV4VerificationRequest<'a> {
    pub request_datetime: &'a str,
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a [(&'a str, &'a str)],
    pub headers: &'a [(&'a str, &'a str)],
    pub payload_hash: &'a str,
}

impl Debug for SigV4VerificationRequest<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigV4VerificationRequest")
            .field(
                "request_datetime",
                &RedactedText::new(self.request_datetime),
            )
            .field("method", &RedactedText::new(self.method))
            .field("path", &RedactedText::new(self.path))
            .field("query", &RedactedPairs::new(self.query))
            .field("headers", &RedactedPairs::new(self.headers))
            .field("payload_hash", &RedactedText::new(self.payload_hash))
            .finish()
    }
}

/// Borrowed request components needed for presigned query SigV4 verification.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SigV4QueryVerificationRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a [(&'a str, &'a str)],
    pub headers: &'a [(&'a str, &'a str)],
}

impl Debug for SigV4QueryVerificationRequest<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigV4QueryVerificationRequest")
            .field("method", &RedactedText::new(self.method))
            .field("path", &RedactedText::new(self.path))
            .field("query", &RedactedPairs::new(self.query))
            .field("headers", &RedactedPairs::new(self.headers))
            .finish()
    }
}

/// Build a deterministic SigV4 canonical request.
///
/// Header names are matched case-insensitively against the supplied signed
/// headers. Duplicate header values are normalized and joined with a comma in
/// input order because SigV4 preserves repeated header value order.
pub fn build_canonical_request(
    method: &str,
    path: &str,
    query: &[(&str, &str)],
    headers: &[(&str, &str)],
    signed_headers: &[SignedHeaderName],
    payload_hash: &str,
) -> Result<String, SigV4VerificationDiagnostic> {
    let canonical_uri = canonical_uri(path);
    let canonical_query = canonical_query(query);
    let canonical_headers = canonical_headers(headers, signed_headers)?;
    let signed_headers_text = signed_headers
        .iter()
        .map(SignedHeaderName::as_str)
        .collect::<Vec<_>>()
        .join(";");

    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers_text,
        payload_hash
    ))
}

/// Build the SigV4 string-to-sign for a canonical request.
pub fn build_string_to_sign(
    request_datetime: &str,
    credential_scope: &SigV4CredentialScope,
    canonical_request: &str,
) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        SIGV4_ALGORITHM,
        request_datetime,
        credential_scope_text(credential_scope),
        sha256_hex(canonical_request.as_bytes())
    )
}

/// Derive the SigV4 signing key for the given secret and credential scope.
pub fn derive_signing_key(
    secret_access_key: &str,
    date: &str,
    region: &str,
    service: &str,
) -> SigV4SigningKey {
    let secret = format!("AWS4{}", secret_access_key);
    let date_key = hmac_sha256(secret.as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, service.as_bytes());
    let signing_key = hmac_sha256(&service_key, CREDENTIAL_SCOPE_TERMINATOR.as_bytes());

    SigV4SigningKey { bytes: signing_key }
}

/// Verify a parsed SigV4 authorization value against request components.
pub fn verify_signature(
    authorization: &SigV4Authorization,
    secret_access_key: &str,
    request: SigV4VerificationRequest<'_>,
) -> Result<SigV4Verification, SigV4VerificationDiagnostic> {
    let canonical_request = build_canonical_request(
        request.method,
        request.path,
        request.query,
        request.headers,
        authorization.signed_headers(),
        request.payload_hash,
    )?;
    let string_to_sign = build_string_to_sign(
        request.request_datetime,
        authorization.credential().scope(),
        &canonical_request,
    );
    let scope = authorization.credential().scope();
    let signing_key = derive_signing_key(
        secret_access_key,
        scope.date(),
        scope.region(),
        scope.service(),
    );
    let expected_signature = signature_hex(&signing_key, &string_to_sign);

    if !signatures_match(&expected_signature, authorization.signature()) {
        return Err(SigV4VerificationDiagnostic::SignatureMismatch);
    }

    Ok(SigV4Verification {
        canonical_request,
        string_to_sign,
    })
}

/// Verify parsed SigV4 presigned query authorization against request components.
///
/// The canonical request excludes `X-Amz-Signature` from the canonical query as
/// required by SigV4 presigned URL verification.
pub fn verify_query_signature(
    authorization: &SigV4QueryAuthorization,
    secret_access_key: &str,
    request: SigV4QueryVerificationRequest<'_>,
) -> Result<SigV4Verification, SigV4VerificationDiagnostic> {
    let query_without_signature = request
        .query
        .iter()
        .copied()
        .filter(|(name, _)| *name != QUERY_SIGNATURE_PARAM)
        .collect::<Vec<_>>();
    let canonical_request = build_canonical_request(
        request.method,
        request.path,
        &query_without_signature,
        request.headers,
        authorization.signed_headers(),
        authorization.payload_hash(),
    )?;
    let string_to_sign = build_string_to_sign(
        authorization.request_datetime(),
        authorization.credential().scope(),
        &canonical_request,
    );
    let scope = authorization.credential().scope();
    let signing_key = derive_signing_key(
        secret_access_key,
        scope.date(),
        scope.region(),
        scope.service(),
    );
    let expected_signature = signature_hex(&signing_key, &string_to_sign);

    if !signatures_match(&expected_signature, authorization.signature()) {
        return Err(SigV4VerificationDiagnostic::SignatureMismatch);
    }

    Ok(SigV4Verification {
        canonical_request,
        string_to_sign,
    })
}

/// Parse AWS SigV4 presigned URL query parameters.
///
/// Query names and values must be decoded by the caller before parsing. Unknown
/// non-SigV4 query parameters are preserved for canonicalization by
/// `verify_query_signature` and ignored by this parser.
pub fn parse_query_authorization(
    query: &[(&str, &str)],
) -> Result<SigV4QueryAuthorization, SigV4QueryParseDiagnostic> {
    let mut algorithm = None;
    let mut credential = None;
    let mut request_datetime = None;
    let mut expires = None;
    let mut signed_headers = None;
    let mut signature = None;
    let mut content_sha256 = None;

    for (name, value) in query {
        match *name {
            QUERY_ALGORITHM_PARAM => {
                set_query_once(&mut algorithm, value, SigV4QueryParameter::Algorithm)?
            }
            QUERY_CREDENTIAL_PARAM => {
                set_query_once(&mut credential, value, SigV4QueryParameter::Credential)?
            }
            QUERY_DATE_PARAM => {
                set_query_once(&mut request_datetime, value, SigV4QueryParameter::Date)?
            }
            QUERY_EXPIRES_PARAM => {
                set_query_once(&mut expires, value, SigV4QueryParameter::Expires)?
            }
            QUERY_SIGNED_HEADERS_PARAM => set_query_once(
                &mut signed_headers,
                value,
                SigV4QueryParameter::SignedHeaders,
            )?,
            QUERY_SIGNATURE_PARAM => {
                set_query_once(&mut signature, value, SigV4QueryParameter::Signature)?
            }
            QUERY_CONTENT_SHA256_PARAM => set_query_once(
                &mut content_sha256,
                value,
                SigV4QueryParameter::ContentSha256,
            )?,
            QUERY_SECURITY_TOKEN_PARAM => {
                return Err(SigV4QueryParseDiagnostic::UnsupportedSessionToken);
            }
            _ => {}
        }
    }

    let algorithm = require_query_parameter(algorithm, SigV4QueryParameter::Algorithm)?;
    if algorithm != SIGV4_ALGORITHM {
        return Err(SigV4QueryParseDiagnostic::UnsupportedAlgorithm);
    }

    let credential = parse_credential(require_query_parameter(
        credential,
        SigV4QueryParameter::Credential,
    )?)
    .map_err(query_diagnostic_from_header_diagnostic)?;
    let request_datetime = require_query_parameter(request_datetime, SigV4QueryParameter::Date)?;
    if !is_valid_request_datetime(request_datetime) {
        return Err(SigV4QueryParseDiagnostic::InvalidRequestDate);
    }
    let expires_seconds = parse_query_expires(require_query_parameter(
        expires,
        SigV4QueryParameter::Expires,
    )?)?;
    let signed_headers = parse_signed_headers(require_query_parameter(
        signed_headers,
        SigV4QueryParameter::SignedHeaders,
    )?)
    .map_err(query_diagnostic_from_header_diagnostic)?;
    let signature = parse_signature(require_query_parameter(
        signature,
        SigV4QueryParameter::Signature,
    )?)
    .map_err(query_diagnostic_from_header_diagnostic)?;

    Ok(SigV4QueryAuthorization {
        algorithm: SigV4Algorithm::Aws4HmacSha256,
        credential,
        request_datetime: request_datetime.to_owned(),
        expires_seconds,
        signed_headers,
        signature,
        content_sha256: content_sha256.map(parse_query_content_sha256).transpose()?,
    })
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

fn set_query_once<'a>(
    target: &mut Option<&'a str>,
    value: &'a str,
    parameter: SigV4QueryParameter,
) -> Result<(), SigV4QueryParseDiagnostic> {
    if target.is_some() {
        return Err(SigV4QueryParseDiagnostic::DuplicateParameter { parameter });
    }

    *target = Some(value);
    Ok(())
}

fn require_query_parameter(
    value: Option<&str>,
    parameter: SigV4QueryParameter,
) -> Result<&str, SigV4QueryParseDiagnostic> {
    value.ok_or(SigV4QueryParseDiagnostic::MissingParameter { parameter })
}

fn parse_query_expires(value: &str) -> Result<u32, SigV4QueryParseDiagnostic> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SigV4QueryParseDiagnostic::InvalidExpires);
    }

    value
        .parse::<u32>()
        .map_err(|_| SigV4QueryParseDiagnostic::InvalidExpires)
}

fn parse_query_content_sha256(value: &str) -> Result<String, SigV4QueryParseDiagnostic> {
    if value == SIGV4_UNSIGNED_PAYLOAD || is_literal_sha256(value) {
        Ok(value.to_owned())
    } else {
        Err(SigV4QueryParseDiagnostic::InvalidContentSha256)
    }
}

fn is_literal_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn query_diagnostic_from_header_diagnostic(
    diagnostic: SigV4ParseDiagnostic,
) -> SigV4QueryParseDiagnostic {
    match diagnostic {
        SigV4ParseDiagnostic::InvalidCredentialScope => {
            SigV4QueryParseDiagnostic::InvalidCredentialScope
        }
        SigV4ParseDiagnostic::EmptyAccessKey => SigV4QueryParseDiagnostic::EmptyAccessKey,
        SigV4ParseDiagnostic::InvalidCredentialDate => {
            SigV4QueryParseDiagnostic::InvalidCredentialDate
        }
        SigV4ParseDiagnostic::EmptyRegion => SigV4QueryParseDiagnostic::EmptyRegion,
        SigV4ParseDiagnostic::EmptyService => SigV4QueryParseDiagnostic::EmptyService,
        SigV4ParseDiagnostic::InvalidCredentialScopeTerminator => {
            SigV4QueryParseDiagnostic::InvalidCredentialScopeTerminator
        }
        SigV4ParseDiagnostic::EmptySignedHeaders => SigV4QueryParseDiagnostic::EmptySignedHeaders,
        SigV4ParseDiagnostic::InvalidSignedHeaderName => {
            SigV4QueryParseDiagnostic::InvalidSignedHeaderName
        }
        SigV4ParseDiagnostic::DuplicateSignedHeader => {
            SigV4QueryParseDiagnostic::DuplicateSignedHeader
        }
        SigV4ParseDiagnostic::UnsortedSignedHeaders => {
            SigV4QueryParseDiagnostic::UnsortedSignedHeaders
        }
        SigV4ParseDiagnostic::InvalidSignatureLength => {
            SigV4QueryParseDiagnostic::InvalidSignatureLength
        }
        SigV4ParseDiagnostic::InvalidSignatureHex => SigV4QueryParseDiagnostic::InvalidSignatureHex,
        SigV4ParseDiagnostic::MalformedAuthorizationHeader
        | SigV4ParseDiagnostic::UnsupportedAlgorithm
        | SigV4ParseDiagnostic::MalformedParameter
        | SigV4ParseDiagnostic::UnknownParameter
        | SigV4ParseDiagnostic::MissingParameter { .. }
        | SigV4ParseDiagnostic::DuplicateParameter { .. } => {
            unreachable!("query parsing does not use Authorization header diagnostics")
        }
    }
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

fn is_valid_request_datetime(value: &str) -> bool {
    value.len() == 16
        && value.as_bytes()[8] == b'T'
        && value.as_bytes()[15] == b'Z'
        && value[..8].bytes().all(|byte| byte.is_ascii_digit())
        && value[9..15].bytes().all(|byte| byte.is_ascii_digit())
}

fn is_valid_signed_header_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-'))
}

fn canonical_uri(path: &str) -> String {
    let normalized_path = if path.is_empty() { "/" } else { path };
    let encoded = percent_encode(normalized_path, false);

    if encoded.starts_with('/') {
        encoded
    } else {
        format!("/{encoded}")
    }
}

fn canonical_query(query: &[(&str, &str)]) -> String {
    let mut encoded_pairs = query
        .iter()
        .map(|(name, value)| (percent_encode(name, true), percent_encode(value, true)))
        .collect::<Vec<_>>();

    encoded_pairs.sort();

    encoded_pairs
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(
    headers: &[(&str, &str)],
    signed_headers: &[SignedHeaderName],
) -> Result<String, SigV4VerificationDiagnostic> {
    let mut grouped_headers = BTreeMap::<String, Vec<String>>::new();

    for (name, value) in headers {
        grouped_headers
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(normalize_header_value(value));
    }

    let mut canonical = String::new();
    for signed_header in signed_headers {
        let Some(values) = grouped_headers.get_mut(signed_header.as_str()) else {
            return Err(SigV4VerificationDiagnostic::MissingSignedHeader {
                header_name: signed_header.as_str().to_owned(),
            });
        };

        canonical.push_str(signed_header.as_str());
        canonical.push(':');
        canonical.push_str(&values.join(","));
        canonical.push('\n');
    }

    Ok(canonical)
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn credential_scope_text(scope: &SigV4CredentialScope) -> String {
    format!(
        "{}/{}/{}/{}",
        scope.date(),
        scope.region(),
        scope.service(),
        CREDENTIAL_SCOPE_TERMINATOR
    )
}

fn signature_hex(signing_key: &SigV4SigningKey, string_to_sign: &str) -> String {
    hex_encode(&hmac_sha256(&signing_key.bytes, string_to_sign.as_bytes()))
}

fn sha256_hex(value: &[u8]) -> String {
    hex_encode(&Sha256::digest(value))
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> [u8; 32] {
    let mut normalized_key = [0_u8; HMAC_SHA256_BLOCK_SIZE];
    if key.len() > HMAC_SHA256_BLOCK_SIZE {
        let digest = Sha256::digest(key);
        normalized_key[..digest.len()].copy_from_slice(&digest);
    } else {
        normalized_key[..key.len()].copy_from_slice(key);
    }

    let mut outer_key_pad = [0x5c_u8; HMAC_SHA256_BLOCK_SIZE];
    let mut inner_key_pad = [0x36_u8; HMAC_SHA256_BLOCK_SIZE];
    for index in 0..HMAC_SHA256_BLOCK_SIZE {
        outer_key_pad[index] ^= normalized_key[index];
        inner_key_pad[index] ^= normalized_key[index];
    }

    let mut inner = Sha256::new();
    inner.update(inner_key_pad);
    inner.update(value);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_key_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn signatures_match(expected: &str, provided: &str) -> bool {
    if expected.len() != provided.len() {
        return false;
    }

    expected
        .bytes()
        .zip(provided.bytes())
        .fold(0_u8, |difference, (expected, provided)| {
            difference | (expected.to_ascii_lowercase() ^ provided.to_ascii_lowercase())
        })
        == 0
}

struct RedactedText<'a> {
    value: &'a str,
}

impl<'a> RedactedText<'a> {
    fn new(value: &'a str) -> Self {
        Self { value }
    }
}

impl Debug for RedactedText<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "[redacted; {} bytes]", self.value.len())
    }
}

struct RedactedBytes<'a> {
    value: &'a [u8],
}

impl<'a> RedactedBytes<'a> {
    fn new(value: &'a [u8]) -> Self {
        Self { value }
    }
}

impl Debug for RedactedBytes<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "[redacted; {} bytes]", self.value.len())
    }
}

struct RedactedPairs<'a> {
    pairs: &'a [(&'a str, &'a str)],
}

impl<'a> RedactedPairs<'a> {
    fn new(pairs: &'a [(&'a str, &'a str)]) -> Self {
        Self { pairs }
    }
}

impl Debug for RedactedPairs<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let byte_count = self
            .pairs
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum::<usize>();

        write!(
            formatter,
            "[redacted; {} pairs; {byte_count} bytes]",
            self.pairs.len()
        )
    }
}

fn hex_encode(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);

    for byte in value {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }

    encoded
}

fn percent_encode(value: &str, encode_slash: bool) -> String {
    let mut encoded = String::new();

    for byte in value.bytes() {
        if is_unreserved_uri_byte(byte) || (!encode_slash && byte == b'/') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(nibble_to_upper_hex(byte >> 4));
            encoded.push(nibble_to_upper_hex(byte & 0x0f));
        }
    }

    encoded
}

fn is_unreserved_uri_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn nibble_to_upper_hex(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'A' + (nibble - 10)) as char,
        _ => unreachable!("nibble is always masked to four bits"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_canonical_request, build_string_to_sign, derive_signing_key,
        parse_authorization_header, parse_query_authorization, signature_hex,
        verify_query_signature, verify_signature, SigV4Algorithm, SigV4AuthorizationParameter,
        SigV4ParseDiagnostic, SigV4QueryParameter, SigV4QueryParseDiagnostic,
        SigV4QueryVerificationRequest, SigV4VerificationDiagnostic, SigV4VerificationRequest,
        SignedHeaderName, SIGV4_UNSIGNED_PAYLOAD,
    };

    const VALID_AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF";
    const VALID_IAM_AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=host;x-amz-date, Signature=b2e4af44cfad96d9ffa3c5653674a927b9b0995c33de22e1f843745ce37c1d5e";
    const EMPTY_PAYLOAD_SHA256: &str =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const TEST_SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const IAM_REQUEST_DATETIME: &str = "20150830T123600Z";
    const IAM_QUERY: &[(&str, &str)] = &[("Action", "ListUsers"), ("Version", "2010-05-08")];
    const IAM_HEADERS: &[(&str, &str)] = &[
        ("Host", "iam.amazonaws.com"),
        ("X-Amz-Date", IAM_REQUEST_DATETIME),
    ];

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

    #[test]
    fn authorization_debug_redacts_signature() {
        let authorization =
            parse_authorization_header(VALID_AUTHORIZATION).expect("valid authorization header");
        let debug = format!("{authorization:?}");

        assert!(debug.contains("SigV4Authorization"));
        assert!(debug.contains("signature"));
        assert!(debug.contains("[redacted; 64 bytes]"));
        assert!(!debug.contains(authorization.signature()));
        assert!(!debug.contains(authorization.credential().access_key_id()));
    }

    #[test]
    fn signing_key_debug_redacts_key_bytes() {
        let signing_key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        let raw_bytes_debug = format!("{:?}", signing_key.bytes);
        let debug = format!("{signing_key:?}");

        assert!(debug.contains("SigV4SigningKey"));
        assert!(debug.contains("bytes"));
        assert!(debug.contains("[redacted; 32 bytes]"));
        assert!(!debug.contains(&raw_bytes_debug));
    }

    #[test]
    fn verification_debug_redacts_canonical_request_and_string_to_sign() {
        let authorization = valid_iam_authorization();
        let verification = verify_signature(
            &authorization,
            TEST_SECRET_ACCESS_KEY,
            valid_iam_verification_request(),
        )
        .expect("signature should verify");
        let debug = format!("{verification:?}");

        assert!(debug.contains("SigV4Verification"));
        assert!(debug.contains("canonical_request"));
        assert!(debug.contains("string_to_sign"));
        assert!(!debug.contains(verification.canonical_request()));
        assert!(!debug.contains(verification.string_to_sign()));
        assert!(!debug.contains("iam.amazonaws.com"));
        assert!(!debug.contains("ListUsers"));
    }

    #[test]
    fn verification_request_debug_redacts_request_components() {
        let request = SigV4VerificationRequest {
            request_datetime: "20150830T123600Z",
            method: "GET",
            path: "/private/object",
            query: &[(
                "X-Amz-Credential",
                "AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request",
            )],
            headers: &[("Authorization", VALID_AUTHORIZATION)],
            payload_hash: EMPTY_PAYLOAD_SHA256,
        };
        let debug = format!("{request:?}");

        assert!(debug.contains("SigV4VerificationRequest"));
        assert!(debug.contains("request_datetime"));
        assert!(debug.contains("query"));
        assert!(debug.contains("[redacted; 1 pairs;"));
        assert!(!debug.contains("20150830T123600Z"));
        assert!(!debug.contains("GET"));
        assert!(!debug.contains("/private/object"));
        assert!(!debug.contains("X-Amz-Credential"));
        assert!(!debug.contains("AKIDEXAMPLE"));
        assert!(!debug.contains("Authorization"));
        assert!(!debug.contains(VALID_AUTHORIZATION));
        assert!(!debug.contains(EMPTY_PAYLOAD_SHA256));
    }

    #[test]
    fn builds_canonical_request_with_sorted_query_and_headers() {
        let canonical = build_canonical_request(
            "get",
            "/bucket/photos/a b.txt",
            &[("b", "two words"), ("a", "~"), ("a", "/slash"), ("a", "")],
            &[
                ("X-Amz-Meta", " second "),
                ("host", " examplebucket.s3.amazonaws.com "),
                ("x-amz-meta", "first  value"),
                ("X-Amz-Date", "20130524T000000Z"),
            ],
            &signed_headers(&["host", "x-amz-date", "x-amz-meta"]),
            EMPTY_PAYLOAD_SHA256,
        )
        .expect("canonical request");

        assert_eq!(
            canonical,
            "GET\n\
             /bucket/photos/a%20b.txt\n\
             a=&a=%2Fslash&a=~&b=two%20words\n\
             host:examplebucket.s3.amazonaws.com\n\
             x-amz-date:20130524T000000Z\n\
             x-amz-meta:second,first value\n\
             \n\
             host;x-amz-date;x-amz-meta\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn preserves_duplicate_header_value_input_order() {
        let canonical = build_canonical_request(
            "GET",
            "/",
            &[],
            &[
                ("x-amz-meta-ordered", " beta "),
                ("X-Amz-Meta-Ordered", "alpha"),
                ("x-amz-meta-ordered", " gamma  delta "),
            ],
            &signed_headers(&["x-amz-meta-ordered"]),
            EMPTY_PAYLOAD_SHA256,
        )
        .expect("canonical request");

        assert_eq!(
            canonical,
            "GET\n\
             /\n\
             \n\
             x-amz-meta-ordered:beta,alpha,gamma delta\n\
             \n\
             x-amz-meta-ordered\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn builds_string_to_sign_and_verifies_signature() {
        let authorization = valid_iam_authorization();
        let canonical = build_canonical_request(
            "GET",
            "/",
            IAM_QUERY,
            IAM_HEADERS,
            authorization.signed_headers(),
            EMPTY_PAYLOAD_SHA256,
        )
        .expect("canonical request");
        let string_to_sign = build_string_to_sign(
            IAM_REQUEST_DATETIME,
            authorization.credential().scope(),
            &canonical,
        );

        assert_eq!(
            string_to_sign,
            "AWS4-HMAC-SHA256\n\
             20150830T123600Z\n\
             20150830/us-east-1/iam/aws4_request\n\
             5599feeca6d065c7c80025038896f3f7f008849eacf307aa7d0cf8be7116cea6"
        );

        let verification = verify_signature(
            &authorization,
            TEST_SECRET_ACCESS_KEY,
            valid_iam_verification_request(),
        )
        .expect("signature should verify");

        assert_eq!(verification.canonical_request(), canonical);
        assert_eq!(verification.string_to_sign(), string_to_sign);
    }

    #[test]
    fn reports_missing_signed_header_without_echoing_header_values() {
        let diagnostic = build_canonical_request(
            "GET",
            "/",
            &[],
            &[("Host", "iam.amazonaws.com")],
            &signed_headers(&["host", "x-amz-date"]),
            EMPTY_PAYLOAD_SHA256,
        )
        .expect_err("missing signed header");

        assert_eq!(
            diagnostic,
            SigV4VerificationDiagnostic::MissingSignedHeader {
                header_name: "x-amz-date".to_owned()
            }
        );
        assert!(!diagnostic.message().contains("iam.amazonaws.com"));
    }

    #[test]
    fn reports_signature_mismatch_without_echoing_signatures_or_secret() {
        let authorization = parse_authorization_header(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000",
        )
        .expect("valid authorization");

        let diagnostic = verify_signature(
            &authorization,
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            SigV4VerificationRequest {
                request_datetime: "20150830T123600Z",
                method: "GET",
                path: "/",
                query: &[("Action", "ListUsers"), ("Version", "2010-05-08")],
                headers: &[
                    ("Host", "iam.amazonaws.com"),
                    ("X-Amz-Date", "20150830T123600Z"),
                ],
                payload_hash: EMPTY_PAYLOAD_SHA256,
            },
        )
        .expect_err("signature should not verify");

        assert_eq!(diagnostic, SigV4VerificationDiagnostic::SignatureMismatch);
        assert!(!diagnostic.message().contains("000000"));
        assert!(!diagnostic
            .message()
            .contains("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"));
    }

    #[test]
    fn parses_and_verifies_valid_presigned_query_authorization() {
        let query = valid_presigned_query(None);
        let query_refs = query_refs(&query);
        let authorization =
            parse_query_authorization(&query_refs).expect("valid presigned query authorization");

        assert_eq!(authorization.algorithm(), SigV4Algorithm::Aws4HmacSha256);
        assert_eq!(authorization.credential().access_key_id(), "AKIDEXAMPLE");
        assert_eq!(authorization.credential().scope().date(), "20130524");
        assert_eq!(authorization.credential().scope().region(), "us-east-1");
        assert_eq!(authorization.credential().scope().service(), "s3");
        assert_eq!(authorization.request_datetime(), "20130524T000000Z");
        assert_eq!(authorization.expires_seconds(), 86400);
        assert_eq!(authorization.content_sha256(), None);
        assert_eq!(authorization.payload_hash(), SIGV4_UNSIGNED_PAYLOAD);
        assert_eq!(
            authorization
                .signed_headers()
                .iter()
                .map(|header| header.as_str())
                .collect::<Vec<_>>(),
            ["host"]
        );

        verify_query_signature(
            &authorization,
            TEST_SECRET_ACCESS_KEY,
            SigV4QueryVerificationRequest {
                method: "GET",
                path: "/example-bucket/object.txt",
                query: &query_refs,
                headers: &[("host", "examplebucket.s3.amazonaws.com")],
            },
        )
        .expect("presigned query signature should verify");
    }

    #[test]
    fn rejects_presigned_query_missing_required_parameter() {
        let query = [
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            (
                "X-Amz-Credential",
                "AKIDEXAMPLE/20130524/us-east-1/s3/aws4_request",
            ),
            ("X-Amz-Date", "20130524T000000Z"),
            ("X-Amz-SignedHeaders", "host"),
            (
                "X-Amz-Signature",
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
        ];

        assert_query_parse_error(
            &query,
            SigV4QueryParseDiagnostic::MissingParameter {
                parameter: SigV4QueryParameter::Expires,
            },
        );
    }

    #[test]
    fn rejects_presigned_query_duplicate_parameter() {
        let query = [
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            (
                "X-Amz-Credential",
                "AKIDEXAMPLE/20130524/us-east-1/s3/aws4_request",
            ),
            ("X-Amz-Date", "20130524T000000Z"),
            ("X-Amz-Expires", "86400"),
            ("X-Amz-SignedHeaders", "host"),
            (
                "X-Amz-Signature",
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
        ];

        assert_query_parse_error(
            &query,
            SigV4QueryParseDiagnostic::DuplicateParameter {
                parameter: SigV4QueryParameter::Algorithm,
            },
        );
    }

    #[test]
    fn rejects_presigned_query_bad_algorithm() {
        assert_query_parse_error(
            &presigned_query_with_overrides(&[("X-Amz-Algorithm", "AWS4-HMAC-SHA1")]),
            SigV4QueryParseDiagnostic::UnsupportedAlgorithm,
        );
    }

    #[test]
    fn rejects_presigned_query_bad_signature() {
        assert_query_parse_error(
            &presigned_query_with_overrides(&[("X-Amz-Signature", "not-hex")]),
            SigV4QueryParseDiagnostic::InvalidSignatureLength,
        );
    }

    #[test]
    fn rejects_presigned_query_bad_credential() {
        assert_query_parse_error(
            &presigned_query_with_overrides(&[("X-Amz-Credential", "AKIDEXAMPLE/20130524/s3")]),
            SigV4QueryParseDiagnostic::InvalidCredentialScope,
        );
    }

    #[test]
    fn rejects_presigned_query_bad_signed_headers() {
        assert_query_parse_error(
            &presigned_query_with_overrides(&[("X-Amz-SignedHeaders", "Host")]),
            SigV4QueryParseDiagnostic::InvalidSignedHeaderName,
        );
    }

    #[test]
    fn rejects_presigned_query_session_token() {
        let query = presigned_query_with_overrides(&[("X-Amz-Security-Token", "session-token")]);

        assert_query_parse_error(&query, SigV4QueryParseDiagnostic::UnsupportedSessionToken);
    }

    #[test]
    fn presigned_canonical_query_excludes_signature() {
        let query = valid_presigned_query(None);
        let query_refs = query_refs(&query);
        let authorization =
            parse_query_authorization(&query_refs).expect("valid presigned query authorization");
        let verification = verify_query_signature(
            &authorization,
            TEST_SECRET_ACCESS_KEY,
            SigV4QueryVerificationRequest {
                method: "GET",
                path: "/example-bucket/object.txt",
                query: &query_refs,
                headers: &[("host", "examplebucket.s3.amazonaws.com")],
            },
        )
        .expect("presigned query signature should verify");

        assert_eq!(
            verification.canonical_request(),
            "GET\n\
             /example-bucket/object.txt\n\
             X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDEXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&response-content-type=text%2Fplain\n\
             host:examplebucket.s3.amazonaws.com\n\
             \n\
             host\n\
             UNSIGNED-PAYLOAD"
        );
        assert!(!verification.canonical_request().contains("X-Amz-Signature"));
    }

    #[test]
    fn presigned_query_defaults_payload_hash_to_unsigned_payload() {
        let query = valid_presigned_query(None);
        let query_refs = query_refs(&query);
        let authorization =
            parse_query_authorization(&query_refs).expect("valid presigned query authorization");

        assert_eq!(authorization.payload_hash(), SIGV4_UNSIGNED_PAYLOAD);
        assert!(verify_query_signature(
            &authorization,
            TEST_SECRET_ACCESS_KEY,
            SigV4QueryVerificationRequest {
                method: "GET",
                path: "/example-bucket/object.txt",
                query: &query_refs,
                headers: &[("host", "examplebucket.s3.amazonaws.com")],
            },
        )
        .expect("presigned query signature should verify")
        .canonical_request()
        .ends_with(SIGV4_UNSIGNED_PAYLOAD));
    }

    #[test]
    fn presigned_query_uses_explicit_content_sha256() {
        let query = valid_presigned_query(Some(EMPTY_PAYLOAD_SHA256));
        let query_refs = query_refs(&query);
        let authorization =
            parse_query_authorization(&query_refs).expect("valid presigned query authorization");

        assert_eq!(authorization.content_sha256(), Some(EMPTY_PAYLOAD_SHA256));
        assert_eq!(authorization.payload_hash(), EMPTY_PAYLOAD_SHA256);
        assert!(verify_query_signature(
            &authorization,
            TEST_SECRET_ACCESS_KEY,
            SigV4QueryVerificationRequest {
                method: "GET",
                path: "/example-bucket/object.txt",
                query: &query_refs,
                headers: &[("host", "examplebucket.s3.amazonaws.com")],
            },
        )
        .expect("presigned query signature should verify")
        .canonical_request()
        .ends_with(EMPTY_PAYLOAD_SHA256));
    }

    #[test]
    fn presigned_query_authorization_debug_redacts_credentials_signature_and_payload_hash() {
        let query = valid_presigned_query(Some(EMPTY_PAYLOAD_SHA256));
        let query_refs = query_refs(&query);
        let authorization =
            parse_query_authorization(&query_refs).expect("valid presigned query authorization");
        let debug = format!("{authorization:?}");

        assert!(debug.contains("SigV4QueryAuthorization"));
        assert!(debug.contains("credential"));
        assert!(debug.contains("signature"));
        assert!(debug.contains("content_sha256"));
        assert!(!debug.contains(authorization.credential().access_key_id()));
        assert!(!debug.contains(authorization.signature()));
        assert!(!debug.contains(EMPTY_PAYLOAD_SHA256));
    }

    #[test]
    fn rejects_presigned_query_invalid_content_sha256() {
        assert_query_parse_error(
            &presigned_query_with_overrides(&[("X-Amz-Content-Sha256", "not-a-sha256")]),
            SigV4QueryParseDiagnostic::InvalidContentSha256,
        );
    }

    fn assert_parse_error(header: &str, expected: SigV4ParseDiagnostic) {
        assert_eq!(
            parse_authorization_header(header).expect_err("header should fail"),
            expected
        );
    }

    fn assert_query_parse_error(query: &[(&str, &str)], expected: SigV4QueryParseDiagnostic) {
        assert_eq!(
            parse_query_authorization(query).expect_err("query should fail"),
            expected
        );
    }

    fn signed_headers(names: &[&str]) -> Vec<SignedHeaderName> {
        names
            .iter()
            .map(|name| SignedHeaderName((*name).to_owned()))
            .collect()
    }

    fn valid_presigned_query(content_sha256: Option<&str>) -> Vec<(String, String)> {
        let mut query = unsigned_presigned_query(content_sha256);
        let query_refs = query_refs(&query);
        let payload_hash = content_sha256.unwrap_or(SIGV4_UNSIGNED_PAYLOAD);
        let signature = presigned_signature(&query_refs, payload_hash);
        query.push(("X-Amz-Signature".to_owned(), signature));
        query
    }

    fn unsigned_presigned_query(content_sha256: Option<&str>) -> Vec<(String, String)> {
        let mut query = vec![
            ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
            (
                "X-Amz-Credential".to_owned(),
                "AKIDEXAMPLE/20130524/us-east-1/s3/aws4_request".to_owned(),
            ),
            ("X-Amz-Date".to_owned(), "20130524T000000Z".to_owned()),
            ("X-Amz-Expires".to_owned(), "86400".to_owned()),
            ("X-Amz-SignedHeaders".to_owned(), "host".to_owned()),
            ("response-content-type".to_owned(), "text/plain".to_owned()),
        ];

        if let Some(content_sha256) = content_sha256 {
            query.push(("X-Amz-Content-Sha256".to_owned(), content_sha256.to_owned()));
        }

        query
    }

    fn presigned_query_with_overrides<'a>(
        overrides: &'a [(&'a str, &'a str)],
    ) -> Vec<(&'a str, &'a str)> {
        let mut query = [
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            (
                "X-Amz-Credential",
                "AKIDEXAMPLE/20130524/us-east-1/s3/aws4_request",
            ),
            ("X-Amz-Date", "20130524T000000Z"),
            ("X-Amz-Expires", "86400"),
            ("X-Amz-SignedHeaders", "host"),
            (
                "X-Amz-Signature",
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
        ]
        .to_vec();

        for (override_name, override_value) in overrides {
            if let Some((_, value)) = query.iter_mut().find(|(name, _)| name == override_name) {
                *value = override_value;
            } else {
                query.push((override_name, override_value));
            }
        }

        query
    }

    fn query_refs(query: &[(String, String)]) -> Vec<(&str, &str)> {
        query
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }

    fn valid_iam_authorization() -> super::SigV4Authorization {
        parse_authorization_header(VALID_IAM_AUTHORIZATION).expect("valid authorization")
    }

    fn valid_iam_verification_request() -> SigV4VerificationRequest<'static> {
        SigV4VerificationRequest {
            request_datetime: IAM_REQUEST_DATETIME,
            method: "GET",
            path: "/",
            query: IAM_QUERY,
            headers: IAM_HEADERS,
            payload_hash: EMPTY_PAYLOAD_SHA256,
        }
    }

    fn presigned_signature(query: &[(&str, &str)], payload_hash: &str) -> String {
        let canonical_request = build_canonical_request(
            "GET",
            "/example-bucket/object.txt",
            query,
            &[("host", "examplebucket.s3.amazonaws.com")],
            &signed_headers(&["host"]),
            payload_hash,
        )
        .expect("canonical request");
        let signing_key = derive_signing_key(TEST_SECRET_ACCESS_KEY, "20130524", "us-east-1", "s3");
        let credential = super::parse_credential("AKIDEXAMPLE/20130524/us-east-1/s3/aws4_request")
            .expect("test credential");
        let string_to_sign =
            build_string_to_sign("20130524T000000Z", credential.scope(), &canonical_request);

        signature_hex(&signing_key, &string_to_sign)
    }
}
