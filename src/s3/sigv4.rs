// SPDX-License-Identifier: Apache-2.0

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};

const SIGV4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const CREDENTIAL_PARAM: &str = "Credential";
const SIGNED_HEADERS_PARAM: &str = "SignedHeaders";
const SIGNATURE_PARAM: &str = "Signature";
const CREDENTIAL_SCOPE_TERMINATOR: &str = "aws4_request";
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
        parse_authorization_header, verify_signature, SigV4Algorithm, SigV4AuthorizationParameter,
        SigV4ParseDiagnostic, SigV4VerificationDiagnostic, SigV4VerificationRequest,
        SignedHeaderName,
    };

    const VALID_AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260512/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF";
    const EMPTY_PAYLOAD_SHA256: &str =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

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
        let authorization = parse_authorization_header(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=host;x-amz-date, Signature=b2e4af44cfad96d9ffa3c5653674a927b9b0995c33de22e1f843745ce37c1d5e",
        )
        .expect("valid authorization");
        let verification = verify_signature(
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
        let authorization = parse_authorization_header(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=host;x-amz-date, Signature=b2e4af44cfad96d9ffa3c5653674a927b9b0995c33de22e1f843745ce37c1d5e",
        )
        .expect("valid authorization");
        let canonical = build_canonical_request(
            "GET",
            "/",
            &[("Action", "ListUsers"), ("Version", "2010-05-08")],
            &[
                ("Host", "iam.amazonaws.com"),
                ("X-Amz-Date", "20150830T123600Z"),
            ],
            authorization.signed_headers(),
            EMPTY_PAYLOAD_SHA256,
        )
        .expect("canonical request");
        let string_to_sign = build_string_to_sign(
            "20150830T123600Z",
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

    fn assert_parse_error(header: &str, expected: SigV4ParseDiagnostic) {
        assert_eq!(
            parse_authorization_header(header).expect_err("header should fail"),
            expected
        );
    }

    fn signed_headers(names: &[&str]) -> Vec<SignedHeaderName> {
        names
            .iter()
            .map(|name| SignedHeaderName((*name).to_owned()))
            .collect()
    }
}
