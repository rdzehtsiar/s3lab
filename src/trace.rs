// SPDX-License-Identifier: Apache-2.0

use sha2::{Digest, Sha256};
#[cfg(test)]
use std::sync::{Arc, Mutex};

use crate::encoding::hex_encode;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TraceEvent {
    RequestReceived(RequestReceivedTrace),
    RouteResolved(RouteResolvedTrace),
    RouteRejected(RouteRejectedTrace),
    SigV4Parsed(SigV4ParsedTrace),
    CanonicalRequestBuilt(CanonicalRequestBuiltTrace),
    AuthDecision(AuthDecisionTrace),
    PayloadVerification(PayloadVerificationTrace),
    StorageMutation(StorageMutationTrace),
    ResponseSent(ResponseSentTrace),
}

pub trait TraceSink: Send + Sync {
    fn record(&self, event: TraceEvent);
}

#[derive(Debug, Default)]
pub struct NoopTraceSink;

impl TraceSink for NoopTraceSink {
    fn record(&self, _event: TraceEvent) {}
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RequestReceivedTrace {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub header_names: Vec<String>,
}

impl RequestReceivedTrace {
    /// Builds a request-received trace while storing only the path component.
    pub fn new(
        request_id: impl Into<String>,
        method: impl Into<String>,
        path: impl Into<String>,
        header_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            method: method.into(),
            path: trace_path_without_query(path),
            header_names: normalized_values(header_names),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RouteResolvedTrace {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub operation: TraceS3Operation,
}

impl RouteResolvedTrace {
    /// Builds a route-resolved trace while storing only the path component.
    pub fn new(
        request_id: impl Into<String>,
        method: impl Into<String>,
        path: impl Into<String>,
        operation: TraceS3Operation,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            method: method.into(),
            path: trace_path_without_query(path),
            operation,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RouteRejectedTrace {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub reason: RouteRejectionReason,
}

impl RouteRejectedTrace {
    /// Builds a route-rejected trace while storing only the path component.
    pub fn new(
        request_id: impl Into<String>,
        method: impl Into<String>,
        path: impl Into<String>,
        reason: RouteRejectionReason,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            method: method.into(),
            path: trace_path_without_query(path),
            reason,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SigV4ParsedTrace {
    pub request_id: String,
    pub outcome: SigV4ParseOutcome,
    pub credential_scope: Option<TraceCredentialScope>,
    pub signed_headers: Vec<String>,
}

impl SigV4ParsedTrace {
    pub fn accepted(
        request_id: impl Into<String>,
        credential_scope: TraceCredentialScope,
        signed_headers: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            outcome: SigV4ParseOutcome::Accepted,
            credential_scope: Some(credential_scope),
            signed_headers: normalized_values(signed_headers),
        }
    }

    pub fn rejected(request_id: impl Into<String>, reason: SigV4ParseRejection) -> Self {
        Self {
            request_id: request_id.into(),
            outcome: SigV4ParseOutcome::Rejected { reason },
            credential_scope: None,
            signed_headers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CanonicalRequestBuiltTrace {
    pub request_id: String,
    pub signed_headers: Vec<String>,
    pub canonical_request_sha256: String,
}

impl CanonicalRequestBuiltTrace {
    pub fn from_canonical_request(
        request_id: impl Into<String>,
        signed_headers: impl IntoIterator<Item = impl Into<String>>,
        canonical_request: &str,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            signed_headers: normalized_values(signed_headers),
            canonical_request_sha256: sha256_hex(canonical_request.as_bytes()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthDecisionTrace {
    pub request_id: String,
    pub decision: AuthDecision,
}

impl AuthDecisionTrace {
    pub fn new(request_id: impl Into<String>, decision: AuthDecision) -> Self {
        Self {
            request_id: request_id.into(),
            decision,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PayloadVerificationTrace {
    pub request_id: String,
    pub outcome: PayloadVerificationOutcome,
}

impl PayloadVerificationTrace {
    pub fn new(request_id: impl Into<String>, outcome: PayloadVerificationOutcome) -> Self {
        Self {
            request_id: request_id.into(),
            outcome,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StorageMutationTrace {
    pub request_id: String,
    pub mutation: StorageMutation,
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub outcome: StorageMutationOutcome,
}

impl StorageMutationTrace {
    pub fn new(
        request_id: impl Into<String>,
        mutation: StorageMutation,
        bucket: Option<impl Into<String>>,
        key: Option<impl Into<String>>,
        outcome: StorageMutationOutcome,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            mutation,
            bucket: bucket.map(Into::into),
            key: key.map(Into::into),
            outcome,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResponseSentTrace {
    pub request_id: String,
    pub status_code: u16,
    pub error_code: Option<String>,
}

impl ResponseSentTrace {
    pub fn new(
        request_id: impl Into<String>,
        status_code: u16,
        error_code: Option<impl Into<String>>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            status_code,
            error_code: error_code.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TraceS3Operation {
    ListBuckets,
    CreateBucket,
    HeadBucket,
    DeleteBucket,
    PutObject,
    GetObject,
    HeadObject,
    DeleteObject,
    ListObjectsV2,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RouteRejectionReason {
    InvalidPath,
    InvalidQuery,
    UnsupportedSubresource,
    UnsupportedOperation,
    MethodNotAllowed,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SigV4ParseOutcome {
    Accepted,
    Rejected { reason: SigV4ParseRejection },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TraceCredentialScope {
    pub date: String,
    pub region: String,
    pub service: String,
}

impl TraceCredentialScope {
    pub fn new(
        date: impl Into<String>,
        region: impl Into<String>,
        service: impl Into<String>,
    ) -> Self {
        Self {
            date: date.into(),
            region: region.into(),
            service: service.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SigV4ParseRejection {
    MissingAuthorization,
    MalformedAuthorization,
    UnsupportedAlgorithm,
    InvalidCredentialScope,
    InvalidSignedHeaders,
    InvalidSignature,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AuthDecision {
    Accepted,
    UnsignedAccepted,
    Rejected(AuthRejectionReason),
    NotConfigured,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PayloadVerificationOutcome {
    Full,
    Partial(PayloadVerificationPartialReason),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PayloadVerificationPartialReason {
    UnsignedPayloadMarker,
    StreamingPayloadMarker,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AuthRejectionReason {
    MissingAuthorization,
    InvalidAuthorization,
    InvalidAccessKey,
    MissingSignedHeader,
    SignatureMismatch,
    Unsupported,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StorageMutation {
    CreateBucket,
    DeleteBucket,
    PutObject,
    DeleteObject,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StorageMutationOutcome {
    Applied,
    Rejected { error_code: String },
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct RecordingTraceSink {
    events: Arc<Mutex<Vec<TraceEvent>>>,
}

#[cfg(test)]
impl RecordingTraceSink {
    pub(crate) fn events(&self) -> Vec<TraceEvent> {
        self.events.lock().expect("recorded trace lock").clone()
    }
}

#[cfg(test)]
impl TraceSink for RecordingTraceSink {
    fn record(&self, event: TraceEvent) {
        self.events.lock().expect("recorded trace lock").push(event);
    }
}

fn normalized_values(values: impl IntoIterator<Item = impl Into<String>>) -> Vec<String> {
    let mut values = values.into_iter().map(Into::into).collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn trace_path_without_query(path: impl Into<String>) -> String {
    let path = path.into();

    match path.split_once('?') {
        Some((path, _query)) => path.to_owned(),
        None => path,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_encode(&digest)
}

#[cfg(test)]
mod tests {
    use super::{
        AuthDecision, CanonicalRequestBuiltTrace, PayloadVerificationOutcome,
        PayloadVerificationPartialReason, PayloadVerificationTrace, RequestReceivedTrace,
        RouteRejectedTrace, RouteRejectionReason, RouteResolvedTrace, SigV4ParsedTrace,
        TraceCredentialScope, TraceS3Operation,
    };

    #[test]
    fn request_received_trace_sorts_and_deduplicates_header_names() {
        let trace = RequestReceivedTrace::new(
            "s3lab-0000000000000001",
            "GET",
            "/bucket/object",
            ["x-amz-date", "authorization", "x-amz-date"],
        );

        assert_eq!(trace.header_names, ["authorization", "x-amz-date"]);
    }

    #[test]
    fn request_received_trace_stores_path_without_query_credentials() {
        let trace = RequestReceivedTrace::new(
            "s3lab-0000000000000001",
            "GET",
            "/bucket/object?X-Amz-Credential=AKIAIOSFODNN7EXAMPLE&X-Amz-Signature=secret",
            ["host"],
        );
        let debug = format!("{trace:?}");

        assert_eq!(trace.path, "/bucket/object");
        assert!(!debug.contains("X-Amz-Credential"));
        assert!(!debug.contains("X-Amz-Signature"));
        assert!(!debug.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn route_resolved_trace_stores_path_without_query_credentials() {
        let trace = RouteResolvedTrace::new(
            "s3lab-0000000000000001",
            "GET",
            "/bucket/object?X-Amz-Credential=AKIAIOSFODNN7EXAMPLE&X-Amz-Signature=secret",
            TraceS3Operation::GetObject,
        );
        let debug = format!("{trace:?}");

        assert_eq!(trace.path, "/bucket/object");
        assert!(!debug.contains("X-Amz-Credential"));
        assert!(!debug.contains("X-Amz-Signature"));
        assert!(!debug.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn route_rejected_trace_stores_path_without_query_credentials() {
        let trace = RouteRejectedTrace::new(
            "s3lab-0000000000000001",
            "GET",
            "/bucket/object?X-Amz-Credential=AKIAIOSFODNN7EXAMPLE&X-Amz-Signature=secret",
            RouteRejectionReason::UnsupportedSubresource,
        );
        let debug = format!("{trace:?}");

        assert_eq!(trace.path, "/bucket/object");
        assert!(!debug.contains("X-Amz-Credential"));
        assert!(!debug.contains("X-Amz-Signature"));
        assert!(!debug.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn canonical_request_trace_stores_hash_not_canonical_request() {
        let canonical_request = "GET\n/private/key\nX-Amz-Signature=super-secret\n\
             authorization:AWS4-HMAC-SHA256 Credential=SECRET\n\n\
             authorization\npayload-hash";
        let trace = CanonicalRequestBuiltTrace::from_canonical_request(
            "s3lab-0000000000000001",
            ["x-amz-date", "host"],
            canonical_request,
        );
        let debug = format!("{trace:?}");

        assert_eq!(trace.canonical_request_sha256.len(), 64);
        assert!(!debug.contains(canonical_request));
        assert!(!debug.contains("super-secret"));
        assert!(!debug.contains("Credential=SECRET"));
        assert!(!debug.contains("authorization:AWS4-HMAC-SHA256"));
    }

    #[test]
    fn sigv4_parsed_trace_omits_access_key_signature_and_authorization_value() {
        let trace = SigV4ParsedTrace::accepted(
            "s3lab-0000000000000001",
            TraceCredentialScope::new("20260512", "us-east-1", "s3"),
            ["host", "x-amz-date", "authorization"],
        );
        let debug = format!("{trace:?}");

        assert!(debug.contains("20260512"));
        assert!(debug.contains("us-east-1"));
        assert!(!debug.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug.contains("Signature="));
        assert!(!debug.contains("AWS4-HMAC-SHA256 Credential="));
    }

    #[test]
    fn auth_decision_trace_uses_typed_reasons_without_secret_text() {
        let trace = super::AuthDecisionTrace::new(
            "s3lab-0000000000000001",
            AuthDecision::Rejected(super::AuthRejectionReason::SignatureMismatch),
        );
        let debug = format!("{trace:?}");

        assert!(debug.contains("SignatureMismatch"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("0000000000000000000000000000000000000000000000000000000000000000"));
    }

    #[test]
    fn payload_verification_trace_uses_typed_partial_reasons() {
        let trace = PayloadVerificationTrace::new(
            "s3lab-0000000000000001",
            PayloadVerificationOutcome::Partial(
                PayloadVerificationPartialReason::UnsignedPayloadMarker,
            ),
        );
        let debug = format!("{trace:?}");

        assert!(debug.contains("UnsignedPayloadMarker"));
        assert!(!debug.contains("sent-secret-body"));
        assert!(!debug.contains("Signature="));
    }
}
