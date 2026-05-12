// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::encoding::hex_encode;

pub const DEFAULT_TRACE_STORE_REQUEST_CAPACITY: usize = 256;

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
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

#[derive(Debug, Clone)]
pub struct InMemoryTraceStore {
    inner: Arc<Mutex<TraceStoreInner>>,
    max_requests: usize,
}

#[derive(Debug, Default)]
struct TraceStoreInner {
    request_order: VecDeque<String>,
    events: VecDeque<TraceEvent>,
}

#[derive(Debug, Default)]
pub struct NoopTraceSink;

impl TraceSink for NoopTraceSink {
    fn record(&self, _event: TraceEvent) {}
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct RequestTraceSummary {
    pub request_id: String,
    pub method: Option<String>,
    pub path: Option<String>,
    pub operation: Option<TraceS3Operation>,
    pub status_code: Option<u16>,
    pub event_count: usize,
}

impl InMemoryTraceStore {
    pub fn new(max_requests: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TraceStoreInner::default())),
            max_requests,
        }
    }

    pub fn request_summaries(&self) -> Vec<RequestTraceSummary> {
        let inner = self.inner.lock().expect("trace store lock");
        let mut summaries = BTreeMap::<String, RequestTraceSummary>::new();

        for event in &inner.events {
            let request_id = event.request_id().to_owned();
            let summary =
                summaries
                    .entry(request_id.clone())
                    .or_insert_with(|| RequestTraceSummary {
                        request_id,
                        method: None,
                        path: None,
                        operation: None,
                        status_code: None,
                        event_count: 0,
                    });

            summary.event_count += 1;
            if summary.method.is_none() {
                summary.method = event.method().map(str::to_owned);
            }
            if summary.path.is_none() {
                summary.path = event.path().map(str::to_owned);
            }
            if summary.operation.is_none() {
                summary.operation = event.operation();
            }
            if let TraceEvent::ResponseSent(trace) = event {
                summary.status_code = Some(trace.status_code);
            }
        }

        summaries.into_values().collect()
    }

    pub fn request_events(&self, request_id: &str) -> Option<Vec<TraceEvent>> {
        let inner = self.inner.lock().expect("trace store lock");
        let events = inner
            .events
            .iter()
            .filter(|event| event.request_id() == request_id)
            .cloned()
            .collect::<Vec<_>>();

        (!events.is_empty()).then_some(events)
    }
}

impl Default for InMemoryTraceStore {
    fn default() -> Self {
        Self::new(DEFAULT_TRACE_STORE_REQUEST_CAPACITY)
    }
}

impl TraceSink for InMemoryTraceStore {
    fn record(&self, event: TraceEvent) {
        if self.max_requests == 0 {
            return;
        }

        let request_id = event.request_id().to_owned();
        let mut inner = self.inner.lock().expect("trace store lock");

        if !inner.request_order.iter().any(|known| known == &request_id) {
            inner.request_order.push_back(request_id);
            while inner.request_order.len() > self.max_requests {
                if let Some(evicted_request_id) = inner.request_order.pop_front() {
                    inner
                        .events
                        .retain(|event| event.request_id() != evicted_request_id);
                }
            }
        }

        inner.events.push_back(event);
    }
}

impl TraceEvent {
    pub fn request_id(&self) -> &str {
        match self {
            Self::RequestReceived(trace) => &trace.request_id,
            Self::RouteResolved(trace) => &trace.request_id,
            Self::RouteRejected(trace) => &trace.request_id,
            Self::SigV4Parsed(trace) => &trace.request_id,
            Self::CanonicalRequestBuilt(trace) => &trace.request_id,
            Self::AuthDecision(trace) => &trace.request_id,
            Self::PayloadVerification(trace) => &trace.request_id,
            Self::StorageMutation(trace) => &trace.request_id,
            Self::ResponseSent(trace) => &trace.request_id,
        }
    }

    fn method(&self) -> Option<&str> {
        match self {
            Self::RequestReceived(trace) => Some(&trace.method),
            Self::RouteResolved(trace) => Some(&trace.method),
            Self::RouteRejected(trace) => Some(&trace.method),
            Self::SigV4Parsed(_)
            | Self::CanonicalRequestBuilt(_)
            | Self::AuthDecision(_)
            | Self::PayloadVerification(_)
            | Self::StorageMutation(_)
            | Self::ResponseSent(_) => None,
        }
    }

    fn path(&self) -> Option<&str> {
        match self {
            Self::RequestReceived(trace) => Some(&trace.path),
            Self::RouteResolved(trace) => Some(&trace.path),
            Self::RouteRejected(trace) => Some(&trace.path),
            Self::SigV4Parsed(_)
            | Self::CanonicalRequestBuilt(_)
            | Self::AuthDecision(_)
            | Self::PayloadVerification(_)
            | Self::StorageMutation(_)
            | Self::ResponseSent(_) => None,
        }
    }

    fn operation(&self) -> Option<TraceS3Operation> {
        match self {
            Self::RouteResolved(trace) => Some(trace.operation),
            Self::RequestReceived(_)
            | Self::RouteRejected(_)
            | Self::SigV4Parsed(_)
            | Self::CanonicalRequestBuilt(_)
            | Self::AuthDecision(_)
            | Self::PayloadVerification(_)
            | Self::StorageMutation(_)
            | Self::ResponseSent(_) => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceS3Operation {
    ListBuckets,
    CreateBucket,
    HeadBucket,
    DeleteBucket,
    PutObject,
    GetObject,
    HeadObject,
    DeleteObject,
    CreateMultipartUpload,
    UploadPart,
    ListParts,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListObjectsV2,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteRejectionReason {
    InvalidPath,
    InvalidQuery,
    UnsupportedSubresource,
    UnsupportedOperation,
    MethodNotAllowed,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SigV4ParseOutcome {
    Accepted,
    Rejected { reason: SigV4ParseRejection },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SigV4ParseRejection {
    MissingAuthorization,
    MalformedAuthorization,
    UnsupportedAlgorithm,
    InvalidCredentialScope,
    InvalidSignedHeaders,
    InvalidSignature,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthDecision {
    Accepted,
    UnsignedAccepted,
    Rejected(AuthRejectionReason),
    NotConfigured,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadVerificationOutcome {
    Full,
    Partial(PayloadVerificationPartialReason),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadVerificationPartialReason {
    UnsignedPayloadMarker,
    StreamingPayloadMarker,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthRejectionReason {
    MissingAuthorization,
    InvalidAuthorization,
    InvalidAccessKey,
    MissingSignedHeader,
    SignatureMismatch,
    Unsupported,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageMutation {
    CreateBucket,
    DeleteBucket,
    PutObject,
    DeleteObject,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
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
        AuthDecision, CanonicalRequestBuiltTrace, InMemoryTraceStore, PayloadVerificationOutcome,
        PayloadVerificationPartialReason, PayloadVerificationTrace, RequestReceivedTrace,
        ResponseSentTrace, RouteRejectedTrace, RouteRejectionReason, RouteResolvedTrace,
        SigV4ParsedTrace, StorageMutation, StorageMutationOutcome, StorageMutationTrace,
        TraceCredentialScope, TraceEvent, TraceS3Operation, TraceSink,
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

    #[test]
    fn multipart_storage_mutation_trace_is_typed_and_secret_free() {
        let trace = StorageMutationTrace::new(
            "s3lab-0000000000000001",
            StorageMutation::UploadPart,
            Some("bucket"),
            Some("object.txt"),
            StorageMutationOutcome::Applied,
        );
        let debug = format!("{trace:?}");

        assert!(debug.contains("UploadPart"));
        assert!(!debug.contains("upload-secret-token"));
        assert!(!debug.contains("part-secret-body"));
        assert!(!debug.contains("Signature="));
    }

    #[test]
    fn in_memory_trace_store_groups_summaries_in_stable_request_id_order() {
        let store = InMemoryTraceStore::new(4);
        store.record(TraceEvent::RequestReceived(RequestReceivedTrace::new(
            "s3lab-0000000000000002",
            "PUT",
            "/bucket/object?X-Amz-Signature=secret",
            ["authorization", "host"],
        )));
        store.record(TraceEvent::ResponseSent(ResponseSentTrace::new(
            "s3lab-0000000000000002",
            200,
            None::<String>,
        )));
        store.record(TraceEvent::RequestReceived(RequestReceivedTrace::new(
            "s3lab-0000000000000001",
            "GET",
            "/",
            ["host"],
        )));
        store.record(TraceEvent::RouteResolved(RouteResolvedTrace::new(
            "s3lab-0000000000000001",
            "GET",
            "/",
            TraceS3Operation::ListBuckets,
        )));

        let summaries = store.request_summaries();

        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.request_id.as_str())
                .collect::<Vec<_>>(),
            ["s3lab-0000000000000001", "s3lab-0000000000000002"]
        );
        assert_eq!(summaries[0].operation, Some(TraceS3Operation::ListBuckets));
        assert_eq!(summaries[0].event_count, 2);
        assert_eq!(summaries[1].path.as_deref(), Some("/bucket/object"));
        assert_eq!(summaries[1].status_code, Some(200));
    }

    #[test]
    fn in_memory_trace_store_bounds_complete_request_groups() {
        let store = InMemoryTraceStore::new(2);
        store.record(TraceEvent::RequestReceived(RequestReceivedTrace::new(
            "s3lab-0000000000000001",
            "GET",
            "/first",
            ["host"],
        )));
        store.record(TraceEvent::ResponseSent(ResponseSentTrace::new(
            "s3lab-0000000000000001",
            404,
            Some("NoSuchKey"),
        )));
        store.record(TraceEvent::RequestReceived(RequestReceivedTrace::new(
            "s3lab-0000000000000002",
            "GET",
            "/second",
            ["host"],
        )));
        store.record(TraceEvent::RequestReceived(RequestReceivedTrace::new(
            "s3lab-0000000000000003",
            "GET",
            "/third",
            ["host"],
        )));

        assert!(store.request_events("s3lab-0000000000000001").is_none());
        assert_eq!(
            store
                .request_summaries()
                .iter()
                .map(|summary| summary.request_id.as_str())
                .collect::<Vec<_>>(),
            ["s3lab-0000000000000002", "s3lab-0000000000000003"]
        );
    }
}
