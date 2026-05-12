// SPDX-License-Identifier: Apache-2.0

use crate::s3::error::S3RequestId;
use crate::storage::Storage;
use crate::trace::{NoopTraceSink, TraceEvent, TraceSink};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use time::OffsetDateTime;

#[derive(Clone)]
pub struct ServerState {
    storage: Arc<dyn Storage + Send + Sync>,
    request_id_generator: Arc<dyn RequestIdGenerator>,
    trace_sink: Arc<dyn TraceSink>,
    auth_clock: Arc<dyn AuthClock>,
}

pub trait RequestIdGenerator: Send + Sync {
    fn next_request_id(&self) -> S3RequestId;
}

pub trait AuthClock: Send + Sync {
    fn now_utc(&self) -> OffsetDateTime;
}

#[derive(Debug, Default)]
pub struct SequentialRequestIdGenerator {
    next: AtomicU64,
}

#[derive(Debug)]
pub struct FixedRequestIdGenerator {
    request_id: S3RequestId,
}

#[derive(Debug, Default)]
pub struct SystemAuthClock;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FixedAuthClock {
    timestamp: OffsetDateTime,
}

impl ServerState {
    pub fn from_storage(storage: impl Storage + Send + Sync + 'static) -> Self {
        Self::with_request_id_generator(storage, SequentialRequestIdGenerator::default())
    }

    pub fn with_trace_sink(
        storage: impl Storage + Send + Sync + 'static,
        trace_sink: impl TraceSink + 'static,
    ) -> Self {
        Self::with_request_id_generator_and_trace_sink(
            storage,
            SequentialRequestIdGenerator::default(),
            trace_sink,
        )
    }

    pub fn with_request_id_generator(
        storage: impl Storage + Send + Sync + 'static,
        request_id_generator: impl RequestIdGenerator + 'static,
    ) -> Self {
        Self::with_request_id_generator_and_trace_sink(storage, request_id_generator, NoopTraceSink)
    }

    pub fn with_request_id_generator_and_trace_sink(
        storage: impl Storage + Send + Sync + 'static,
        request_id_generator: impl RequestIdGenerator + 'static,
        trace_sink: impl TraceSink + 'static,
    ) -> Self {
        Self {
            storage: Arc::new(storage),
            request_id_generator: Arc::new(request_id_generator),
            trace_sink: Arc::new(trace_sink),
            auth_clock: Arc::new(SystemAuthClock),
        }
    }

    pub fn with_auth_clock(mut self, auth_clock: impl AuthClock + 'static) -> Self {
        self.auth_clock = Arc::new(auth_clock);
        self
    }

    pub fn with_fixed_request_id(
        storage: impl Storage + Send + Sync + 'static,
        request_id: impl Into<String>,
    ) -> Self {
        Self::with_request_id_generator(storage, FixedRequestIdGenerator::new(request_id))
    }

    pub fn storage(&self) -> &(dyn Storage + Send + Sync) {
        self.storage.as_ref()
    }

    pub fn next_request_id(&self) -> S3RequestId {
        self.request_id_generator.next_request_id()
    }

    pub fn record_trace(&self, event: TraceEvent) {
        self.trace_sink.record(event);
    }

    pub fn auth_now_utc(&self) -> OffsetDateTime {
        self.auth_clock.now_utc()
    }
}

impl SequentialRequestIdGenerator {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RequestIdGenerator for SequentialRequestIdGenerator {
    fn next_request_id(&self) -> S3RequestId {
        let value = self.next.fetch_add(1, Ordering::Relaxed) + 1;

        S3RequestId::new(format!("s3lab-{value:016}"))
    }
}

impl FixedRequestIdGenerator {
    pub fn new(request_id: impl Into<String>) -> Self {
        Self {
            request_id: S3RequestId::new(request_id),
        }
    }
}

impl RequestIdGenerator for FixedRequestIdGenerator {
    fn next_request_id(&self) -> S3RequestId {
        self.request_id.clone()
    }
}

impl AuthClock for SystemAuthClock {
    fn now_utc(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

impl FixedAuthClock {
    pub fn new(timestamp: OffsetDateTime) -> Self {
        Self { timestamp }
    }
}

impl AuthClock for FixedAuthClock {
    fn now_utc(&self) -> OffsetDateTime {
        self.timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::{FixedAuthClock, RequestIdGenerator, SequentialRequestIdGenerator, ServerState};
    use crate::storage::fs::FilesystemStorage;
    use crate::trace::{
        RecordingTraceSink, RequestReceivedTrace, RouteResolvedTrace, TraceEvent, TraceS3Operation,
    };
    use time::{Date, Month, PrimitiveDateTime, Time};

    #[test]
    fn from_storage_wraps_filesystem_backed_state() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let state = ServerState::from_storage(FilesystemStorage::new(temp_dir.path()));

        assert_eq!(
            state
                .storage()
                .list_buckets()
                .expect("empty filesystem storage lists buckets"),
            []
        );
    }

    #[test]
    fn state_can_be_cloned_without_losing_storage_access() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let state = ServerState::from_storage(FilesystemStorage::new(temp_dir.path()));
        let cloned = state.clone();

        assert_eq!(
            cloned.storage().list_buckets().expect("list from clone"),
            state.storage().list_buckets().expect("list from original")
        );
    }

    #[test]
    fn from_storage_uses_noop_trace_sink() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let state = ServerState::from_storage(FilesystemStorage::new(temp_dir.path()));

        state.record_trace(TraceEvent::RequestReceived(RequestReceivedTrace::new(
            "s3lab-0000000000000001",
            "GET",
            "/",
            ["authorization"],
        )));
    }

    #[test]
    fn trace_sink_can_be_injected_and_records_events() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let sink = RecordingTraceSink::default();
        let recorded = sink.clone();
        let state = ServerState::with_trace_sink(FilesystemStorage::new(temp_dir.path()), sink);
        let event = TraceEvent::RouteResolved(RouteResolvedTrace::new(
            "s3lab-0000000000000001",
            "GET",
            "/",
            TraceS3Operation::ListBuckets,
        ));

        state.record_trace(event.clone());

        assert_eq!(recorded.events(), vec![event]);
    }

    #[test]
    fn sequential_request_ids_are_process_local_and_zero_padded() {
        let generator = SequentialRequestIdGenerator::new();

        assert_eq!(
            generator.next_request_id().as_str(),
            "s3lab-0000000000000001"
        );
        assert_eq!(
            generator.next_request_id().as_str(),
            "s3lab-0000000000000002"
        );
    }

    #[test]
    fn auth_clock_can_be_injected_for_deterministic_validation() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let timestamp = PrimitiveDateTime::new(
            Date::from_calendar_date(2026, Month::May, 12).expect("valid test date"),
            Time::from_hms(1, 2, 30).expect("valid test time"),
        )
        .assume_utc();
        let state = ServerState::from_storage(FilesystemStorage::new(temp_dir.path()))
            .with_auth_clock(FixedAuthClock::new(timestamp));

        assert_eq!(state.auth_now_utc(), timestamp);
    }
}
