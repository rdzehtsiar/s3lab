// SPDX-License-Identifier: Apache-2.0

use crate::s3::bucket::{is_valid_s3_bucket_name, BucketName};
use crate::s3::error::{S3Error, S3ErrorCode, S3RequestId};
use crate::s3::object::{is_valid_s3_object_key, is_valid_s3_object_key_prefix, ObjectKey};
use crate::s3::operation::{ListObjectsEncoding, S3Operation};
use crate::s3::sigv4::{
    build_canonical_request, parse_authorization_header, parse_query_authorization,
    verify_query_signature, verify_signature, SigV4Authorization, SigV4CredentialScope,
    SigV4ParseDiagnostic, SigV4QueryAuthorization, SigV4QueryParseDiagnostic,
    SigV4QueryVerificationRequest, SigV4VerificationDiagnostic, SigV4VerificationRequest,
    SignedHeaderName,
};
use crate::s3::time::http_date;
use crate::s3::xml::{
    error_response_xml, list_buckets_response_xml, list_objects_v2_response_xml, ListBucketXml,
    ListBucketsXml, ListObjectXml, ListObjectsV2Xml, ListObjectsV2XmlEntry, XML_CONTENT_TYPE,
};
use crate::server::state::ServerState;
use crate::storage::{
    BucketSummary, ListObjectsOptions, ObjectListing, ObjectListingEntry, PutObjectRequest,
    StorageError, StoredObject, StoredObjectMetadata,
};
use crate::trace::{
    AuthDecision, AuthDecisionTrace, AuthRejectionReason, CanonicalRequestBuiltTrace,
    PayloadVerificationOutcome, PayloadVerificationPartialReason, PayloadVerificationTrace,
    RequestReceivedTrace, ResponseSentTrace, RouteRejectedTrace, RouteRejectionReason,
    RouteResolvedTrace, SigV4ParseRejection, SigV4ParsedTrace, StorageMutation,
    StorageMutationOutcome, StorageMutationTrace, TraceCredentialScope, TraceEvent,
    TraceS3Operation,
};
use axum::body::{to_bytes, Body, Bytes};
use axum::extract::State;
use axum::http::header::{
    AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE,
};
use axum::http::HeaderName;
use axum::http::{HeaderMap, HeaderValue, Method, Response, StatusCode, Uri};
use http_body_util::LengthLimitError;
use percent_encoding::percent_decode_str;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::path::PathBuf;
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time};

const REQUEST_ID_HEADER: &str = "x-amz-request-id";
const USER_METADATA_HEADER_PREFIX: &str = "x-amz-meta-";
const LOCAL_ACCESS_KEY_ID: &str = "s3lab";
const LOCAL_SECRET_ACCESS_KEY: &str = "s3lab-secret";
const X_AMZ_CONTENT_SHA256: &str = "x-amz-content-sha256";
const X_AMZ_DATE: &str = "x-amz-date";
const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// Phase 1 buffers PUT object bodies explicitly while allowing objects above Axum's 2 MiB default.
pub const PHASE1_MAX_PUT_OBJECT_BODY_BYTES: usize = 8 * 1024 * 1024;
const LIST_TYPE: &str = "list-type";
const PREFIX: &str = "prefix";
const CONTINUATION_TOKEN: &str = "continuation-token";
const MAX_KEYS: &str = "max-keys";
const DELIMITER: &str = "delimiter";
const ENCODING_TYPE: &str = "encoding-type";
const START_AFTER: &str = "start-after";
const FETCH_OWNER: &str = "fetch-owner";
const X_ID: &str = "x-id";
const X_AMZ_CHECKSUM_ALGORITHM: &str = "x-amz-checksum-algorithm";
const X_AMZ_CHECKSUM_CRC32: &str = "x-amz-checksum-crc32";
const X_AMZ_CHECKSUM_CRC64NVME: &str = "x-amz-checksum-crc64nvme";
const X_AMZ_DECODED_CONTENT_LENGTH: &str = "x-amz-decoded-content-length";
const X_AMZ_SDK_CHECKSUM_ALGORITHM: &str = "x-amz-sdk-checksum-algorithm";
const X_AMZ_TRAILER: &str = "x-amz-trailer";
const X_AMZ_ALGORITHM_QUERY: &str = "X-Amz-Algorithm";
const X_AMZ_CREDENTIAL_QUERY: &str = "X-Amz-Credential";
const X_AMZ_DATE_QUERY: &str = "X-Amz-Date";
const X_AMZ_EXPIRES_QUERY: &str = "X-Amz-Expires";
const X_AMZ_SIGNED_HEADERS_QUERY: &str = "X-Amz-SignedHeaders";
const X_AMZ_SIGNATURE_QUERY: &str = "X-Amz-Signature";
const X_AMZ_CONTENT_SHA256_QUERY: &str = "X-Amz-Content-Sha256";
const X_AMZ_SECURITY_TOKEN_QUERY: &str = "X-Amz-Security-Token";
const RESPONSE_CACHE_CONTROL_QUERY: &str = "response-cache-control";
const RESPONSE_CONTENT_DISPOSITION_QUERY: &str = "response-content-disposition";
const RESPONSE_CONTENT_ENCODING_QUERY: &str = "response-content-encoding";
const RESPONSE_CONTENT_LANGUAGE_QUERY: &str = "response-content-language";
const RESPONSE_CONTENT_TYPE_QUERY: &str = "response-content-type";
const RESPONSE_EXPIRES_QUERY: &str = "response-expires";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const STREAMING_PAYLOAD_PREFIX: &str = "STREAMING-";
const MAX_PRESIGNED_EXPIRES_SECONDS: u32 = 604_800;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RouteScope {
    PathStyle,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RouteMatch {
    pub scope: RouteScope,
    pub operation: S3Operation,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RouteRejection {
    pub code: S3ErrorCode,
    pub resource: String,
}

pub fn resolve_operation(method: &Method, uri: &Uri) -> Result<RouteMatch, RouteRejection> {
    let resource = resource_for_uri(uri);
    let route_resource = parse_path(uri.path(), &resource)?;
    let query = parse_query(uri.query(), &resource)?;

    if query.has_unsupported_subresource() {
        return Err(RouteRejection::new(S3ErrorCode::NotImplemented, resource));
    }

    let operation = match route_resource {
        RouteResource::Root => resolve_root_operation(method, &query, &resource)?,
        RouteResource::Bucket { bucket } => {
            resolve_bucket_operation(method, bucket, &query, &resource)?
        }
        RouteResource::Object { bucket, key } => {
            resolve_object_operation(method, bucket, key, &query, &resource)?
        }
    };

    Ok(RouteMatch {
        scope: RouteScope::PathStyle,
        operation,
    })
}

pub async fn handle_request(
    State(state): State<ServerState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response<Body> {
    let is_head = method == Method::HEAD;
    let request_id = state.next_request_id();
    record_request_received(&state, &method, &uri, &headers, &request_id);

    let response = match resolve_operation(&method, &uri) {
        Ok(route_match) => {
            record_route_resolved(&state, &method, &uri, &route_match.operation, &request_id);
            if let Some(response) =
                auth_rejection_response(&state, &method, &uri, &headers, is_head, &request_id)
            {
                response
            } else {
                execute_operation(
                    state.clone(),
                    headers,
                    body,
                    route_match.operation,
                    uri.clone(),
                    is_head,
                    request_id.clone(),
                )
                .await
            }
        }
        Err(rejection) => {
            record_route_rejected(&state, &method, &uri, &rejection, &request_id);
            route_error_response(rejection, is_head, &request_id)
        }
    };
    record_response_sent(&state, &request_id, &response);
    log_request_outcome(&method, &uri, response.status(), &request_id);
    response
}

fn log_request_outcome(method: &Method, uri: &Uri, status: StatusCode, request_id: &S3RequestId) {
    tracing::info!(
        method = %method,
        path = %safe_log_path(uri),
        status = status.as_u16(),
        request_id = %request_id.as_str(),
        "request completed"
    );
}

fn safe_log_path(uri: &Uri) -> &str {
    uri.path()
}

fn auth_rejection_response(
    state: &ServerState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    is_head: bool,
    request_id: &S3RequestId,
) -> Option<Response<Body>> {
    let resource = resource_for_uri(uri);
    let query_pairs = match sigv4_query_pairs(uri.query(), &resource, is_head, request_id) {
        Ok(query_pairs) => query_pairs,
        Err(response) => {
            return invalid_authorization_response(state, request_id, *response);
        }
    };
    let has_query_authorization = has_presigned_query_authorization(&query_pairs);
    let authorization = match authorization_header(headers, &resource, is_head, request_id) {
        Ok(Some(authorization)) => {
            if has_query_authorization {
                state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
                    request_id.as_str(),
                    AuthDecision::Rejected(AuthRejectionReason::InvalidAuthorization),
                )));
                return Some(auth_error_response(
                    S3ErrorCode::AuthorizationHeaderMalformed,
                    "Use either an Authorization header or SigV4 presigned query parameters, not both.",
                    &resource,
                    is_head,
                    request_id,
                ));
            }
            authorization
        }
        Ok(None) => {
            if has_query_authorization {
                return query_auth_rejection_response(
                    state,
                    method,
                    uri,
                    headers,
                    query_pairs,
                    is_head,
                    request_id,
                );
            }
            state.record_trace(TraceEvent::SigV4Parsed(SigV4ParsedTrace::rejected(
                request_id.as_str(),
                SigV4ParseRejection::MissingAuthorization,
            )));
            state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
                request_id.as_str(),
                AuthDecision::UnsignedAccepted,
            )));
            return None;
        }
        Err(response) => {
            state.record_trace(TraceEvent::SigV4Parsed(SigV4ParsedTrace::rejected(
                request_id.as_str(),
                SigV4ParseRejection::MalformedAuthorization,
            )));
            state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
                request_id.as_str(),
                AuthDecision::Rejected(AuthRejectionReason::InvalidAuthorization),
            )));
            return Some(*response);
        }
    };

    let authorization = match parse_authorization_header(authorization) {
        Ok(authorization) => {
            record_sigv4_parsed(state, request_id, &authorization);
            authorization
        }
        Err(diagnostic) => {
            state.record_trace(TraceEvent::SigV4Parsed(SigV4ParsedTrace::rejected(
                request_id.as_str(),
                sigv4_parse_rejection(&diagnostic),
            )));
            state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
                request_id.as_str(),
                AuthDecision::Rejected(AuthRejectionReason::InvalidAuthorization),
            )));
            return Some(auth_error_response(
                S3ErrorCode::AuthorizationHeaderMalformed,
                diagnostic.message(),
                &resource,
                is_head,
                request_id,
            ));
        }
    };

    if let Some(response) = invalid_access_key_response(
        state,
        authorization.credential().access_key_id(),
        &resource,
        is_head,
        request_id,
    ) {
        return Some(response);
    }

    let verification_request = match owned_sigv4_verification_request(
        method, uri, headers, &resource, is_head, request_id,
    ) {
        Ok(request) => request,
        Err(response) => {
            return invalid_authorization_response(state, request_id, *response);
        }
    };
    let query = verification_request.query_refs();
    let header_values = verification_request.header_refs();
    let request = SigV4VerificationRequest {
        request_datetime: &verification_request.request_datetime,
        method: &verification_request.method,
        path: &verification_request.path,
        query: &query,
        headers: &header_values,
        payload_hash: &verification_request.payload_hash,
    };

    if let Some(response) = record_canonical_request_built(
        state,
        request_id,
        CanonicalRequestBuildInput {
            method: request.method,
            path: request.path,
            query: request.query,
            headers: request.headers,
            signed_headers: authorization.signed_headers(),
            payload_hash: request.payload_hash,
        },
        AuthErrorContext {
            resource: &resource,
            is_head,
        },
    ) {
        return Some(response);
    }

    sigv4_verification_response(
        state,
        request_id,
        verify_signature(&authorization, LOCAL_SECRET_ACCESS_KEY, request),
        AuthErrorContext {
            resource: &resource,
            is_head,
        },
    )
}

fn query_auth_rejection_response(
    state: &ServerState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    query_pairs: Vec<(String, String)>,
    is_head: bool,
    request_id: &S3RequestId,
) -> Option<Response<Body>> {
    let resource = resource_for_uri(uri);
    let query_refs = query_pairs
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let authorization = match parse_query_authorization(&query_refs) {
        Ok(authorization) => {
            record_sigv4_query_parsed(state, request_id, &authorization);
            authorization
        }
        Err(diagnostic) => {
            state.record_trace(TraceEvent::SigV4Parsed(SigV4ParsedTrace::rejected(
                request_id.as_str(),
                sigv4_query_parse_rejection(&diagnostic),
            )));
            state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
                request_id.as_str(),
                AuthDecision::Rejected(AuthRejectionReason::InvalidAuthorization),
            )));
            return Some(auth_error_response(
                S3ErrorCode::AuthorizationHeaderMalformed,
                diagnostic.message(),
                &resource,
                is_head,
                request_id,
            ));
        }
    };

    if let Some(response) = invalid_access_key_response(
        state,
        authorization.credential().access_key_id(),
        &resource,
        is_head,
        request_id,
    ) {
        return Some(response);
    }

    if let Some(response) = presigned_expiration_rejection_response(
        state,
        &authorization,
        &resource,
        is_head,
        request_id,
    ) {
        return Some(response);
    }

    let header_values = match sigv4_header_pairs(headers, &resource, is_head, request_id) {
        Ok(headers) => headers,
        Err(response) => {
            return invalid_authorization_response(state, request_id, *response);
        }
    };
    let header_refs = header_values
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let method = method.as_str();
    let path = uri.path();

    let query_without_signature = query_refs
        .iter()
        .copied()
        .filter(|(name, _)| *name != X_AMZ_SIGNATURE_QUERY)
        .collect::<Vec<_>>();
    if let Some(response) = record_canonical_request_built(
        state,
        request_id,
        CanonicalRequestBuildInput {
            method,
            path,
            query: &query_without_signature,
            headers: &header_refs,
            signed_headers: authorization.signed_headers(),
            payload_hash: authorization.payload_hash(),
        },
        AuthErrorContext {
            resource: &resource,
            is_head,
        },
    ) {
        return Some(response);
    }

    let request = SigV4QueryVerificationRequest {
        method,
        path,
        query: &query_refs,
        headers: &header_refs,
    };
    sigv4_verification_response(
        state,
        request_id,
        verify_query_signature(&authorization, LOCAL_SECRET_ACCESS_KEY, request),
        AuthErrorContext {
            resource: &resource,
            is_head,
        },
    )
}

fn presigned_expiration_rejection_response(
    state: &ServerState,
    authorization: &SigV4QueryAuthorization,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Option<Response<Body>> {
    let expires_seconds = authorization.expires_seconds();
    if !(1..=MAX_PRESIGNED_EXPIRES_SECONDS).contains(&expires_seconds) {
        state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
            request_id.as_str(),
            AuthDecision::Rejected(AuthRejectionReason::InvalidAuthorization),
        )));
        return Some(auth_error_response(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "Use an X-Amz-Expires value from 1 through 604800 seconds.",
            resource,
            is_head,
            request_id,
        ));
    }

    let request_time = match parse_presigned_request_datetime(authorization.request_datetime()) {
        Ok(request_time) => request_time,
        Err(()) => {
            state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
                request_id.as_str(),
                AuthDecision::Rejected(AuthRejectionReason::InvalidAuthorization),
            )));
            return Some(auth_error_response(
                S3ErrorCode::AuthorizationHeaderMalformed,
                "Use a real UTC X-Amz-Date value in YYYYMMDDTHHMMSSZ form.",
                resource,
                is_head,
                request_id,
            ));
        }
    };
    let Some(expires_at) = request_time.checked_add(Duration::seconds(expires_seconds.into()))
    else {
        state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
            request_id.as_str(),
            AuthDecision::Rejected(AuthRejectionReason::InvalidAuthorization),
        )));
        return Some(auth_error_response(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "Use an X-Amz-Date and X-Amz-Expires combination that stays within supported UTC time.",
            resource,
            is_head,
            request_id,
        ));
    };

    if state.auth_now_utc() > expires_at {
        state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
            request_id.as_str(),
            AuthDecision::Rejected(AuthRejectionReason::InvalidAuthorization),
        )));
        return Some(auth_error_response(
            S3ErrorCode::AccessDenied,
            "The presigned URL has expired; generate a new URL with a later X-Amz-Date or X-Amz-Expires value.",
            resource,
            is_head,
            request_id,
        ));
    }

    None
}

fn invalid_authorization_response(
    state: &ServerState,
    request_id: &S3RequestId,
    response: Response<Body>,
) -> Option<Response<Body>> {
    state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
        request_id.as_str(),
        AuthDecision::Rejected(AuthRejectionReason::InvalidAuthorization),
    )));
    Some(response)
}

fn invalid_access_key_response(
    state: &ServerState,
    access_key_id: &str,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Option<Response<Body>> {
    if access_key_id == LOCAL_ACCESS_KEY_ID {
        return None;
    }

    state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
        request_id.as_str(),
        AuthDecision::Rejected(AuthRejectionReason::InvalidAccessKey),
    )));
    Some(auth_error_response(
        S3ErrorCode::InvalidAccessKeyId,
        "The access key id is not configured for this local S3Lab server.",
        resource,
        is_head,
        request_id,
    ))
}

fn parse_presigned_request_datetime(value: &str) -> Result<OffsetDateTime, ()> {
    if value.len() != 16
        || value.as_bytes()[8] != b'T'
        || value.as_bytes()[15] != b'Z'
        || !value[..8].bytes().all(|byte| byte.is_ascii_digit())
        || !value[9..15].bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(());
    }

    let year = parse_decimal_i32(&value[0..4])?;
    let month = Month::try_from(parse_decimal_u8(&value[4..6])?).map_err(|_| ())?;
    let day = parse_decimal_u8(&value[6..8])?;
    let hour = parse_decimal_u8(&value[9..11])?;
    let minute = parse_decimal_u8(&value[11..13])?;
    let second = parse_decimal_u8(&value[13..15])?;
    let date = Date::from_calendar_date(year, month, day).map_err(|_| ())?;
    let time = Time::from_hms(hour, minute, second).map_err(|_| ())?;

    Ok(PrimitiveDateTime::new(date, time).assume_utc())
}

fn parse_decimal_i32(value: &str) -> Result<i32, ()> {
    value.parse::<i32>().map_err(|_| ())
}

fn parse_decimal_u8(value: &str) -> Result<u8, ()> {
    value.parse::<u8>().map_err(|_| ())
}

fn authorization_header<'a>(
    headers: &'a HeaderMap,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<Option<&'a str>, Box<Response<Body>>> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(Box::new(auth_error_response(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "Include exactly one Authorization header.",
            resource,
            is_head,
            request_id,
        )));
    }

    value.to_str().map(Some).map_err(|_| {
        Box::new(auth_error_response(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "Use a UTF-8 Authorization header value.",
            resource,
            is_head,
            request_id,
        ))
    })
}

fn owned_sigv4_verification_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<OwnedSigV4VerificationRequest, Box<Response<Body>>> {
    let request_datetime = single_utf8_header(
        headers,
        X_AMZ_DATE,
        "Include exactly one x-amz-date header.",
        resource,
        is_head,
        request_id,
    )?;
    let payload_hash = optional_single_utf8_header(
        headers,
        X_AMZ_CONTENT_SHA256,
        "Include at most one x-amz-content-sha256 header.",
        resource,
        is_head,
        request_id,
    )?
    .unwrap_or_else(|| EMPTY_PAYLOAD_SHA256.to_owned());

    Ok(OwnedSigV4VerificationRequest {
        request_datetime,
        method: method.as_str().to_owned(),
        path: uri.path().to_owned(),
        query: sigv4_query_pairs(uri.query(), resource, is_head, request_id)?,
        headers: sigv4_header_pairs(headers, resource, is_head, request_id)?,
        payload_hash,
    })
}

fn single_utf8_header(
    headers: &HeaderMap,
    name: &'static str,
    message: &'static str,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<String, Box<Response<Body>>> {
    optional_single_utf8_header(headers, name, message, resource, is_head, request_id)?.ok_or_else(
        || {
            Box::new(auth_error_response(
                S3ErrorCode::AuthorizationHeaderMalformed,
                message,
                resource,
                is_head,
                request_id,
            ))
        },
    )
}

fn optional_single_utf8_header(
    headers: &HeaderMap,
    name: &'static str,
    message: &'static str,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<Option<String>, Box<Response<Body>>> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(Box::new(auth_error_response(
            S3ErrorCode::AuthorizationHeaderMalformed,
            message,
            resource,
            is_head,
            request_id,
        )));
    }

    value
        .to_str()
        .map(|value| Some(value.to_owned()))
        .map_err(|_| {
            Box::new(auth_error_response(
                S3ErrorCode::AuthorizationHeaderMalformed,
                "Signed request header values must be UTF-8.",
                resource,
                is_head,
                request_id,
            ))
        })
}

fn sigv4_query_pairs(
    raw_query: Option<&str>,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<Vec<(String, String)>, Box<Response<Body>>> {
    let mut query = Vec::new();
    for raw_pair in raw_query.into_iter().flat_map(|query| query.split('&')) {
        if raw_pair.is_empty() {
            continue;
        }
        let (raw_name, raw_value) = raw_pair.split_once('=').unwrap_or((raw_pair, ""));
        query.push((
            decode_sigv4_query_component(raw_name, resource, is_head, request_id)?,
            decode_sigv4_query_component(raw_value, resource, is_head, request_id)?,
        ));
    }
    query.sort();
    Ok(query)
}

fn has_presigned_query_authorization(query: &[(String, String)]) -> bool {
    query
        .iter()
        .any(|(name, _)| PRESIGNED_QUERY_AUTH_PARAMS.contains(&name.as_str()))
}

fn decode_sigv4_query_component(
    raw: &str,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<String, Box<Response<Body>>> {
    if !has_valid_percent_encoding(raw) {
        return Err(Box::new(auth_error_response(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "Use valid percent-encoding in the signed request query.",
            resource,
            is_head,
            request_id,
        )));
    }

    percent_decode_str(raw)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| {
            Box::new(auth_error_response(
                S3ErrorCode::AuthorizationHeaderMalformed,
                "Use UTF-8 query values in signed requests.",
                resource,
                is_head,
                request_id,
            ))
        })
}

fn sigv4_header_pairs(
    headers: &HeaderMap,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<Vec<(String, String)>, Box<Response<Body>>> {
    headers
        .iter()
        .map(|(name, value)| {
            value
                .to_str()
                .map(|value| (name.as_str().to_owned(), value.to_owned()))
                .map_err(|_| {
                    Box::new(auth_error_response(
                        S3ErrorCode::AuthorizationHeaderMalformed,
                        "Signed request header values must be UTF-8.",
                        resource,
                        is_head,
                        request_id,
                    ))
                })
        })
        .collect()
}

struct CanonicalRequestBuildInput<'a> {
    method: &'a str,
    path: &'a str,
    query: &'a [(&'a str, &'a str)],
    headers: &'a [(&'a str, &'a str)],
    signed_headers: &'a [SignedHeaderName],
    payload_hash: &'a str,
}

struct AuthErrorContext<'a> {
    resource: &'a str,
    is_head: bool,
}

fn record_canonical_request_built(
    state: &ServerState,
    request_id: &S3RequestId,
    input: CanonicalRequestBuildInput<'_>,
    error_context: AuthErrorContext<'_>,
) -> Option<Response<Body>> {
    match build_canonical_request(
        input.method,
        input.path,
        input.query,
        input.headers,
        input.signed_headers,
        input.payload_hash,
    ) {
        Ok(canonical_request) => {
            state.record_trace(TraceEvent::CanonicalRequestBuilt(
                CanonicalRequestBuiltTrace::from_canonical_request(
                    request_id.as_str(),
                    input.signed_headers.iter().map(|header| header.as_str()),
                    &canonical_request,
                ),
            ));
            None
        }
        Err(diagnostic) => {
            state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
                request_id.as_str(),
                AuthDecision::Rejected(auth_rejection_reason(&diagnostic)),
            )));
            Some(sigv4_verification_error_response(
                diagnostic,
                error_context.resource,
                error_context.is_head,
                request_id,
            ))
        }
    }
}

fn sigv4_verification_response<T>(
    state: &ServerState,
    request_id: &S3RequestId,
    result: Result<T, SigV4VerificationDiagnostic>,
    error_context: AuthErrorContext<'_>,
) -> Option<Response<Body>> {
    match result {
        Ok(_) => {
            state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
                request_id.as_str(),
                AuthDecision::Accepted,
            )));
            None
        }
        Err(diagnostic) => {
            state.record_trace(TraceEvent::AuthDecision(AuthDecisionTrace::new(
                request_id.as_str(),
                AuthDecision::Rejected(auth_rejection_reason(&diagnostic)),
            )));
            Some(sigv4_verification_error_response(
                diagnostic,
                error_context.resource,
                error_context.is_head,
                request_id,
            ))
        }
    }
}

fn sigv4_verification_error_response(
    diagnostic: SigV4VerificationDiagnostic,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Response<Body> {
    match diagnostic {
        SigV4VerificationDiagnostic::MissingSignedHeader { .. } => auth_error_response(
            S3ErrorCode::AccessDenied,
            diagnostic.message(),
            resource,
            is_head,
            request_id,
        ),
        SigV4VerificationDiagnostic::SignatureMismatch => auth_error_response(
            S3ErrorCode::SignatureDoesNotMatch,
            diagnostic.message(),
            resource,
            is_head,
            request_id,
        ),
    }
}

fn auth_error_response(
    code: S3ErrorCode,
    message: impl Into<String>,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Response<Body> {
    s3_error_response(
        S3Error::with_message(code, message, resource, request_id.clone()),
        is_head,
    )
}

fn record_sigv4_parsed(
    state: &ServerState,
    request_id: &S3RequestId,
    authorization: &SigV4Authorization,
) {
    let scope = authorization.credential().scope();
    record_accepted_sigv4_parsed(
        state,
        request_id,
        trace_credential_scope(scope),
        authorization
            .signed_headers()
            .iter()
            .map(|header| header.as_str()),
    );
}

fn record_sigv4_query_parsed(
    state: &ServerState,
    request_id: &S3RequestId,
    authorization: &SigV4QueryAuthorization,
) {
    let scope = authorization.credential().scope();
    record_accepted_sigv4_parsed(
        state,
        request_id,
        trace_credential_scope(scope),
        authorization
            .signed_headers()
            .iter()
            .map(|header| header.as_str()),
    );
}

fn record_accepted_sigv4_parsed<'a>(
    state: &ServerState,
    request_id: &S3RequestId,
    scope: TraceCredentialScope,
    signed_headers: impl Iterator<Item = &'a str>,
) {
    state.record_trace(TraceEvent::SigV4Parsed(SigV4ParsedTrace::accepted(
        request_id.as_str(),
        scope,
        signed_headers,
    )));
}

fn trace_credential_scope(scope: &SigV4CredentialScope) -> TraceCredentialScope {
    TraceCredentialScope::new(scope.date(), scope.region(), scope.service())
}

fn sigv4_parse_rejection(diagnostic: &SigV4ParseDiagnostic) -> SigV4ParseRejection {
    match diagnostic {
        SigV4ParseDiagnostic::UnsupportedAlgorithm => SigV4ParseRejection::UnsupportedAlgorithm,
        SigV4ParseDiagnostic::InvalidCredentialScope
        | SigV4ParseDiagnostic::EmptyAccessKey
        | SigV4ParseDiagnostic::InvalidCredentialDate
        | SigV4ParseDiagnostic::EmptyRegion
        | SigV4ParseDiagnostic::EmptyService
        | SigV4ParseDiagnostic::InvalidCredentialScopeTerminator => {
            SigV4ParseRejection::InvalidCredentialScope
        }
        SigV4ParseDiagnostic::EmptySignedHeaders
        | SigV4ParseDiagnostic::InvalidSignedHeaderName
        | SigV4ParseDiagnostic::DuplicateSignedHeader
        | SigV4ParseDiagnostic::UnsortedSignedHeaders => SigV4ParseRejection::InvalidSignedHeaders,
        SigV4ParseDiagnostic::InvalidSignatureLength
        | SigV4ParseDiagnostic::InvalidSignatureHex => SigV4ParseRejection::InvalidSignature,
        SigV4ParseDiagnostic::MalformedAuthorizationHeader
        | SigV4ParseDiagnostic::MalformedParameter
        | SigV4ParseDiagnostic::UnknownParameter
        | SigV4ParseDiagnostic::MissingParameter { .. }
        | SigV4ParseDiagnostic::DuplicateParameter { .. } => {
            SigV4ParseRejection::MalformedAuthorization
        }
    }
}

fn sigv4_query_parse_rejection(diagnostic: &SigV4QueryParseDiagnostic) -> SigV4ParseRejection {
    match diagnostic {
        SigV4QueryParseDiagnostic::UnsupportedAlgorithm => {
            SigV4ParseRejection::UnsupportedAlgorithm
        }
        SigV4QueryParseDiagnostic::InvalidCredentialScope
        | SigV4QueryParseDiagnostic::EmptyAccessKey
        | SigV4QueryParseDiagnostic::InvalidCredentialDate
        | SigV4QueryParseDiagnostic::EmptyRegion
        | SigV4QueryParseDiagnostic::EmptyService
        | SigV4QueryParseDiagnostic::InvalidCredentialScopeTerminator => {
            SigV4ParseRejection::InvalidCredentialScope
        }
        SigV4QueryParseDiagnostic::EmptySignedHeaders
        | SigV4QueryParseDiagnostic::InvalidSignedHeaderName
        | SigV4QueryParseDiagnostic::DuplicateSignedHeader
        | SigV4QueryParseDiagnostic::UnsortedSignedHeaders => {
            SigV4ParseRejection::InvalidSignedHeaders
        }
        SigV4QueryParseDiagnostic::InvalidSignatureLength
        | SigV4QueryParseDiagnostic::InvalidSignatureHex => SigV4ParseRejection::InvalidSignature,
        SigV4QueryParseDiagnostic::MissingParameter { .. }
        | SigV4QueryParseDiagnostic::DuplicateParameter { .. }
        | SigV4QueryParseDiagnostic::UnsupportedSessionToken
        | SigV4QueryParseDiagnostic::InvalidRequestDate
        | SigV4QueryParseDiagnostic::InvalidExpires
        | SigV4QueryParseDiagnostic::InvalidContentSha256 => {
            SigV4ParseRejection::MalformedAuthorization
        }
    }
}

fn auth_rejection_reason(diagnostic: &SigV4VerificationDiagnostic) -> AuthRejectionReason {
    match diagnostic {
        SigV4VerificationDiagnostic::MissingSignedHeader { .. } => {
            AuthRejectionReason::MissingSignedHeader
        }
        SigV4VerificationDiagnostic::SignatureMismatch => AuthRejectionReason::SignatureMismatch,
    }
}

fn record_request_received(
    state: &ServerState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    request_id: &S3RequestId,
) {
    state.record_trace(TraceEvent::RequestReceived(RequestReceivedTrace::new(
        request_id.as_str(),
        method.as_str(),
        uri.path(),
        headers.keys().map(HeaderName::as_str),
    )));
}

fn record_route_resolved(
    state: &ServerState,
    method: &Method,
    uri: &Uri,
    operation: &S3Operation,
    request_id: &S3RequestId,
) {
    state.record_trace(TraceEvent::RouteResolved(RouteResolvedTrace::new(
        request_id.as_str(),
        method.as_str(),
        uri.path(),
        trace_operation(operation),
    )));
}

fn record_route_rejected(
    state: &ServerState,
    method: &Method,
    uri: &Uri,
    rejection: &RouteRejection,
    request_id: &S3RequestId,
) {
    state.record_trace(TraceEvent::RouteRejected(RouteRejectedTrace::new(
        request_id.as_str(),
        method.as_str(),
        uri.path(),
        route_rejection_reason(rejection),
    )));
}

fn record_response_sent(state: &ServerState, request_id: &S3RequestId, response: &Response<Body>) {
    state.record_trace(TraceEvent::ResponseSent(ResponseSentTrace::new(
        request_id.as_str(),
        response.status().as_u16(),
        None::<String>,
    )));
}

fn trace_operation(operation: &S3Operation) -> TraceS3Operation {
    match operation {
        S3Operation::ListBuckets => TraceS3Operation::ListBuckets,
        S3Operation::CreateBucket { .. } => TraceS3Operation::CreateBucket,
        S3Operation::HeadBucket { .. } => TraceS3Operation::HeadBucket,
        S3Operation::DeleteBucket { .. } => TraceS3Operation::DeleteBucket,
        S3Operation::PutObject { .. } => TraceS3Operation::PutObject,
        S3Operation::GetObject { .. } => TraceS3Operation::GetObject,
        S3Operation::HeadObject { .. } => TraceS3Operation::HeadObject,
        S3Operation::DeleteObject { .. } => TraceS3Operation::DeleteObject,
        S3Operation::ListObjectsV2 { .. } => TraceS3Operation::ListObjectsV2,
    }
}

fn route_rejection_reason(rejection: &RouteRejection) -> RouteRejectionReason {
    match rejection.code {
        S3ErrorCode::InvalidBucketName => RouteRejectionReason::InvalidPath,
        S3ErrorCode::InvalidArgument => RouteRejectionReason::InvalidQuery,
        S3ErrorCode::MethodNotAllowed => RouteRejectionReason::MethodNotAllowed,
        S3ErrorCode::NotImplemented => RouteRejectionReason::UnsupportedOperation,
        _ => RouteRejectionReason::UnsupportedOperation,
    }
}

fn record_storage_mutation_result<T>(
    state: &ServerState,
    request_id: &S3RequestId,
    mutation: StorageMutation,
    bucket: Option<&str>,
    key: Option<&str>,
    result: Result<T, &StorageError>,
) {
    let outcome = match result {
        Ok(_) | Err(StorageError::NoSuchKey { .. }) => StorageMutationOutcome::Applied,
        Err(error) => StorageMutationOutcome::Rejected {
            error_code: s3_error_code_from_storage_error(error).as_str().to_owned(),
        },
    };

    state.record_trace(TraceEvent::StorageMutation(StorageMutationTrace::new(
        request_id.as_str(),
        mutation,
        bucket.map(str::to_owned),
        key.map(str::to_owned),
        outcome,
    )));
}

fn resolve_root_operation(
    method: &Method,
    query: &QueryParams,
    resource: &str,
) -> Result<S3Operation, RouteRejection> {
    reject_query_params(query, resource)?;

    if method == Method::GET {
        return Ok(S3Operation::ListBuckets);
    }

    Err(RouteRejection::new(
        S3ErrorCode::MethodNotAllowed,
        resource.to_owned(),
    ))
}

fn resolve_bucket_operation(
    method: &Method,
    bucket: BucketName,
    query: &QueryParams,
    resource: &str,
) -> Result<S3Operation, RouteRejection> {
    match *method {
        Method::PUT => {
            reject_query_params(query, resource)?;
            Ok(S3Operation::CreateBucket { bucket })
        }
        Method::HEAD => {
            reject_query_params(query, resource)?;
            Ok(S3Operation::HeadBucket { bucket })
        }
        Method::DELETE => {
            reject_query_params(query, resource)?;
            Ok(S3Operation::DeleteBucket { bucket })
        }
        Method::GET => resolve_bucket_get_operation(bucket, query, resource),
        _ => Err(RouteRejection::new(
            S3ErrorCode::MethodNotAllowed,
            resource.to_owned(),
        )),
    }
}

fn resolve_bucket_get_operation(
    bucket: BucketName,
    query: &QueryParams,
    resource: &str,
) -> Result<S3Operation, RouteRejection> {
    reject_query_params_except(
        query,
        [
            LIST_TYPE,
            PREFIX,
            CONTINUATION_TOKEN,
            MAX_KEYS,
            DELIMITER,
            ENCODING_TYPE,
            START_AFTER,
            FETCH_OWNER,
        ],
        resource,
    )?;

    let Some(list_type) = query.get(LIST_TYPE) else {
        return Err(RouteRejection::new(
            S3ErrorCode::NotImplemented,
            resource.to_owned(),
        ));
    };

    if list_type != "2" {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        ));
    }

    let max_keys = parse_max_keys(query.get(MAX_KEYS), resource)?;
    if query
        .get(PREFIX)
        .is_some_and(|prefix| !is_valid_s3_object_key_prefix(prefix))
    {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        ));
    }
    if query
        .get(DELIMITER)
        .is_some_and(|delimiter| delimiter != "/")
    {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        ));
    }

    let encoding = parse_list_objects_encoding(query.get(ENCODING_TYPE), resource)?;

    if query.has_unsupported_list_objects_v2_param() {
        return Err(RouteRejection::new(
            S3ErrorCode::NotImplemented,
            resource.to_owned(),
        ));
    }

    Ok(S3Operation::ListObjectsV2 {
        bucket,
        prefix: query.get(PREFIX).cloned().map(ObjectKey::new),
        delimiter: query.get(DELIMITER).cloned(),
        continuation_token: query.get(CONTINUATION_TOKEN).cloned(),
        max_keys,
        encoding,
    })
}

fn resolve_object_operation(
    method: &Method,
    bucket: BucketName,
    key: ObjectKey,
    query: &QueryParams,
    resource: &str,
) -> Result<S3Operation, RouteRejection> {
    match *method {
        Method::PUT => {
            reject_object_query_params(query, resource)?;
            Ok(S3Operation::PutObject { bucket, key })
        }
        Method::GET => {
            reject_object_query_params(query, resource)?;
            Ok(S3Operation::GetObject { bucket, key })
        }
        Method::HEAD => {
            reject_query_params(query, resource)?;
            Ok(S3Operation::HeadObject { bucket, key })
        }
        Method::DELETE => {
            reject_query_params(query, resource)?;
            Ok(S3Operation::DeleteObject { bucket, key })
        }
        _ => Err(RouteRejection::new(
            S3ErrorCode::MethodNotAllowed,
            resource.to_owned(),
        )),
    }
}

async fn execute_operation(
    state: ServerState,
    headers: HeaderMap,
    body: Body,
    operation: S3Operation,
    uri: Uri,
    is_head: bool,
    request_id: S3RequestId,
) -> Response<Body> {
    let presigned_payload_hash = presigned_payload_hash(&uri, is_head, &request_id);
    let result = execute_storage_operation(
        state,
        headers,
        body,
        operation,
        presigned_payload_hash,
        is_head,
        &request_id,
    )
    .await;

    match result {
        Ok(response) => {
            if is_head {
                head_from_response(response)
            } else {
                response
            }
        }
        Err((error, resource)) => storage_error_response(error, resource, is_head, &request_id),
    }
}

async fn execute_storage_operation(
    state: ServerState,
    headers: HeaderMap,
    body: Body,
    operation: S3Operation,
    presigned_payload_hash: RouteResponseResult<Option<String>>,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    match operation {
        S3Operation::ListBuckets => list_buckets_response(&state, request_id),
        S3Operation::CreateBucket { bucket } => create_bucket_response(&state, bucket, request_id),
        S3Operation::HeadBucket { bucket } => head_bucket_response(&state, bucket, request_id),
        S3Operation::DeleteBucket { bucket } => delete_bucket_response(&state, bucket, request_id),
        S3Operation::PutObject { bucket, key } => {
            put_object_route_response(
                state,
                headers,
                body,
                PutObjectRouteRequest {
                    bucket,
                    key,
                    presigned_payload_hash,
                },
                is_head,
                request_id,
            )
            .await
        }
        S3Operation::GetObject { bucket, key } => {
            get_object_route_response(&state, &headers, bucket, key, is_head, request_id)
        }
        S3Operation::HeadObject { bucket, key } => {
            head_object_route_response(&state, &headers, bucket, key, is_head, request_id)
        }
        S3Operation::DeleteObject { bucket, key } => {
            delete_object_response(&state, bucket, key, request_id)
        }
        S3Operation::ListObjectsV2 {
            bucket,
            prefix,
            delimiter,
            continuation_token,
            max_keys,
            encoding,
        } => list_objects_v2_route_response(
            &state,
            ListObjectsV2RouteRequest {
                bucket,
                prefix,
                delimiter,
                continuation_token,
                max_keys,
                encoding,
            },
            request_id,
        ),
    }
}

fn list_buckets_response(
    state: &ServerState,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    state
        .storage()
        .list_buckets()
        .map(|buckets| {
            xml_response(
                list_buckets_response_xml(&list_buckets_xml(buckets)),
                request_id,
            )
        })
        .map_err(|error| (error, "/".to_owned()))
}

fn create_bucket_response(
    state: &ServerState,
    bucket: BucketName,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    let result = state.storage().create_bucket(&bucket);
    record_storage_mutation_result(
        state,
        request_id,
        StorageMutation::CreateBucket,
        Some(bucket.as_str()),
        None,
        result.as_ref().map(|_| ()),
    );

    result
        .map(|()| empty_response(request_id))
        .map_err(|error| (error, bucket_resource(&bucket)))
}

fn head_bucket_response(
    state: &ServerState,
    bucket: BucketName,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    match state.storage().bucket_exists(&bucket) {
        Ok(true) => Ok(empty_response(request_id)),
        Ok(false) => Err((
            StorageError::NoSuchBucket {
                bucket: bucket.clone(),
            },
            bucket_resource(&bucket),
        )),
        Err(error) => Err((error, bucket_resource(&bucket))),
    }
}

fn delete_bucket_response(
    state: &ServerState,
    bucket: BucketName,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    let result = state.storage().delete_bucket(&bucket);
    record_storage_mutation_result(
        state,
        request_id,
        StorageMutation::DeleteBucket,
        Some(bucket.as_str()),
        None,
        result.as_ref().map(|_| ()),
    );

    result
        .map(|()| no_content_response(request_id))
        .map_err(|error| (error, bucket_resource(&bucket)))
}

async fn put_object_route_response(
    state: ServerState,
    headers: HeaderMap,
    body: Body,
    route_request: PutObjectRouteRequest,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    let resource = object_resource(&route_request.bucket, &route_request.key);
    if let Some(header_name) = unsupported_put_object_header(&headers) {
        log_put_object_rejection(
            &headers,
            &resource,
            request_id,
            "unsupported header",
            &header_name,
        );
        return Ok(route_error_response(
            RouteRejection::new(S3ErrorCode::NotImplemented, resource),
            is_head,
            request_id,
        ));
    }
    let checksum = match extract_put_object_checksum(&headers, &resource) {
        Ok(checksum) => checksum,
        Err(rejection) => return Ok(route_error_response(rejection, is_head, request_id)),
    };
    let body_encoding = match extract_put_object_body_encoding(&headers, &resource) {
        Ok(body_encoding) => body_encoding,
        Err(rejection) => return Ok(route_error_response(rejection, is_head, request_id)),
    };

    let content_type = match extract_content_type(&headers, &resource) {
        Ok(content_type) => content_type,
        Err(rejection) => return Ok(route_error_response(rejection, is_head, request_id)),
    };
    let user_metadata = match extract_user_metadata(&headers, &resource) {
        Ok(user_metadata) => user_metadata,
        Err(rejection) => return Ok(route_error_response(rejection, is_head, request_id)),
    };
    let bytes = match read_put_object_body(body, &resource, is_head, request_id).await {
        Ok(bytes) => bytes,
        Err(response) => return Ok(response),
    };
    let body = match decode_put_object_body(
        body_encoding,
        bytes.as_ref(),
        checksum,
        &headers,
        &resource,
        request_id,
    ) {
        Ok(body) => body,
        Err(response) => return Ok(*response),
    };
    for checksum in &body.checksums {
        if let Err(response) =
            validate_put_object_checksum(checksum, &body.bytes, &resource, request_id)
        {
            return Ok(*response);
        }
    }
    match validate_signed_put_object_payload_hash(
        &headers,
        route_request.presigned_payload_hash,
        &body.bytes,
        &resource,
        request_id,
    ) {
        Ok(Some(outcome)) => {
            state.record_trace(TraceEvent::PayloadVerification(
                PayloadVerificationTrace::new(request_id.as_str(), outcome),
            ));
        }
        Ok(None) => {}
        Err(response) => return Ok(*response),
    }

    let result = state.storage().put_object(PutObjectRequest {
        bucket: route_request.bucket.clone(),
        key: route_request.key.clone(),
        bytes: body.bytes,
        content_type,
        user_metadata,
    });
    record_storage_mutation_result(
        &state,
        request_id,
        StorageMutation::PutObject,
        Some(route_request.bucket.as_str()),
        Some(route_request.key.as_str()),
        result.as_ref().map(|_| ()),
    );

    result
        .map(|metadata| put_object_response(metadata, request_id))
        .map_err(|error| (error, resource))
}

fn get_object_route_response(
    state: &ServerState,
    headers: &HeaderMap,
    bucket: BucketName,
    key: ObjectKey,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    if let Some(response) = unsupported_range_response(headers, &bucket, &key, is_head, request_id)
    {
        return Ok(response);
    }

    state
        .storage()
        .get_object(&bucket, &key)
        .and_then(|object| object_response(object, request_id))
        .map_err(|error| (error, object_resource(&bucket, &key)))
}

fn head_object_route_response(
    state: &ServerState,
    headers: &HeaderMap,
    bucket: BucketName,
    key: ObjectKey,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    if let Some(response) = unsupported_range_response(headers, &bucket, &key, is_head, request_id)
    {
        return Ok(response);
    }

    state
        .storage()
        .get_object_metadata(&bucket, &key)
        .and_then(|metadata| object_metadata_response(metadata, Body::empty(), request_id))
        .map_err(|error| (error, object_resource(&bucket, &key)))
}

fn delete_object_response(
    state: &ServerState,
    bucket: BucketName,
    key: ObjectKey,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    let result = state.storage().delete_object(&bucket, &key);
    record_storage_mutation_result(
        state,
        request_id,
        StorageMutation::DeleteObject,
        Some(bucket.as_str()),
        Some(key.as_str()),
        result.as_ref().map(|_| ()),
    );

    match result {
        Ok(()) | Err(StorageError::NoSuchKey { .. }) => Ok(no_content_response(request_id)),
        Err(error) => Err((error, object_resource(&bucket, &key))),
    }
}

fn list_objects_v2_route_response(
    state: &ServerState,
    request: ListObjectsV2RouteRequest,
    request_id: &S3RequestId,
) -> Result<Response<Body>, (StorageError, String)> {
    let ListObjectsV2RouteRequest {
        bucket,
        prefix,
        delimiter,
        continuation_token,
        max_keys,
        encoding,
    } = request;
    let request_continuation_token = continuation_token.clone();
    let options = ListObjectsOptions {
        prefix: prefix.clone(),
        delimiter: delimiter.clone(),
        continuation_token,
        max_keys,
    };

    state
        .storage()
        .list_objects(&bucket, options)
        .map(|listing| {
            let listing = list_objects_v2_xml(listing, encoding);
            let response_prefix = prefix
                .as_ref()
                .map(ObjectKey::as_str)
                .map(|value| list_objects_xml_text(value, encoding));
            let response_delimiter = delimiter
                .as_deref()
                .map(|value| list_objects_xml_text(value, encoding));
            xml_response(
                list_objects_v2_response_xml(
                    &listing,
                    response_prefix.as_deref(),
                    response_delimiter.as_deref(),
                    request_continuation_token.as_deref(),
                    encoding.map(list_objects_encoding_value),
                ),
                request_id,
            )
        })
        .map_err(|error| (error, bucket_resource(&bucket)))
}

fn unsupported_range_response(
    headers: &HeaderMap,
    bucket: &BucketName,
    key: &ObjectKey,
    is_head: bool,
    request_id: &S3RequestId,
) -> Option<Response<Body>> {
    headers.contains_key(RANGE).then(|| {
        route_error_response(
            RouteRejection::new(S3ErrorCode::NotImplemented, object_resource(bucket, key)),
            is_head,
            request_id,
        )
    })
}

async fn read_put_object_body(
    body: Body,
    resource: &str,
    is_head: bool,
    request_id: &S3RequestId,
) -> Result<Bytes, Response<Body>> {
    match to_bytes(body, PHASE1_MAX_PUT_OBJECT_BODY_BYTES).await {
        Ok(bytes) => Ok(bytes),
        Err(error) if body_limit_exceeded(&error) => Err(s3_error_response(
            S3Error::with_message(
                S3ErrorCode::EntityTooLarge,
                format!(
                    "Object body exceeds S3Lab Phase 1 PUT object limit of {PHASE1_MAX_PUT_OBJECT_BODY_BYTES} bytes."
                ),
                resource,
                request_id.clone(),
            ),
            is_head,
        )),
        Err(_) => Err(s3_error_response(
            S3Error::new(
                S3ErrorCode::InternalError,
                resource,
                request_id.clone(),
            ),
            is_head,
        )),
    }
}

fn body_limit_exceeded(error: &axum::Error) -> bool {
    error
        .source()
        .is_some_and(|source| source.is::<LengthLimitError>())
}

fn parse_path(path: &str, resource: &str) -> Result<RouteResource, RouteRejection> {
    if path == "/" || path.is_empty() {
        return Ok(RouteResource::Root);
    }

    let path_without_root = path.strip_prefix('/').unwrap_or(path);
    let (raw_bucket, raw_key) = path_without_root
        .split_once('/')
        .map_or((path_without_root, None), |(bucket, key)| {
            (bucket, Some(key))
        });

    let bucket = decode_route_component(raw_bucket, resource)?;
    if !is_valid_s3_bucket_name(&bucket) {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidBucketName,
            resource.to_owned(),
        ));
    }

    let Some(raw_key) = raw_key else {
        return Ok(RouteResource::Bucket {
            bucket: BucketName::new(bucket),
        });
    };

    let key = decode_route_component(raw_key, resource)?;
    if !is_valid_s3_object_key(&key) {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        ));
    }

    Ok(RouteResource::Object {
        bucket: BucketName::new(bucket),
        key: ObjectKey::new(key),
    })
}

fn parse_query(raw_query: Option<&str>, resource: &str) -> Result<QueryParams, RouteRejection> {
    let mut values = BTreeMap::new();

    for raw_pair in raw_query.into_iter().flat_map(|query| query.split('&')) {
        if raw_pair.is_empty() {
            continue;
        }

        let (raw_name, raw_value) = raw_pair.split_once('=').unwrap_or((raw_pair, ""));
        let name = decode_route_component(raw_name, resource)?;
        if name.is_empty() {
            return Err(RouteRejection::new(
                S3ErrorCode::InvalidArgument,
                resource.to_owned(),
            ));
        }
        if name == X_ID {
            continue;
        }

        if values.contains_key(&name) {
            return Err(RouteRejection::new(
                S3ErrorCode::InvalidArgument,
                resource.to_owned(),
            ));
        }

        values.insert(name, decode_route_component(raw_value, resource)?);
    }

    Ok(QueryParams { values })
}

fn reject_query_params(query: &QueryParams, resource: &str) -> Result<(), RouteRejection> {
    if query.values.is_empty() {
        return Ok(());
    }

    Err(RouteRejection::new(
        S3ErrorCode::InvalidArgument,
        resource.to_owned(),
    ))
}

fn reject_object_query_params(query: &QueryParams, resource: &str) -> Result<(), RouteRejection> {
    if query.values.is_empty() {
        return Ok(());
    }

    if query.has_presigned_query_authorization()
        && query.values.keys().all(|name| {
            PRESIGNED_QUERY_AUTH_PARAMS.contains(&name.as_str())
                || PRESIGNED_RESPONSE_OVERRIDE_PARAMS.contains(&name.as_str())
        })
    {
        return Ok(());
    }

    Err(RouteRejection::new(
        S3ErrorCode::InvalidArgument,
        resource.to_owned(),
    ))
}

fn reject_query_params_except<const N: usize>(
    query: &QueryParams,
    allowed: [&str; N],
    resource: &str,
) -> Result<(), RouteRejection> {
    let allowed = allowed.into_iter().collect::<BTreeSet<_>>();
    if query
        .values
        .keys()
        .all(|name| allowed.contains(name.as_str()))
    {
        return Ok(());
    }

    Err(RouteRejection::new(
        S3ErrorCode::InvalidArgument,
        resource.to_owned(),
    ))
}

fn decode_route_component(raw: &str, resource: &str) -> Result<String, RouteRejection> {
    if !has_valid_percent_encoding(raw) {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        ));
    }

    percent_decode_str(raw)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| RouteRejection::new(S3ErrorCode::InvalidArgument, resource.to_owned()))
}

fn parse_max_keys(value: Option<&String>, resource: &str) -> Result<usize, RouteRejection> {
    let Some(value) = value else {
        return Ok(1000);
    };

    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        ));
    }

    let max_keys = value
        .parse::<usize>()
        .map_err(|_| RouteRejection::new(S3ErrorCode::InvalidArgument, resource.to_owned()))?;
    if max_keys > 1000 {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        ));
    }

    Ok(max_keys)
}

fn parse_list_objects_encoding(
    value: Option<&String>,
    resource: &str,
) -> Result<Option<ListObjectsEncoding>, RouteRejection> {
    match value.map(String::as_str) {
        None => Ok(None),
        Some("url") => Ok(Some(ListObjectsEncoding::Url)),
        Some(_) => Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        )),
    }
}

fn has_valid_percent_encoding(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}

fn route_error_response(
    rejection: RouteRejection,
    is_head: bool,
    request_id: &S3RequestId,
) -> Response<Body> {
    s3_error_response(
        S3Error::new(rejection.code, rejection.resource, request_id.clone()),
        is_head,
    )
}

fn storage_error_response(
    error: StorageError,
    resource: impl Into<String>,
    is_head: bool,
    request_id: &S3RequestId,
) -> Response<Body> {
    s3_error_response(
        s3_error_from_storage_error(error, resource, request_id.clone()),
        is_head,
    )
}

fn s3_error_from_storage_error(
    error: StorageError,
    resource: impl Into<String>,
    request_id: S3RequestId,
) -> S3Error {
    let code = s3_error_code_from_storage_error(&error);
    let resource = resource.into();

    match code {
        S3ErrorCode::InternalError => S3Error::new(code, resource, request_id),
        S3ErrorCode::InvalidArgument => {
            S3Error::with_message(code, error.to_string(), resource, request_id)
        }
        _ => S3Error::new(code, resource, request_id),
    }
}

fn s3_error_code_from_storage_error(error: &StorageError) -> S3ErrorCode {
    match error {
        StorageError::BucketAlreadyExists { .. } => S3ErrorCode::BucketAlreadyOwnedByYou,
        StorageError::BucketNotEmpty { .. } => S3ErrorCode::BucketNotEmpty,
        StorageError::NoSuchBucket { .. } => S3ErrorCode::NoSuchBucket,
        StorageError::NoSuchKey { .. } => S3ErrorCode::NoSuchKey,
        StorageError::InvalidBucketName { .. } => S3ErrorCode::InvalidBucketName,
        StorageError::InvalidObjectKey { .. } | StorageError::InvalidArgument { .. } => {
            S3ErrorCode::InvalidArgument
        }
        StorageError::CorruptState { .. } | StorageError::Io { .. } => S3ErrorCode::InternalError,
    }
}

fn s3_error_response(error: S3Error, is_head: bool) -> Response<Body> {
    let status = status_code(error.code.http_status_code());
    if is_head {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = status;
        response.headers_mut().insert(
            REQUEST_ID_HEADER,
            HeaderValue::from_str(error.request_id.as_str()).expect("request id is valid"),
        );
        return response;
    }

    let mut response = Response::new(Body::from(error_response_xml(&error)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(XML_CONTENT_TYPE));
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(error.request_id.as_str()).expect("request id is valid"),
    );
    response
}

fn empty_response(request_id: &S3RequestId) -> Response<Body> {
    with_request_id(Response::new(Body::empty()), request_id)
}

fn no_content_response(request_id: &S3RequestId) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    with_request_id(response, request_id)
}

fn xml_response(xml: String, request_id: &S3RequestId) -> Response<Body> {
    let mut response = Response::new(Body::from(xml));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(XML_CONTENT_TYPE));
    with_request_id(response, request_id)
}

fn list_buckets_xml(buckets: Vec<BucketSummary>) -> ListBucketsXml {
    ListBucketsXml {
        buckets: buckets
            .into_iter()
            .map(|bucket| ListBucketXml {
                name: bucket.name.as_str().to_owned(),
            })
            .collect(),
    }
}

fn list_objects_v2_xml(
    listing: ObjectListing,
    encoding: Option<ListObjectsEncoding>,
) -> ListObjectsV2Xml {
    ListObjectsV2Xml {
        bucket: list_objects_xml_text(listing.bucket.as_str(), encoding),
        entries: listing
            .entries
            .into_iter()
            .map(|entry| match entry {
                ObjectListingEntry::Object(object) => {
                    ListObjectsV2XmlEntry::Object(ListObjectXml {
                        key: list_objects_xml_text(object.key.as_str(), encoding),
                        etag: object.etag,
                        content_length: object.content_length,
                        last_modified: object.last_modified,
                    })
                }
                ObjectListingEntry::CommonPrefix(prefix) => ListObjectsV2XmlEntry::CommonPrefix(
                    list_objects_xml_text(prefix.as_str(), encoding),
                ),
            })
            .collect(),
        max_keys: listing.max_keys,
        is_truncated: listing.is_truncated,
        next_continuation_token: listing.next_continuation_token,
    }
}

fn list_objects_xml_text(value: &str, encoding: Option<ListObjectsEncoding>) -> String {
    match encoding {
        Some(ListObjectsEncoding::Url) => s3_url_encode(value),
        None => value.to_owned(),
    }
}

fn list_objects_encoding_value(encoding: ListObjectsEncoding) -> &'static str {
    match encoding {
        ListObjectsEncoding::Url => "url",
    }
}

fn s3_url_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(char::from(*byte));
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn object_response(
    object: StoredObject,
    request_id: &S3RequestId,
) -> Result<Response<Body>, StorageError> {
    object_metadata_response(object.metadata, Body::from(object.bytes), request_id)
}

fn put_object_response(metadata: StoredObjectMetadata, request_id: &S3RequestId) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(&metadata.etag)
            .expect("storage-generated ETag must be a valid HTTP header value"),
    );
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(request_id.as_str()).expect("request id is valid"),
    );
    response
}

fn object_metadata_response(
    metadata: StoredObjectMetadata,
    body: Body,
    request_id: &S3RequestId,
) -> Result<Response<Body>, StorageError> {
    let mut response = Response::new(body);
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&metadata.content_length.to_string())
            .expect("content length is valid"),
    );
    if let Some(content_type) = metadata.content_type {
        response.headers_mut().insert(
            CONTENT_TYPE,
            metadata_header_value(&content_type, "Content-Type")?,
        );
    }
    response
        .headers_mut()
        .insert(ETAG, metadata_header_value(&metadata.etag, "ETag")?);
    response.headers_mut().insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&http_date(metadata.last_modified)).expect("HTTP date is valid"),
    );
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(request_id.as_str()).expect("request id is valid"),
    );
    for (key, value) in metadata.user_metadata {
        let header_name = format!("{USER_METADATA_HEADER_PREFIX}{key}");
        let header_name = HeaderName::from_bytes(header_name.as_bytes()).map_err(|error| {
            corrupt_response_metadata(format!(
                "stored user metadata key cannot be returned as an HTTP header: {error}"
            ))
        })?;
        response
            .headers_mut()
            .insert(header_name, metadata_header_value(&value, "user metadata")?);
    }
    Ok(response)
}

fn with_request_id(mut response: Response<Body>, request_id: &S3RequestId) -> Response<Body> {
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(request_id.as_str()).expect("request id is valid"),
    );
    response
}

fn metadata_header_value(value: &str, field: &str) -> Result<HeaderValue, StorageError> {
    HeaderValue::from_str(value).map_err(|error| {
        corrupt_response_metadata(format!(
            "stored {field} cannot be returned as an HTTP header: {error}"
        ))
    })
}

fn corrupt_response_metadata(message: impl Into<String>) -> StorageError {
    StorageError::CorruptState {
        path: PathBuf::from("object metadata"),
        message: message.into(),
    }
}

fn header_value_to_owned(value: &HeaderValue, resource: &str) -> Result<String, RouteRejection> {
    value
        .to_str()
        .map(str::to_owned)
        .map_err(|_| RouteRejection::new(S3ErrorCode::InvalidArgument, resource.to_owned()))
}

fn extract_user_metadata(
    headers: &HeaderMap,
    resource: &str,
) -> Result<BTreeMap<String, String>, RouteRejection> {
    let mut user_metadata = BTreeMap::new();
    for (name, value) in headers {
        let Some(suffix) = name.as_str().strip_prefix(USER_METADATA_HEADER_PREFIX) else {
            continue;
        };
        if suffix.is_empty() {
            return Err(invalid_user_metadata(resource));
        }

        let key = suffix.to_ascii_lowercase();
        let value = value
            .to_str()
            .map_err(|_| invalid_user_metadata(resource))?
            .to_owned();
        if user_metadata.insert(key, value).is_some() {
            return Err(invalid_user_metadata(resource));
        }
    }

    Ok(user_metadata)
}

fn extract_content_type(
    headers: &HeaderMap,
    resource: &str,
) -> Result<Option<String>, RouteRejection> {
    headers
        .get(CONTENT_TYPE)
        .map(|value| header_value_to_owned(value, resource))
        .transpose()
}

fn extract_put_object_checksum(
    headers: &HeaderMap,
    resource: &str,
) -> Result<PutObjectChecksum, RouteRejection> {
    reject_unsupported_checksum_algorithm(headers, X_AMZ_SDK_CHECKSUM_ALGORITHM, resource)?;
    reject_unsupported_checksum_algorithm(headers, X_AMZ_CHECKSUM_ALGORITHM, resource)?;
    reject_unsupported_checksum_trailer(headers, resource)?;

    let crc32 = extract_checksum_header(headers, X_AMZ_CHECKSUM_CRC32, resource)?;
    let crc64_nvme = extract_checksum_header(headers, X_AMZ_CHECKSUM_CRC64NVME, resource)?;

    match (crc32, crc64_nvme) {
        (Some(_), Some(_)) => Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        )),
        (Some(value), None) => Ok(PutObjectChecksum::Crc32(value)),
        (None, Some(value)) => Ok(PutObjectChecksum::Crc64Nvme(value)),
        (None, None) => Ok(PutObjectChecksum::None),
    }
}

fn extract_checksum_header(
    headers: &HeaderMap,
    header_name: &str,
    resource: &str,
) -> Result<Option<String>, RouteRejection> {
    headers
        .get(header_name)
        .map(|value| header_value_to_owned(value, resource))
        .transpose()
}

fn extract_put_object_body_encoding(
    headers: &HeaderMap,
    resource: &str,
) -> Result<PutObjectBodyEncoding, RouteRejection> {
    let Some(value) = headers.get(CONTENT_ENCODING) else {
        return Ok(PutObjectBodyEncoding::Plain);
    };
    let value = value
        .to_str()
        .map_err(|_| RouteRejection::new(S3ErrorCode::InvalidArgument, resource.to_owned()))?;

    if value.eq_ignore_ascii_case("aws-chunked") {
        Ok(PutObjectBodyEncoding::AwsChunked)
    } else {
        log_put_object_protocol_rejection(
            resource,
            "unsupported content encoding",
            CONTENT_ENCODING.as_str(),
            value,
        );
        Err(RouteRejection::new(
            S3ErrorCode::NotImplemented,
            resource.to_owned(),
        ))
    }
}

fn reject_unsupported_checksum_trailer(
    headers: &HeaderMap,
    resource: &str,
) -> Result<(), RouteRejection> {
    let Some(value) = headers.get(X_AMZ_TRAILER) else {
        return Ok(());
    };
    let value = value
        .to_str()
        .map_err(|_| RouteRejection::new(S3ErrorCode::InvalidArgument, resource.to_owned()))?;

    if value.eq_ignore_ascii_case(X_AMZ_CHECKSUM_CRC32)
        || value.eq_ignore_ascii_case(X_AMZ_CHECKSUM_CRC64NVME)
    {
        Ok(())
    } else {
        log_put_object_protocol_rejection(
            resource,
            "unsupported checksum trailer",
            X_AMZ_TRAILER,
            value,
        );
        Err(RouteRejection::new(
            S3ErrorCode::NotImplemented,
            resource.to_owned(),
        ))
    }
}

fn reject_unsupported_checksum_algorithm(
    headers: &HeaderMap,
    header_name: &str,
    resource: &str,
) -> Result<(), RouteRejection> {
    let Some(value) = headers.get(header_name) else {
        return Ok(());
    };
    let value = value
        .to_str()
        .map_err(|_| RouteRejection::new(S3ErrorCode::InvalidArgument, resource.to_owned()))?;

    if value.eq_ignore_ascii_case("CRC32") || value.eq_ignore_ascii_case("CRC64NVME") {
        Ok(())
    } else {
        log_put_object_protocol_rejection(
            resource,
            "unsupported checksum algorithm",
            header_name,
            value,
        );
        Err(RouteRejection::new(
            S3ErrorCode::NotImplemented,
            resource.to_owned(),
        ))
    }
}

fn decode_put_object_body(
    body_encoding: PutObjectBodyEncoding,
    bytes: &[u8],
    header_checksum: PutObjectChecksum,
    headers: &HeaderMap,
    resource: &str,
    request_id: &S3RequestId,
) -> RouteResponseResult<PutObjectBody> {
    match body_encoding {
        PutObjectBodyEncoding::Plain => Ok(PutObjectBody {
            bytes: bytes.to_vec(),
            checksums: put_object_checksums([header_checksum]),
        }),
        PutObjectBodyEncoding::AwsChunked => {
            let decoded = decode_aws_chunked_body(bytes, resource, request_id)?;
            validate_decoded_content_length(headers, decoded.bytes.len(), resource, request_id)?;
            Ok(PutObjectBody {
                bytes: decoded.bytes,
                checksums: put_object_checksums([header_checksum])
                    .into_iter()
                    .chain(decoded.trailer_checksum)
                    .collect(),
            })
        }
    }
}

fn validate_decoded_content_length(
    headers: &HeaderMap,
    decoded_len: usize,
    resource: &str,
    request_id: &S3RequestId,
) -> RouteResponseResult<()> {
    let Some(value) = headers.get(X_AMZ_DECODED_CONTENT_LENGTH) else {
        return Ok(());
    };
    let expected = value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<usize>().ok());

    if expected == Some(decoded_len) {
        Ok(())
    } else {
        Err(Box::new(invalid_argument_response(
            "x-amz-decoded-content-length does not match the decoded object body length.",
            resource,
            request_id,
        )))
    }
}

fn decode_aws_chunked_body(
    bytes: &[u8],
    resource: &str,
    request_id: &S3RequestId,
) -> RouteResponseResult<AwsChunkedBody> {
    let mut position = 0;
    let mut decoded = Vec::new();

    loop {
        let chunk_header = read_crlf_line(bytes, &mut position)
            .ok_or_else(|| Box::new(malformed_aws_chunked_response(resource, request_id)))?;
        let chunk_size = parse_aws_chunk_size(chunk_header)
            .ok_or_else(|| Box::new(malformed_aws_chunked_response(resource, request_id)))?;
        if chunk_size == 0 {
            let trailer_checksum =
                read_aws_chunked_trailer(bytes, &mut position, resource, request_id)?;
            if position != bytes.len() {
                return Err(Box::new(malformed_aws_chunked_response(
                    resource, request_id,
                )));
            }
            return Ok(AwsChunkedBody {
                bytes: decoded,
                trailer_checksum,
            });
        }

        let chunk_end = position
            .checked_add(chunk_size)
            .ok_or_else(|| Box::new(malformed_aws_chunked_response(resource, request_id)))?;
        let chunk_crlf_end = chunk_end
            .checked_add(2)
            .ok_or_else(|| Box::new(malformed_aws_chunked_response(resource, request_id)))?;
        if chunk_crlf_end > bytes.len() || &bytes[chunk_end..chunk_crlf_end] != b"\r\n" {
            return Err(Box::new(malformed_aws_chunked_response(
                resource, request_id,
            )));
        }
        decoded.extend_from_slice(&bytes[position..chunk_end]);
        position = chunk_crlf_end;
    }
}

fn read_aws_chunked_trailer(
    bytes: &[u8],
    position: &mut usize,
    resource: &str,
    request_id: &S3RequestId,
) -> RouteResponseResult<Option<PutObjectChecksum>> {
    let mut trailer_checksum = None;
    while let Some(line) = read_crlf_line(bytes, position) {
        if line.is_empty() {
            return Ok(trailer_checksum);
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(Box::new(invalid_argument_response(
                "aws-chunked trailer is malformed.",
                resource,
                request_id,
            )));
        };
        if name.trim().eq_ignore_ascii_case(X_AMZ_CHECKSUM_CRC32) {
            trailer_checksum = Some(PutObjectChecksum::Crc32(value.trim().to_owned()));
        } else if name.trim().eq_ignore_ascii_case(X_AMZ_CHECKSUM_CRC64NVME) {
            trailer_checksum = Some(PutObjectChecksum::Crc64Nvme(value.trim().to_owned()));
        }
    }

    Err(Box::new(invalid_argument_response(
        "aws-chunked trailer is malformed.",
        resource,
        request_id,
    )))
}

fn read_crlf_line<'a>(bytes: &'a [u8], position: &mut usize) -> Option<&'a str> {
    let relative_end = bytes
        .get(*position..)?
        .windows(2)
        .position(|window| window == b"\r\n")?;
    let line_start = *position;
    let line_end = line_start + relative_end;
    *position = line_end + 2;
    std::str::from_utf8(&bytes[line_start..line_end]).ok()
}

fn parse_aws_chunk_size(header: &str) -> Option<usize> {
    let size = header.split_once(';').map_or(header, |(size, _)| size);
    usize::from_str_radix(size, 16).ok()
}

fn malformed_aws_chunked_response(resource: &str, request_id: &S3RequestId) -> Response<Body> {
    invalid_argument_response(
        "aws-chunked request body is malformed.",
        resource,
        request_id,
    )
}

fn invalid_argument_response(
    message: impl Into<String>,
    resource: &str,
    request_id: &S3RequestId,
) -> Response<Body> {
    s3_error_response(
        S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            message,
            resource,
            request_id.clone(),
        ),
        false,
    )
}

fn validate_put_object_checksum(
    checksum: &PutObjectChecksum,
    bytes: &[u8],
    resource: &str,
    request_id: &S3RequestId,
) -> RouteResponseResult<()> {
    match checksum {
        PutObjectChecksum::None => Ok(()),
        PutObjectChecksum::Crc32(expected) => {
            let actual = crc32_base64(bytes);
            if expected == &actual {
                Ok(())
            } else {
                Err(Box::new(s3_error_response(
                    S3Error::with_message(
                        S3ErrorCode::BadDigest,
                        format!(
                            "The x-amz-checksum-crc32 value did not match the object body. Expected {expected}, computed {actual}."
                        ),
                        resource,
                        request_id.clone(),
                    ),
                    false,
                )))
            }
        }
        PutObjectChecksum::Crc64Nvme(expected) => {
            let actual = crc64_nvme_base64(bytes);
            if expected == &actual {
                Ok(())
            } else {
                Err(Box::new(s3_error_response(
                    S3Error::with_message(
                        S3ErrorCode::BadDigest,
                        format!(
                            "The x-amz-checksum-crc64nvme value did not match the object body. Expected {expected}, computed {actual}."
                        ),
                        resource,
                        request_id.clone(),
                    ),
                    false,
                )))
            }
        }
    }
}

fn validate_signed_put_object_payload_hash(
    headers: &HeaderMap,
    presigned_payload_hash: RouteResponseResult<Option<String>>,
    bytes: &[u8],
    resource: &str,
    request_id: &S3RequestId,
) -> RouteResponseResult<Option<PayloadVerificationOutcome>> {
    let expected = if headers.contains_key(AUTHORIZATION) {
        let Some(expected) = headers
            .get(X_AMZ_CONTENT_SHA256)
            .and_then(|value| value.to_str().ok())
        else {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(Box::new(s3_error_response(
                    S3Error::with_message(
                        S3ErrorCode::XAmzContentSHA256Mismatch,
                        "Signed PUT object requests with a body must include x-amz-content-sha256. Sign the exact bytes sent in the PUT request body, or send UNSIGNED-PAYLOAD when payload integrity is intentionally disabled.",
                        resource,
                        request_id.clone(),
                    ),
                    false,
                )))
            };
        };
        Some(expected.to_owned())
    } else {
        presigned_payload_hash?
    };

    let Some(expected) = expected else {
        return Ok(None);
    };

    match payload_hash_policy(&expected) {
        PayloadHashPolicy::LiteralSha256 => {
            validate_literal_payload_hash(&expected, bytes, resource, request_id).map(Some)
        }
        PayloadHashPolicy::UnsignedPayload => Ok(Some(PayloadVerificationOutcome::Partial(
            PayloadVerificationPartialReason::UnsignedPayloadMarker,
        ))),
        PayloadHashPolicy::StreamingPayload => Ok(Some(PayloadVerificationOutcome::Partial(
            PayloadVerificationPartialReason::StreamingPayloadMarker,
        ))),
        PayloadHashPolicy::Unsupported => Err(Box::new(s3_error_response(
            S3Error::with_message(
                S3ErrorCode::XAmzContentSHA256Mismatch,
                "Signed PUT object requests must use a literal SHA-256 x-amz-content-sha256 value, UNSIGNED-PAYLOAD, or a STREAMING-* payload marker.",
                resource,
                request_id.clone(),
            ),
            false,
        ))),
    }
}

fn presigned_payload_hash(
    uri: &Uri,
    is_head: bool,
    request_id: &S3RequestId,
) -> RouteResponseResult<Option<String>> {
    let resource = resource_for_uri(uri);
    let query = sigv4_query_pairs(uri.query(), &resource, is_head, request_id)?;
    if !has_presigned_query_authorization(&query) {
        return Ok(None);
    }

    Ok(query
        .into_iter()
        .find(|(name, _)| name == X_AMZ_CONTENT_SHA256_QUERY)
        .map(|(_, value)| value))
}

fn validate_literal_payload_hash(
    expected: &str,
    bytes: &[u8],
    resource: &str,
    request_id: &S3RequestId,
) -> RouteResponseResult<PayloadVerificationOutcome> {
    let actual = sha256_lower_hex(bytes);
    if expected.eq_ignore_ascii_case(&actual) {
        Ok(PayloadVerificationOutcome::Full)
    } else {
        Err(Box::new(s3_error_response(
            S3Error::with_message(
                S3ErrorCode::XAmzContentSHA256Mismatch,
                "The x-amz-content-sha256 header value did not match the request body bytes. Sign the exact bytes sent in the PUT request body.",
                resource,
                request_id.clone(),
            ),
            false,
        )))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PayloadHashPolicy {
    LiteralSha256,
    UnsignedPayload,
    StreamingPayload,
    Unsupported,
}

fn payload_hash_policy(value: &str) -> PayloadHashPolicy {
    if is_literal_sha256_payload_hash(value) {
        PayloadHashPolicy::LiteralSha256
    } else if value == UNSIGNED_PAYLOAD {
        PayloadHashPolicy::UnsignedPayload
    } else if value.starts_with(STREAMING_PAYLOAD_PREFIX) {
        PayloadHashPolicy::StreamingPayload
    } else {
        PayloadHashPolicy::Unsupported
    }
}

fn is_literal_sha256_payload_hash(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
}

fn sha256_lower_hex(bytes: &[u8]) -> String {
    hex_encode(&Sha256::digest(bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn put_object_checksums(
    checksums: impl IntoIterator<Item = PutObjectChecksum>,
) -> Vec<PutObjectChecksum> {
    checksums
        .into_iter()
        .filter(|checksum| !matches!(checksum, PutObjectChecksum::None))
        .collect()
}

type RouteResponseResult<T> = Result<T, Box<Response<Body>>>;

#[derive(Debug, Clone, Eq, PartialEq)]
struct OwnedSigV4VerificationRequest {
    request_datetime: String,
    method: String,
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    payload_hash: String,
}

impl OwnedSigV4VerificationRequest {
    fn query_refs(&self) -> Vec<(&str, &str)> {
        self.query
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }

    fn header_refs(&self) -> Vec<(&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ListObjectsV2RouteRequest {
    bucket: BucketName,
    prefix: Option<ObjectKey>,
    delimiter: Option<String>,
    continuation_token: Option<String>,
    max_keys: usize,
    encoding: Option<ListObjectsEncoding>,
}

#[derive(Debug)]
struct PutObjectRouteRequest {
    bucket: BucketName,
    key: ObjectKey,
    presigned_payload_hash: RouteResponseResult<Option<String>>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PutObjectBodyEncoding {
    Plain,
    AwsChunked,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PutObjectBody {
    bytes: Vec<u8>,
    checksums: Vec<PutObjectChecksum>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct AwsChunkedBody {
    bytes: Vec<u8>,
    trailer_checksum: Option<PutObjectChecksum>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
enum PutObjectChecksum {
    #[default]
    None,
    Crc32(String),
    Crc64Nvme(String),
}

fn crc32_base64(bytes: &[u8]) -> String {
    base64_bytes(&crc32(bytes).to_be_bytes())
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

fn crc64_nvme_base64(bytes: &[u8]) -> String {
    base64_bytes(&crc64_nvme(bytes).to_be_bytes())
}

fn crc64_nvme(bytes: &[u8]) -> u64 {
    let mut crc = 0xffff_ffff_ffff_ffff;
    for byte in bytes {
        crc ^= u64::from(*byte);
        for _ in 0..8 {
            let mask = 0u64.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x9a6c_9329_ac4b_c9b5 & mask);
        }
    }
    !crc
}

fn base64_bytes(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);

        encoded.push(ALPHABET[(b0 >> 2) as usize] as char);
        encoded.push(ALPHABET[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(b2 & 0b0011_1111) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

fn invalid_user_metadata(resource: &str) -> RouteRejection {
    RouteRejection::new(S3ErrorCode::InvalidArgument, resource.to_owned())
}

fn unsupported_put_object_header(headers: &HeaderMap) -> Option<String> {
    headers.keys().find_map(|name| {
        let name = name.as_str();
        if UNSUPPORTED_PUT_OBJECT_HEADERS.contains(&name)
            || UNSUPPORTED_PUT_OBJECT_HEADER_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
        {
            Some(name.to_owned())
        } else {
            None
        }
    })
}

fn log_put_object_rejection(
    headers: &HeaderMap,
    resource: &str,
    request_id: &S3RequestId,
    reason: &str,
    detail: &str,
) {
    tracing::info!(
        resource = %resource,
        request_id = %request_id.as_str(),
        reason = %reason,
        detail = %detail,
        header_names = %put_object_diagnostic_header_names(headers),
        diagnostic_headers = %put_object_diagnostic_header_values(headers),
        "put object rejected"
    );
}

fn log_put_object_protocol_rejection(resource: &str, reason: &str, header_name: &str, value: &str) {
    tracing::info!(
        resource = %resource,
        reason = %reason,
        header_name = %header_name,
        header_value = %value,
        "put object rejected"
    );
}

fn put_object_diagnostic_header_names(headers: &HeaderMap) -> String {
    headers
        .keys()
        .map(|name| name.as_str().to_owned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",")
}

fn put_object_diagnostic_header_values(headers: &HeaderMap) -> String {
    SAFE_PUT_OBJECT_DIAGNOSTIC_VALUE_HEADERS
        .iter()
        .filter_map(|name| {
            headers.get(*name).map(|value| {
                let value = value.to_str().unwrap_or("<non-utf8>");
                format!("{name}={value}")
            })
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn head_from_response(response: Response<Body>) -> Response<Body> {
    let (parts, _) = response.into_parts();
    Response::from_parts(parts, Body::empty())
}

fn status_code(status: u16) -> StatusCode {
    StatusCode::from_u16(status).expect("S3 error status mapping uses valid HTTP status codes")
}

fn resource_for_uri(uri: &Uri) -> String {
    uri.path().to_owned()
}

fn bucket_resource(bucket: &BucketName) -> String {
    format!("/{bucket}")
}

fn object_resource(bucket: &BucketName, key: &ObjectKey) -> String {
    format!("{}/{}", bucket_resource(bucket), key)
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum RouteResource {
    Root,
    Bucket { bucket: BucketName },
    Object { bucket: BucketName, key: ObjectKey },
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct QueryParams {
    values: BTreeMap<String, String>,
}

impl QueryParams {
    fn get(&self, name: &str) -> Option<&String> {
        self.values.get(name)
    }

    fn has_unsupported_subresource(&self) -> bool {
        self.values
            .keys()
            .any(|name| UNSUPPORTED_SUBRESOURCES.contains(&name.as_str()))
    }

    fn has_unsupported_list_objects_v2_param(&self) -> bool {
        self.values
            .keys()
            .any(|name| UNSUPPORTED_LIST_OBJECTS_V2_PARAMS.contains(&name.as_str()))
    }

    fn has_presigned_query_authorization(&self) -> bool {
        self.values
            .keys()
            .any(|name| PRESIGNED_QUERY_AUTH_PARAMS.contains(&name.as_str()))
    }
}

impl RouteRejection {
    fn new(code: S3ErrorCode, resource: impl Into<String>) -> Self {
        Self {
            code,
            resource: resource.into(),
        }
    }
}

const UNSUPPORTED_SUBRESOURCES: &[&str] = &[
    "acl",
    "delete",
    "tagging",
    "uploads",
    "uploadId",
    "partNumber",
    "versionId",
    "versions",
    "policy",
    "location",
    "cors",
    "website",
    "lifecycle",
    "notification",
    "replication",
    "encryption",
    "retention",
    "legal-hold",
    "object-lock",
];

const UNSUPPORTED_LIST_OBJECTS_V2_PARAMS: &[&str] = &[START_AFTER, FETCH_OWNER];

const PRESIGNED_QUERY_AUTH_PARAMS: &[&str] = &[
    X_AMZ_ALGORITHM_QUERY,
    X_AMZ_CREDENTIAL_QUERY,
    X_AMZ_DATE_QUERY,
    X_AMZ_EXPIRES_QUERY,
    X_AMZ_SIGNED_HEADERS_QUERY,
    X_AMZ_SIGNATURE_QUERY,
    X_AMZ_CONTENT_SHA256_QUERY,
    X_AMZ_SECURITY_TOKEN_QUERY,
];

const PRESIGNED_RESPONSE_OVERRIDE_PARAMS: &[&str] = &[
    RESPONSE_CACHE_CONTROL_QUERY,
    RESPONSE_CONTENT_DISPOSITION_QUERY,
    RESPONSE_CONTENT_ENCODING_QUERY,
    RESPONSE_CONTENT_LANGUAGE_QUERY,
    RESPONSE_CONTENT_TYPE_QUERY,
    RESPONSE_EXPIRES_QUERY,
];

const UNSUPPORTED_PUT_OBJECT_HEADERS: &[&str] = &[
    "cache-control",
    "content-disposition",
    "content-language",
    "content-md5",
    "expires",
    "x-amz-acl",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
    "x-amz-copy-source",
    "x-amz-expected-bucket-owner",
    "x-amz-request-payer",
    "x-amz-server-side-encryption",
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-bucket-key-enabled",
    "x-amz-server-side-encryption-context",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
    "x-amz-storage-class",
    "x-amz-tagging",
    "x-amz-website-redirect-location",
    "x-amz-write-offset-bytes",
];

const UNSUPPORTED_PUT_OBJECT_HEADER_PREFIXES: &[&str] = &["x-amz-grant-", "x-amz-object-lock-"];

const SAFE_PUT_OBJECT_DIAGNOSTIC_VALUE_HEADERS: &[&str] = &[
    "content-encoding",
    "transfer-encoding",
    X_AMZ_CHECKSUM_ALGORITHM,
    X_AMZ_CHECKSUM_CRC32,
    X_AMZ_CHECKSUM_CRC64NVME,
    X_AMZ_DECODED_CONTENT_LENGTH,
    X_AMZ_SDK_CHECKSUM_ALGORITHM,
    X_AMZ_TRAILER,
];

#[cfg(test)]
mod tests {
    use super::{
        crc32_base64, crc64_nvme_base64, put_object_diagnostic_header_names,
        put_object_diagnostic_header_values, resolve_operation, s3_error_code_from_storage_error,
        s3_error_from_storage_error, safe_log_path, RouteScope,
    };
    use crate::s3::bucket::BucketName;
    use crate::s3::error::{S3ErrorCode, S3RequestId, STATIC_REQUEST_ID};
    use crate::s3::object::ObjectKey;
    use crate::s3::operation::S3Operation;
    use crate::storage::StorageError;
    use axum::http::{HeaderMap, HeaderValue, Method, Uri};
    use std::path::PathBuf;

    #[test]
    fn list_buckets_route_resolves() {
        let route = resolve_operation(&Method::GET, &Uri::from_static("/")).expect("route");

        assert_eq!(route.scope, RouteScope::PathStyle);
        assert_eq!(route.operation, S3Operation::ListBuckets);
    }

    #[test]
    fn safe_log_path_omits_query_string_credentials() {
        let uri = Uri::from_static(
            "/example-bucket/object.txt?X-Amz-Credential=secret&X-Amz-Signature=signature",
        );

        assert_eq!(safe_log_path(&uri), "/example-bucket/object.txt");
    }

    #[test]
    fn put_object_diagnostic_header_values_only_include_safe_protocol_values() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=secret, Signature=super-secret"),
        );
        headers.insert("content-encoding", HeaderValue::from_static("aws-chunked"));
        headers.insert(
            "x-amz-sdk-checksum-algorithm",
            HeaderValue::from_static("CRC64NVME"),
        );
        headers.insert(
            "x-amz-trailer",
            HeaderValue::from_static("x-amz-checksum-crc64nvme"),
        );
        headers.insert(
            "x-amz-checksum-crc64nvme",
            HeaderValue::from_static("M3eFcAZSQlc="),
        );

        assert_eq!(
            put_object_diagnostic_header_values(&headers),
            "content-encoding=aws-chunked,x-amz-checksum-crc64nvme=M3eFcAZSQlc=,x-amz-sdk-checksum-algorithm=CRC64NVME,x-amz-trailer=x-amz-checksum-crc64nvme"
        );
        assert!(!put_object_diagnostic_header_values(&headers).contains("secret"));
    }

    #[test]
    fn checksum_encodings_match_known_vectors() {
        assert_eq!(crc32_base64(b"hello"), "NhCmhg==");
        assert_eq!(crc64_nvme_base64(b"hello"), "M3eFcAZSQlc=");
        assert_eq!(crc64_nvme_base64(b"123456789"), "rosUhgp5mIg=");
    }

    #[test]
    fn put_object_diagnostic_header_names_are_sorted_and_value_free() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-case", HeaderValue::from_static("private-value"));
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        headers.insert("authorization", HeaderValue::from_static("secret"));

        assert_eq!(
            put_object_diagnostic_header_names(&headers),
            "authorization,content-type,x-amz-meta-case"
        );
    }

    #[test]
    fn bucket_route_rejects_unknown_method() {
        let rejection =
            resolve_operation(&Method::POST, &Uri::from_static("/example-bucket")).unwrap_err();

        assert_eq!(rejection.code, S3ErrorCode::MethodNotAllowed);
    }

    #[test]
    fn storage_errors_map_to_s3_error_codes_at_server_boundary() {
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("missing.txt");

        let cases = [
            (
                StorageError::BucketAlreadyExists {
                    bucket: bucket.clone(),
                },
                S3ErrorCode::BucketAlreadyOwnedByYou,
            ),
            (
                StorageError::BucketNotEmpty {
                    bucket: bucket.clone(),
                },
                S3ErrorCode::BucketNotEmpty,
            ),
            (
                StorageError::NoSuchBucket {
                    bucket: bucket.clone(),
                },
                S3ErrorCode::NoSuchBucket,
            ),
            (
                StorageError::NoSuchKey {
                    bucket: bucket.clone(),
                    key,
                },
                S3ErrorCode::NoSuchKey,
            ),
            (
                StorageError::InvalidBucketName {
                    bucket: "bad_bucket".to_owned(),
                },
                S3ErrorCode::InvalidBucketName,
            ),
            (
                StorageError::InvalidObjectKey { key: String::new() },
                S3ErrorCode::InvalidArgument,
            ),
            (
                StorageError::InvalidArgument {
                    message: "bad continuation token".to_owned(),
                },
                S3ErrorCode::InvalidArgument,
            ),
            (
                StorageError::Io {
                    path: PathBuf::from("metadata.json"),
                    source: std::io::Error::other("disk error"),
                },
                S3ErrorCode::InternalError,
            ),
            (
                StorageError::CorruptState {
                    path: PathBuf::from("metadata.json"),
                    message: "invalid json".to_owned(),
                },
                S3ErrorCode::InternalError,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(s3_error_code_from_storage_error(&error), expected);
        }
    }

    #[test]
    fn storage_error_conversion_keeps_resource_and_request_id() {
        let error = s3_error_from_storage_error(
            StorageError::NoSuchBucket {
                bucket: BucketName::new("example-bucket"),
            },
            "/example-bucket",
            S3RequestId::new(STATIC_REQUEST_ID),
        );

        assert_eq!(error.code, S3ErrorCode::NoSuchBucket);
        assert_eq!(error.resource, "/example-bucket");
        assert_eq!(error.request_id.as_str(), STATIC_REQUEST_ID);
    }

    #[test]
    fn invalid_storage_argument_conversion_keeps_actionable_message() {
        let error = s3_error_from_storage_error(
            StorageError::InvalidObjectKey { key: String::new() },
            "/example-bucket/",
            S3RequestId::new(STATIC_REQUEST_ID),
        );

        assert_eq!(error.code, S3ErrorCode::InvalidArgument);
        assert_eq!(error.message, "invalid object key: ");
    }

    #[test]
    fn internal_storage_error_conversion_uses_generic_message() {
        let error = s3_error_from_storage_error(
            StorageError::Io {
                path: PathBuf::from("C:\\private\\bucket\\metadata.json"),
                source: std::io::Error::other("disk failure at private path"),
            },
            "/example-bucket",
            S3RequestId::new(STATIC_REQUEST_ID),
        );

        assert_eq!(error.code, S3ErrorCode::InternalError);
        assert_eq!(
            error.message,
            "We encountered an internal error. Please try again."
        );
        assert!(!error.message.contains("C:\\private"));
        assert!(!error.message.contains("disk failure"));
        assert_eq!(error.resource, "/example-bucket");
        assert_eq!(error.request_id.as_str(), STATIC_REQUEST_ID);
    }
}
