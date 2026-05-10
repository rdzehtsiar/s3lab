// SPDX-License-Identifier: Apache-2.0

use crate::s3::bucket::BucketName;
use crate::s3::error::{S3Error, S3ErrorCode, S3RequestId, TEST_REQUEST_ID};
use crate::s3::object::ObjectKey;
use crate::s3::operation::S3Operation;
use crate::s3::xml::{
    error_response_xml, list_buckets_response_xml, list_objects_v2_response_xml, XML_CONTENT_TYPE,
};
use crate::server::state::ServerState;
use crate::storage::{
    ListObjectsOptions, PutObjectRequest, StorageError, StoredObject, StoredObjectMetadata,
};
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED};
use axum::http::HeaderName;
use axum::http::{HeaderMap, HeaderValue, Method, Response, StatusCode, Uri};
use percent_encoding::percent_decode_str;
use std::collections::{BTreeMap, BTreeSet};
use time::{Month, OffsetDateTime, UtcOffset, Weekday};

const REQUEST_ID_HEADER: &str = "x-amz-request-id";
const USER_METADATA_HEADER_PREFIX: &str = "x-amz-meta-";
const LIST_TYPE: &str = "list-type";
const PREFIX: &str = "prefix";
const CONTINUATION_TOKEN: &str = "continuation-token";
const MAX_KEYS: &str = "max-keys";
const X_ID: &str = "x-id";

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
    body: Bytes,
) -> Response<Body> {
    let is_head = method == Method::HEAD;
    match resolve_operation(&method, &uri) {
        Ok(route_match) => execute_operation(state, headers, body, route_match.operation, is_head),
        Err(rejection) => route_error_response(rejection, is_head),
    }
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
    bucket: String,
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
    bucket: String,
    query: &QueryParams,
    resource: &str,
) -> Result<S3Operation, RouteRejection> {
    reject_query_params_except(
        query,
        [LIST_TYPE, PREFIX, CONTINUATION_TOKEN, MAX_KEYS],
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

    Ok(S3Operation::ListObjectsV2 {
        bucket,
        prefix: query.get(PREFIX).cloned(),
        continuation_token: query.get(CONTINUATION_TOKEN).cloned(),
        max_keys,
    })
}

fn resolve_object_operation(
    method: &Method,
    bucket: String,
    key: String,
    query: &QueryParams,
    resource: &str,
) -> Result<S3Operation, RouteRejection> {
    reject_query_params(query, resource)?;

    match *method {
        Method::PUT => Ok(S3Operation::PutObject { bucket, key }),
        Method::GET => Ok(S3Operation::GetObject { bucket, key }),
        Method::HEAD => Ok(S3Operation::HeadObject { bucket, key }),
        Method::DELETE => Ok(S3Operation::DeleteObject { bucket, key }),
        _ => Err(RouteRejection::new(
            S3ErrorCode::MethodNotAllowed,
            resource.to_owned(),
        )),
    }
}

fn execute_operation(
    state: ServerState,
    headers: HeaderMap,
    body: Bytes,
    operation: S3Operation,
    is_head: bool,
) -> Response<Body> {
    let result = match operation {
        S3Operation::ListBuckets => state
            .storage()
            .list_buckets()
            .map(|buckets| xml_response(list_buckets_response_xml(&buckets)))
            .map_err(|error| (error, "/".to_owned())),
        S3Operation::CreateBucket { bucket } => state
            .storage()
            .create_bucket(&BucketName::new(bucket.clone()))
            .map(|()| empty_response())
            .map_err(|error| (error, format!("/{bucket}"))),
        S3Operation::HeadBucket { bucket } => match state
            .storage()
            .bucket_exists(&BucketName::new(bucket.clone()))
        {
            Ok(true) => Ok(empty_response()),
            Ok(false) => Err((
                StorageError::NoSuchBucket {
                    bucket: BucketName::new(bucket.clone()),
                },
                format!("/{bucket}"),
            )),
            Err(error) => Err((error, format!("/{bucket}"))),
        },
        S3Operation::DeleteBucket { bucket } => state
            .storage()
            .delete_bucket(&BucketName::new(bucket.clone()))
            .map(|()| no_content_response())
            .map_err(|error| (error, format!("/{bucket}"))),
        S3Operation::PutObject { bucket, key } => {
            let content_type = headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let user_metadata = match extract_user_metadata(&headers) {
                Ok(user_metadata) => user_metadata,
                Err(error) => {
                    return storage_error_response(error, format!("/{bucket}/{key}"), is_head)
                }
            };
            state
                .storage()
                .put_object(PutObjectRequest {
                    bucket: BucketName::new(bucket.clone()),
                    key: ObjectKey::new(key.clone()),
                    bytes: body.to_vec(),
                    content_type,
                    user_metadata,
                })
                .map(|metadata| {
                    object_metadata_response(metadata, Body::empty(), ContentLength::Body)
                })
                .map_err(|error| (error, format!("/{bucket}/{key}")))
        }
        S3Operation::GetObject { bucket, key } => state
            .storage()
            .get_object(
                &BucketName::new(bucket.clone()),
                &ObjectKey::new(key.clone()),
            )
            .map(object_response)
            .map_err(|error| (error, format!("/{bucket}/{key}"))),
        S3Operation::HeadObject { bucket, key } => state
            .storage()
            .get_object_metadata(
                &BucketName::new(bucket.clone()),
                &ObjectKey::new(key.clone()),
            )
            .map(|metadata| {
                object_metadata_response(metadata, Body::empty(), ContentLength::Object)
            })
            .map_err(|error| (error, format!("/{bucket}/{key}"))),
        S3Operation::DeleteObject { bucket, key } => match state.storage().delete_object(
            &BucketName::new(bucket.clone()),
            &ObjectKey::new(key.clone()),
        ) {
            Ok(()) | Err(StorageError::NoSuchKey { .. }) => Ok(no_content_response()),
            Err(error) => Err((error, format!("/{bucket}/{key}"))),
        },
        S3Operation::ListObjectsV2 {
            bucket,
            prefix,
            continuation_token,
            max_keys,
        } => {
            let options = ListObjectsOptions {
                prefix: prefix.clone().map(ObjectKey::new),
                continuation_token,
                max_keys,
            };
            state
                .storage()
                .list_objects(&BucketName::new(bucket.clone()), options)
                .map(|listing| {
                    xml_response(list_objects_v2_response_xml(&listing, prefix.as_deref()))
                })
                .map_err(|error| (error, format!("/{bucket}")))
        }
    };

    match result {
        Ok(response) => {
            if is_head {
                head_from_response(response)
            } else {
                response
            }
        }
        Err((error, resource)) => storage_error_response(error, resource, is_head),
    }
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
    if bucket.is_empty() {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidBucketName,
            resource.to_owned(),
        ));
    }

    let Some(raw_key) = raw_key else {
        return Ok(RouteResource::Bucket { bucket });
    };

    let key = decode_route_component(raw_key, resource)?;
    if key.is_empty() {
        return Err(RouteRejection::new(
            S3ErrorCode::InvalidArgument,
            resource.to_owned(),
        ));
    }

    Ok(RouteResource::Object { bucket, key })
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

fn route_error_response(rejection: RouteRejection, is_head: bool) -> Response<Body> {
    s3_error_response(
        S3Error::new(
            rejection.code,
            rejection.resource,
            S3RequestId::new(TEST_REQUEST_ID),
        ),
        is_head,
    )
}

fn storage_error_response(
    error: StorageError,
    resource: impl Into<String>,
    is_head: bool,
) -> Response<Body> {
    s3_error_response(
        S3Error::from_storage_error(&error, resource, S3RequestId::new(TEST_REQUEST_ID)),
        is_head,
    )
}

fn s3_error_response(error: S3Error, is_head: bool) -> Response<Body> {
    let status = status_code(error.code.http_status_code());
    if is_head {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = status;
        response.headers_mut().insert(
            REQUEST_ID_HEADER,
            HeaderValue::from_str(error.request_id.as_str()).expect("static request id is valid"),
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
        HeaderValue::from_str(error.request_id.as_str()).expect("static request id is valid"),
    );
    response
}

fn empty_response() -> Response<Body> {
    Response::new(Body::empty())
}

fn no_content_response() -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    response
}

fn xml_response(xml: String) -> Response<Body> {
    let mut response = Response::new(Body::from(xml));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(XML_CONTENT_TYPE));
    response
}

fn object_response(object: StoredObject) -> Response<Body> {
    object_metadata_response(
        object.metadata,
        Body::from(object.bytes),
        ContentLength::Object,
    )
}

enum ContentLength {
    Body,
    Object,
}

fn object_metadata_response(
    metadata: StoredObjectMetadata,
    body: Body,
    content_length: ContentLength,
) -> Response<Body> {
    let mut response = Response::new(body);
    if matches!(content_length, ContentLength::Object) {
        response.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&metadata.content_length.to_string())
                .expect("content length is valid"),
        );
    }
    if let Some(content_type) = metadata.content_type {
        if let Ok(value) = HeaderValue::from_str(&content_type) {
            response.headers_mut().insert(CONTENT_TYPE, value);
        }
    }
    if let Ok(value) = HeaderValue::from_str(&metadata.etag) {
        response.headers_mut().insert(ETAG, value);
    }
    response.headers_mut().insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&http_date(metadata.last_modified)).expect("HTTP date is valid"),
    );
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(TEST_REQUEST_ID).expect("static request id is valid"),
    );
    for (key, value) in metadata.user_metadata {
        let header_name = format!("{USER_METADATA_HEADER_PREFIX}{key}");
        if let (Ok(header_name), Ok(header_value)) = (
            HeaderName::from_bytes(header_name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(header_name, header_value);
        }
    }
    response
}

fn extract_user_metadata(headers: &HeaderMap) -> Result<BTreeMap<String, String>, StorageError> {
    let mut user_metadata = BTreeMap::new();
    for (name, value) in headers {
        let Some(suffix) = name.as_str().strip_prefix(USER_METADATA_HEADER_PREFIX) else {
            continue;
        };
        if suffix.is_empty() {
            return Err(invalid_user_metadata("empty user metadata header name"));
        }

        let key = suffix.to_ascii_lowercase();
        let value = value
            .to_str()
            .map_err(|_| invalid_user_metadata("user metadata values must be valid UTF-8"))?
            .to_owned();
        if user_metadata.insert(key, value).is_some() {
            return Err(invalid_user_metadata(
                "duplicate user metadata header name after lowercase normalization",
            ));
        }
    }

    Ok(user_metadata)
}

fn invalid_user_metadata(message: impl Into<String>) -> StorageError {
    StorageError::InvalidArgument {
        message: message.into(),
    }
}

fn http_date(timestamp: OffsetDateTime) -> String {
    let timestamp = timestamp.to_offset(UtcOffset::UTC);
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        weekday_name(timestamp.weekday()),
        timestamp.day(),
        month_name(timestamp.month()),
        timestamp.year(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second()
    )
}

fn weekday_name(weekday: Weekday) -> &'static str {
    match weekday {
        Weekday::Monday => "Mon",
        Weekday::Tuesday => "Tue",
        Weekday::Wednesday => "Wed",
        Weekday::Thursday => "Thu",
        Weekday::Friday => "Fri",
        Weekday::Saturday => "Sat",
        Weekday::Sunday => "Sun",
    }
}

fn month_name(month: Month) -> &'static str {
    match month {
        Month::January => "Jan",
        Month::February => "Feb",
        Month::March => "Mar",
        Month::April => "Apr",
        Month::May => "May",
        Month::June => "Jun",
        Month::July => "Jul",
        Month::August => "Aug",
        Month::September => "Sep",
        Month::October => "Oct",
        Month::November => "Nov",
        Month::December => "Dec",
    }
}

fn head_from_response(response: Response<Body>) -> Response<Body> {
    let (parts, _) = response.into_parts();
    Response::from_parts(parts, Body::empty())
}

fn status_code(status: u16) -> StatusCode {
    StatusCode::from_u16(status).expect("S3 error status mapping uses valid HTTP status codes")
}

fn resource_for_uri(uri: &Uri) -> String {
    uri.path_and_query()
        .map(|path_and_query| path_and_query.as_str().to_owned())
        .unwrap_or_else(|| uri.path().to_owned())
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum RouteResource {
    Root,
    Bucket { bucket: String },
    Object { bucket: String, key: String },
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

#[cfg(test)]
mod tests {
    use super::{resolve_operation, RouteScope};
    use crate::s3::error::S3ErrorCode;
    use crate::s3::operation::S3Operation;
    use axum::http::{Method, Uri};

    #[test]
    fn list_buckets_route_resolves() {
        let route = resolve_operation(&Method::GET, &Uri::from_static("/")).expect("route");

        assert_eq!(route.scope, RouteScope::PathStyle);
        assert_eq!(route.operation, S3Operation::ListBuckets);
    }

    #[test]
    fn bucket_route_rejects_unknown_method() {
        let rejection =
            resolve_operation(&Method::POST, &Uri::from_static("/example-bucket")).unwrap_err();

        assert_eq!(rejection.code, S3ErrorCode::MethodNotAllowed);
    }
}
