// SPDX-License-Identifier: Apache-2.0

use crate::s3::bucket::{is_valid_s3_bucket_name, BucketName};
use crate::s3::error::{S3Error, S3ErrorCode, S3RequestId};
use crate::s3::object::{is_valid_s3_object_key, is_valid_s3_object_key_prefix, ObjectKey};
use crate::s3::operation::S3Operation;
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
use axum::body::{to_bytes, Body, Bytes};
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE};
use axum::http::HeaderName;
use axum::http::{HeaderMap, HeaderValue, Method, Response, StatusCode, Uri};
use http_body_util::LengthLimitError;
use percent_encoding::percent_decode_str;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::path::PathBuf;

const REQUEST_ID_HEADER: &str = "x-amz-request-id";
const USER_METADATA_HEADER_PREFIX: &str = "x-amz-meta-";
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

    let response = match resolve_operation(&method, &uri) {
        Ok(route_match) => {
            execute_operation(
                state,
                headers,
                body,
                route_match.operation,
                is_head,
                request_id.clone(),
            )
            .await
        }
        Err(rejection) => route_error_response(rejection, is_head, &request_id),
    };
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
    })
}

fn resolve_object_operation(
    method: &Method,
    bucket: BucketName,
    key: ObjectKey,
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

async fn execute_operation(
    state: ServerState,
    headers: HeaderMap,
    body: Body,
    operation: S3Operation,
    is_head: bool,
    request_id: S3RequestId,
) -> Response<Body> {
    let result = match operation {
        S3Operation::ListBuckets => state
            .storage()
            .list_buckets()
            .map(|buckets| {
                xml_response(
                    list_buckets_response_xml(&list_buckets_xml(buckets)),
                    &request_id,
                )
            })
            .map_err(|error| (error, "/".to_owned())),
        S3Operation::CreateBucket { bucket } => state
            .storage()
            .create_bucket(&bucket)
            .map(|()| empty_response(&request_id))
            .map_err(|error| (error, bucket_resource(&bucket))),
        S3Operation::HeadBucket { bucket } => match state.storage().bucket_exists(&bucket) {
            Ok(true) => Ok(empty_response(&request_id)),
            Ok(false) => Err((
                StorageError::NoSuchBucket {
                    bucket: bucket.clone(),
                },
                bucket_resource(&bucket),
            )),
            Err(error) => Err((error, bucket_resource(&bucket))),
        },
        S3Operation::DeleteBucket { bucket } => state
            .storage()
            .delete_bucket(&bucket)
            .map(|()| no_content_response(&request_id))
            .map_err(|error| (error, bucket_resource(&bucket))),
        S3Operation::PutObject { bucket, key } => {
            if let Some(code) = unsupported_put_object_header(&headers) {
                return route_error_response(
                    RouteRejection::new(code, object_resource(&bucket, &key)),
                    is_head,
                    &request_id,
                );
            }
            let resource = object_resource(&bucket, &key);
            let content_type = match extract_content_type(&headers, &resource) {
                Ok(content_type) => content_type,
                Err(rejection) => return route_error_response(rejection, is_head, &request_id),
            };
            let user_metadata = match extract_user_metadata(&headers, &resource) {
                Ok(user_metadata) => user_metadata,
                Err(rejection) => return route_error_response(rejection, is_head, &request_id),
            };
            let bytes = match read_put_object_body(body, &resource, is_head, &request_id).await {
                Ok(bytes) => bytes,
                Err(response) => return response,
            };
            state
                .storage()
                .put_object(PutObjectRequest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    bytes: bytes.to_vec(),
                    content_type,
                    user_metadata,
                })
                .map(|metadata| put_object_response(metadata, &request_id))
                .map_err(|error| (error, resource))
        }
        S3Operation::GetObject { bucket, key } => {
            if headers.contains_key(RANGE) {
                return route_error_response(
                    RouteRejection::new(
                        S3ErrorCode::NotImplemented,
                        object_resource(&bucket, &key),
                    ),
                    is_head,
                    &request_id,
                );
            }
            state
                .storage()
                .get_object(&bucket, &key)
                .and_then(|object| object_response(object, &request_id))
                .map_err(|error| (error, object_resource(&bucket, &key)))
        }
        S3Operation::HeadObject { bucket, key } => {
            if headers.contains_key(RANGE) {
                return route_error_response(
                    RouteRejection::new(
                        S3ErrorCode::NotImplemented,
                        object_resource(&bucket, &key),
                    ),
                    is_head,
                    &request_id,
                );
            }
            state
                .storage()
                .get_object_metadata(&bucket, &key)
                .and_then(|metadata| object_metadata_response(metadata, Body::empty(), &request_id))
                .map_err(|error| (error, object_resource(&bucket, &key)))
        }
        S3Operation::DeleteObject { bucket, key } => {
            match state.storage().delete_object(&bucket, &key) {
                Ok(()) | Err(StorageError::NoSuchKey { .. }) => {
                    Ok(no_content_response(&request_id))
                }
                Err(error) => Err((error, object_resource(&bucket, &key))),
            }
        }
        S3Operation::ListObjectsV2 {
            bucket,
            prefix,
            delimiter,
            continuation_token,
            max_keys,
        } => {
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
                    let listing = list_objects_v2_xml(listing);
                    xml_response(
                        list_objects_v2_response_xml(
                            &listing,
                            prefix.as_ref().map(ObjectKey::as_str),
                            delimiter.as_deref(),
                            request_continuation_token.as_deref(),
                        ),
                        &request_id,
                    )
                })
                .map_err(|error| (error, bucket_resource(&bucket)))
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
        Err((error, resource)) => storage_error_response(error, resource, is_head, &request_id),
    }
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

fn list_objects_v2_xml(listing: ObjectListing) -> ListObjectsV2Xml {
    ListObjectsV2Xml {
        bucket: listing.bucket.as_str().to_owned(),
        entries: listing
            .entries
            .into_iter()
            .map(|entry| match entry {
                ObjectListingEntry::Object(object) => {
                    ListObjectsV2XmlEntry::Object(ListObjectXml {
                        key: object.key.as_str().to_owned(),
                        etag: object.etag,
                        content_length: object.content_length,
                        last_modified: object.last_modified,
                    })
                }
                ObjectListingEntry::CommonPrefix(prefix) => {
                    ListObjectsV2XmlEntry::CommonPrefix(prefix.as_str().to_owned())
                }
            })
            .collect(),
        max_keys: listing.max_keys,
        is_truncated: listing.is_truncated,
        next_continuation_token: listing.next_continuation_token,
    }
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
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| RouteRejection::new(S3ErrorCode::InvalidArgument, resource.to_owned()))
        })
        .transpose()
}

fn invalid_user_metadata(resource: &str) -> RouteRejection {
    RouteRejection::new(S3ErrorCode::InvalidArgument, resource.to_owned())
}

fn unsupported_put_object_header(headers: &HeaderMap) -> Option<S3ErrorCode> {
    headers.keys().find_map(|name| {
        let name = name.as_str();
        if UNSUPPORTED_PUT_OBJECT_HEADERS.contains(&name)
            || UNSUPPORTED_PUT_OBJECT_HEADER_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
        {
            Some(S3ErrorCode::NotImplemented)
        } else {
            None
        }
    })
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

const UNSUPPORTED_LIST_OBJECTS_V2_PARAMS: &[&str] = &[ENCODING_TYPE, START_AFTER, FETCH_OWNER];

const UNSUPPORTED_PUT_OBJECT_HEADERS: &[&str] = &[
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-md5",
    "expires",
    "x-amz-acl",
    "x-amz-checksum-algorithm",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
    "x-amz-copy-source",
    "x-amz-expected-bucket-owner",
    "x-amz-request-payer",
    "x-amz-sdk-checksum-algorithm",
    "x-amz-server-side-encryption",
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-bucket-key-enabled",
    "x-amz-server-side-encryption-context",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
    "x-amz-storage-class",
    "x-amz-tagging",
    "x-amz-trailer",
    "x-amz-website-redirect-location",
    "x-amz-write-offset-bytes",
];

const UNSUPPORTED_PUT_OBJECT_HEADER_PREFIXES: &[&str] = &["x-amz-grant-", "x-amz-object-lock-"];

#[cfg(test)]
mod tests {
    use super::{
        resolve_operation, s3_error_code_from_storage_error, s3_error_from_storage_error,
        safe_log_path, RouteScope,
    };
    use crate::s3::bucket::BucketName;
    use crate::s3::error::{S3ErrorCode, S3RequestId, STATIC_REQUEST_ID};
    use crate::s3::object::ObjectKey;
    use crate::s3::operation::S3Operation;
    use crate::storage::StorageError;
    use axum::http::{Method, Uri};
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
