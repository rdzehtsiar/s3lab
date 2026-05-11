// SPDX-License-Identifier: Apache-2.0

use super::key::{encode_bucket_name, encode_object_key, EncodedObjectKey};
use super::{
    BucketSummary, ListObjectsOptions, ObjectListing, ObjectListingEntry, PutObjectRequest,
    Storage, StorageError, StoredObject, StoredObjectMetadata, DEFAULT_OBJECT_CONTENT_TYPE,
    STORAGE_ROOT_DIR,
};
use crate::s3::bucket::{is_valid_s3_bucket_name, BucketName};
use crate::s3::object::{is_valid_s3_object_key_prefix, ObjectKey};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, Write};
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use time::OffsetDateTime;

const BUCKET_METADATA_FILE: &str = "bucket.json";
const OBJECTS_DIR: &str = "objects";
const OBJECT_CONTENT_FILE: &str = "content.bin";
const OBJECT_METADATA_FILE: &str = "metadata.json";
const OBJECT_METADATA_SCHEMA_VERSION: u32 = 1;
const CONTINUATION_TOKEN_PREFIX: &str = "s3lab-v1:";
#[cfg(windows)]
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
static TEMPORARY_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
const SIBLING_PATH_RETRY_LIMIT: u64 = 64;
// Process-local serialization keeps filesystem transitions invisible to other
// public storage operations while the storage layout is intentionally simple.
static STORAGE_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_NEW_OBJECT_STATE_WRITE: Cell<bool> = const { Cell::new(false) };
}

pub trait StorageClock {
    fn now_utc(&self) -> OffsetDateTime;
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct SystemClock;

impl StorageClock for SystemClock {
    fn now_utc(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FilesystemStorage<C = SystemClock> {
    root: PathBuf,
    clock: C,
}

impl FilesystemStorage<SystemClock> {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_clock(root, SystemClock)
    }
}

impl<C> FilesystemStorage<C> {
    pub fn with_clock(root: impl Into<PathBuf>, clock: C) -> Self {
        Self {
            root: root.into(),
            clock,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl<C: StorageClock> Storage for FilesystemStorage<C> {
    fn create_bucket(&self, bucket: &BucketName) -> Result<(), StorageError> {
        if !bucket.is_valid_s3_name() {
            return Err(StorageError::InvalidBucketName {
                bucket: bucket.as_str().to_owned(),
            });
        }

        let _guard = lock_storage(&self.root, "create_bucket")?;
        let bucket_root = self.bucket_root();
        create_dir_all(&bucket_root)?;

        let bucket_dir = self.bucket_dir(bucket);
        if path_exists(&bucket_dir)? {
            let record = self.read_bucket_record(bucket)?;
            if record.bucket != bucket.as_str() {
                return Err(corrupt_state(
                    self.bucket_metadata_path(bucket),
                    "bucket metadata does not match encoded bucket path",
                ));
            }

            return Err(StorageError::BucketAlreadyExists {
                bucket: bucket.clone(),
            });
        }

        write_new_bucket_state_atomically(
            &bucket_dir,
            &BucketRecord {
                bucket: bucket.as_str().to_owned(),
            },
            bucket,
        )
    }

    fn list_buckets(&self) -> Result<Vec<BucketSummary>, StorageError> {
        let _guard = lock_storage(&self.root, "list_buckets")?;
        let bucket_root = self.bucket_root();
        if !path_exists(&bucket_root)? {
            return Ok(Vec::new());
        }

        let mut buckets = Vec::new();
        for entry in sorted_directory_entries(&bucket_root)? {
            if is_hidden_storage_entry(&entry) {
                continue;
            }

            let entry_path = entry.path();
            let Some(entry_metadata) = storage_path_metadata(&entry_path)? else {
                continue;
            };
            if !entry_metadata.is_dir() {
                continue;
            }

            let metadata_path = entry_path.join(BUCKET_METADATA_FILE);
            if !path_exists(&metadata_path)? {
                return Err(corrupt_state(metadata_path, "missing bucket metadata"));
            }

            let record: BucketRecord = read_json(&metadata_path)?;
            validate_listed_bucket_record(&entry_path, &metadata_path, &record)?;
            require_storage_directory(
                &entry_path.join(OBJECTS_DIR),
                "missing bucket objects directory",
            )?;
            buckets.push(BucketSummary {
                name: BucketName::new(record.bucket),
            });
        }

        buckets.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(buckets)
    }

    fn bucket_exists(&self, bucket: &BucketName) -> Result<bool, StorageError> {
        let _guard = lock_storage(&self.root, "bucket_exists")?;
        let bucket_dir = self.bucket_dir(bucket);
        if !path_exists(&bucket_dir)? {
            return Ok(false);
        }

        let record = self.read_bucket_record(bucket)?;
        validate_bucket_record(bucket, &self.bucket_metadata_path(bucket), &record)?;
        Ok(true)
    }

    fn delete_bucket(&self, bucket: &BucketName) -> Result<(), StorageError> {
        let _guard = lock_storage(&self.root, "delete_bucket")?;
        self.ensure_bucket(bucket)?;

        let objects_dir = self.bucket_dir(bucket).join(OBJECTS_DIR);
        if path_exists(&objects_dir)?
            && self.bucket_has_visible_committed_objects(bucket, &objects_dir)?
        {
            return Err(StorageError::BucketNotEmpty {
                bucket: bucket.clone(),
            });
        }

        remove_dir_all(&self.bucket_dir(bucket))
    }

    fn put_object(&self, request: PutObjectRequest) -> Result<StoredObjectMetadata, StorageError> {
        let _guard = lock_storage(&self.root, "put_object")?;
        self.ensure_bucket(&request.bucket)?;
        let encoded_key = encode_object_key(&request.key)?;
        let object_dir = self.object_dir(&request.bucket, &encoded_key);
        let object_state = object_path_state(&object_dir)?;

        let now = self.clock.now_utc();
        let metadata = StoredObjectMetadata {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            etag: etag_for_bytes(&request.bytes),
            content_length: request.bytes.len() as u64,
            content_type: Some(
                request
                    .content_type
                    .unwrap_or_else(|| DEFAULT_OBJECT_CONTENT_TYPE.to_owned()),
            ),
            last_modified: now,
            user_metadata: request.user_metadata,
        };
        let record = ObjectMetadataRecord::from_metadata(&metadata);

        let write_result = match object_state {
            ObjectPathState::Committed => {
                write_object_files_atomically(&object_dir, &request.bytes, &record)
            }
            ObjectPathState::Missing => {
                write_new_object_dir_atomically(&object_dir, &request.bytes, &record)
            }
        };

        if let Err(error) = write_result {
            if object_state == ObjectPathState::Missing {
                remove_new_object_dirs_best_effort(&object_dir);
            }
            return Err(error);
        }

        Ok(metadata)
    }

    fn get_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObject, StorageError> {
        let _guard = lock_storage(&self.root, "get_object")?;
        self.ensure_bucket(bucket)?;
        let encoded_key = encode_object_key(key)?;
        self.read_object(bucket, key, &encoded_key)
    }

    fn get_object_metadata(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObjectMetadata, StorageError> {
        let _guard = lock_storage(&self.root, "get_object_metadata")?;
        self.ensure_bucket(bucket)?;
        let encoded_key = encode_object_key(key)?;
        self.read_object_metadata(bucket, key, &encoded_key)
    }

    fn get_object_bytes(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<u8>, StorageError> {
        let _guard = lock_storage(&self.root, "get_object_bytes")?;
        self.ensure_bucket(bucket)?;
        let encoded_key = encode_object_key(key)?;
        self.read_object(bucket, key, &encoded_key)
            .map(|object| object.bytes)
    }

    fn list_objects(
        &self,
        bucket: &BucketName,
        options: ListObjectsOptions,
    ) -> Result<ObjectListing, StorageError> {
        let _guard = lock_storage(&self.root, "list_objects")?;
        self.ensure_bucket(bucket)?;
        if options
            .prefix
            .as_ref()
            .is_some_and(|prefix| !is_valid_s3_object_key_prefix(prefix.as_str()))
        {
            return Err(invalid_list_objects_prefix());
        }
        if options
            .delimiter
            .as_ref()
            .is_some_and(|delimiter| delimiter != "/")
        {
            return Err(invalid_list_objects_delimiter());
        }
        let resume_key = options
            .continuation_token
            .as_deref()
            .map(decode_continuation_token)
            .transpose()?;

        let objects_dir = self.bucket_dir(bucket).join(OBJECTS_DIR);
        if !path_exists(&objects_dir)? {
            return Ok(ObjectListing {
                bucket: bucket.clone(),
                entries: Vec::new(),
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                max_keys: options.max_keys,
                is_truncated: false,
                next_continuation_token: None,
            });
        }

        let mut objects = Vec::new();
        self.visit_visible_committed_objects(bucket, &objects_dir, |metadata| {
            if options
                .prefix
                .as_ref()
                .is_some_and(|prefix| !metadata.key.as_str().starts_with(prefix.as_str()))
            {
                return Ok(VisibleCommittedObjectVisit::Continue);
            }

            objects.push(metadata);
            Ok(VisibleCommittedObjectVisit::Continue)
        })?;

        objects.sort_by(|left, right| left.key.cmp(&right.key));
        let entries =
            visible_list_objects_entries(objects, options.prefix.as_ref(), &options.delimiter);
        let start_index = resume_key.as_ref().map_or(0, |resume_key| {
            entries.partition_point(|entry| entry.marker() < resume_key)
        });
        if options.max_keys == 0 {
            let start_index = resume_key.as_ref().map_or(0, |resume_key| {
                entries.partition_point(|entry| entry.marker() <= resume_key)
            });
            let next_continuation_token = entries
                .get(start_index)
                .map(|entry| encode_continuation_token(entry.marker()));
            return Ok(ObjectListing {
                bucket: bucket.clone(),
                entries: Vec::new(),
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                max_keys: options.max_keys,
                is_truncated: next_continuation_token.is_some(),
                next_continuation_token,
            });
        }

        let remaining_entries = &entries[start_index..];
        let page_count = remaining_entries.len().min(options.max_keys);
        let next_continuation_token = remaining_entries
            .get(page_count)
            .map(|entry| encode_continuation_token(entry.marker()));
        let page_entries = remaining_entries[..page_count].to_vec();
        let (page_objects, page_common_prefixes) = object_listing_parts(&page_entries);

        Ok(ObjectListing {
            bucket: bucket.clone(),
            entries: page_entries,
            objects: page_objects,
            common_prefixes: page_common_prefixes,
            max_keys: options.max_keys,
            is_truncated: next_continuation_token.is_some(),
            next_continuation_token,
        })
    }

    fn delete_object(&self, bucket: &BucketName, key: &ObjectKey) -> Result<(), StorageError> {
        let _guard = lock_storage(&self.root, "delete_object")?;
        self.ensure_bucket(bucket)?;
        let encoded_key = encode_object_key(key)?;
        self.read_object_metadata(bucket, key, &encoded_key)?;

        let object_dir = self.object_dir(bucket, &encoded_key);
        remove_dir_all(&object_dir)?;

        let shard_dir = self
            .bucket_dir(bucket)
            .join(OBJECTS_DIR)
            .join(encoded_key.shard());
        if path_exists(&shard_dir)? && !directory_has_entries(&shard_dir)? {
            remove_dir(&shard_dir)?;
        }

        Ok(())
    }
}

impl<C> FilesystemStorage<C> {
    fn bucket_root(&self) -> PathBuf {
        self.root.join(STORAGE_ROOT_DIR)
    }

    fn bucket_dir(&self, bucket: &BucketName) -> PathBuf {
        self.bucket_root()
            .join(encode_bucket_name(bucket).as_path_component())
    }

    fn bucket_metadata_path(&self, bucket: &BucketName) -> PathBuf {
        self.bucket_dir(bucket).join(BUCKET_METADATA_FILE)
    }

    fn object_dir(&self, bucket: &BucketName, encoded_key: &EncodedObjectKey) -> PathBuf {
        self.bucket_dir(bucket)
            .join(OBJECTS_DIR)
            .join(encoded_key.shard())
            .join(encoded_key.as_path_component())
    }

    fn ensure_bucket(&self, bucket: &BucketName) -> Result<(), StorageError> {
        if !path_exists(&self.bucket_dir(bucket))? {
            return Err(StorageError::NoSuchBucket {
                bucket: bucket.clone(),
            });
        }

        let record = self.read_bucket_record(bucket)?;
        validate_bucket_record(bucket, &self.bucket_metadata_path(bucket), &record)?;
        require_storage_directory(
            &self.bucket_dir(bucket).join(OBJECTS_DIR),
            "missing bucket objects directory",
        )?;

        Ok(())
    }

    fn read_bucket_record(&self, bucket: &BucketName) -> Result<BucketRecord, StorageError> {
        let metadata_path = self.bucket_metadata_path(bucket);
        if !path_exists(&metadata_path)? {
            return Err(corrupt_state(metadata_path, "missing bucket metadata"));
        }

        read_json(&metadata_path)
    }

    fn read_object_metadata(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        encoded_key: &EncodedObjectKey,
    ) -> Result<StoredObjectMetadata, StorageError> {
        self.read_object(bucket, key, encoded_key)
            .map(|object| object.metadata)
    }

    fn read_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        encoded_key: &EncodedObjectKey,
    ) -> Result<StoredObject, StorageError> {
        let object_dir = self.object_dir(bucket, encoded_key);
        match object_path_state(&object_dir)? {
            ObjectPathState::Committed => {}
            ObjectPathState::Missing => {
                return Err(StorageError::NoSuchKey {
                    bucket: bucket.clone(),
                    key: key.clone(),
                });
            }
        }

        let metadata_path = object_dir.join(OBJECT_METADATA_FILE);
        let content_path = object_dir.join(OBJECT_CONTENT_FILE);

        let metadata = object_metadata_from_path(&metadata_path)?;
        if metadata.bucket != *bucket || metadata.key != *key {
            return Err(corrupt_state(
                metadata_path,
                "object metadata does not match requested bucket/key",
            ));
        }
        let bytes = read_object_content_bytes(&content_path)?;
        validate_object_content_bytes(&content_path, &metadata, &bytes)?;

        Ok(StoredObject { metadata, bytes })
    }

    fn read_listed_object_metadata(
        &self,
        bucket: &BucketName,
        metadata_path: &Path,
    ) -> Result<StoredObjectMetadata, StorageError> {
        if !path_exists(metadata_path)? {
            return Err(corrupt_state(metadata_path, "missing object metadata"));
        }

        let metadata = object_metadata_from_path(metadata_path)?;
        if metadata.bucket != *bucket {
            return Err(corrupt_state(
                metadata_path.to_path_buf(),
                "object metadata bucket does not match containing bucket",
            ));
        }

        let encoded_key = encode_object_key(&metadata.key).map_err(|_| {
            corrupt_state(
                metadata_path.to_path_buf(),
                "object metadata contains an invalid key",
            )
        })?;
        let expected_path = self
            .object_dir(bucket, &encoded_key)
            .join(OBJECT_METADATA_FILE);
        if expected_path != metadata_path {
            return Err(corrupt_state(
                metadata_path.to_path_buf(),
                "object metadata is stored under the wrong hashed path",
            ));
        }

        let content_path = self
            .object_dir(bucket, &encoded_key)
            .join(OBJECT_CONTENT_FILE);
        if !path_exists(&content_path)? {
            return Err(corrupt_state(content_path, "missing object content"));
        }
        validate_object_content(&content_path, &metadata)?;

        Ok(metadata)
    }

    fn bucket_has_visible_committed_objects(
        &self,
        bucket: &BucketName,
        objects_dir: &Path,
    ) -> Result<bool, StorageError> {
        let mut found_object = false;
        self.visit_visible_committed_objects(bucket, objects_dir, |_| {
            found_object = true;
            Ok(VisibleCommittedObjectVisit::Stop)
        })?;

        Ok(found_object)
    }

    fn visit_visible_committed_objects<F>(
        &self,
        bucket: &BucketName,
        objects_dir: &Path,
        mut visit: F,
    ) -> Result<(), StorageError>
    where
        F: FnMut(StoredObjectMetadata) -> Result<VisibleCommittedObjectVisit, StorageError>,
    {
        for shard_entry in sorted_directory_entries(objects_dir)? {
            let Some(shard_path) = visible_storage_directory_path(
                &shard_entry,
                "visible object shard path exists but is not a directory",
            )?
            else {
                continue;
            };

            if self.visit_visible_committed_object_shard(bucket, &shard_path, &mut visit)?
                == VisibleCommittedObjectVisit::Stop
            {
                return Ok(());
            }
        }

        Ok(())
    }

    fn visit_visible_committed_object_shard<F>(
        &self,
        bucket: &BucketName,
        shard_path: &Path,
        visit: &mut F,
    ) -> Result<VisibleCommittedObjectVisit, StorageError>
    where
        F: FnMut(StoredObjectMetadata) -> Result<VisibleCommittedObjectVisit, StorageError>,
    {
        for object_entry in sorted_directory_entries(shard_path)? {
            let Some(object_path) = visible_storage_directory_path(
                &object_entry,
                "visible object path exists but is not a directory",
            )?
            else {
                continue;
            };

            let metadata =
                self.read_listed_object_metadata(bucket, &object_path.join(OBJECT_METADATA_FILE))?;
            if visit(metadata)? == VisibleCommittedObjectVisit::Stop {
                return Ok(VisibleCommittedObjectVisit::Stop);
            }
        }

        Ok(VisibleCommittedObjectVisit::Continue)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum VisibleCommittedObjectVisit {
    Continue,
    Stop,
}

impl ObjectListingEntry {
    fn marker(&self) -> &ObjectKey {
        match self {
            Self::Object(metadata) => &metadata.key,
            Self::CommonPrefix(prefix) => prefix,
        }
    }
}

fn visible_list_objects_entries(
    objects: Vec<StoredObjectMetadata>,
    prefix: Option<&ObjectKey>,
    delimiter: &Option<String>,
) -> Vec<ObjectListingEntry> {
    if delimiter.is_none() {
        return objects
            .into_iter()
            .map(ObjectListingEntry::Object)
            .collect();
    }

    let prefix = prefix.map_or("", ObjectKey::as_str);
    let mut entries = BTreeMap::new();
    for object in objects {
        let remaining_key = object
            .key
            .as_str()
            .strip_prefix(prefix)
            .expect("objects were filtered by prefix before delimiter grouping");
        if let Some(delimiter_index) = remaining_key.find('/') {
            let common_prefix =
                ObjectKey::new(format!("{}{}", prefix, &remaining_key[..=delimiter_index]));
            entries
                .entry(common_prefix.clone())
                .or_insert(ObjectListingEntry::CommonPrefix(common_prefix));
        } else {
            entries.insert(object.key.clone(), ObjectListingEntry::Object(object));
        }
    }

    entries.into_values().collect()
}

fn object_listing_parts(
    entries: &[ObjectListingEntry],
) -> (Vec<StoredObjectMetadata>, Vec<ObjectKey>) {
    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    for entry in entries {
        match entry {
            ObjectListingEntry::Object(metadata) => objects.push(metadata.clone()),
            ObjectListingEntry::CommonPrefix(prefix) => common_prefixes.push(prefix.clone()),
        }
    }

    (objects, common_prefixes)
}

#[derive(Debug, Deserialize, Serialize)]
struct BucketRecord {
    bucket: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ObjectMetadataRecord {
    schema_version: u32,
    bucket: String,
    key: String,
    etag: String,
    content_length: u64,
    content_type: Option<String>,
    last_modified_unix_seconds: i64,
    last_modified_nanoseconds: u32,
    user_metadata: BTreeMap<String, String>,
}

impl ObjectMetadataRecord {
    fn from_metadata(metadata: &StoredObjectMetadata) -> Self {
        Self {
            schema_version: OBJECT_METADATA_SCHEMA_VERSION,
            bucket: metadata.bucket.as_str().to_owned(),
            key: metadata.key.as_str().to_owned(),
            etag: metadata.etag.clone(),
            content_length: metadata.content_length,
            content_type: metadata.content_type.clone(),
            last_modified_unix_seconds: metadata.last_modified.unix_timestamp(),
            last_modified_nanoseconds: metadata.last_modified.nanosecond(),
            user_metadata: metadata.user_metadata.clone(),
        }
    }

    fn into_metadata(self, path: &Path) -> Result<StoredObjectMetadata, StorageError> {
        if self.schema_version != OBJECT_METADATA_SCHEMA_VERSION {
            return Err(corrupt_state(
                path.to_path_buf(),
                "unsupported object metadata schema version",
            ));
        }

        let base_time = OffsetDateTime::from_unix_timestamp(self.last_modified_unix_seconds)
            .map_err(|error| {
                corrupt_state(
                    path.to_path_buf(),
                    format!("invalid last_modified_unix_seconds: {error}"),
                )
            })?;
        let last_modified = base_time
            .replace_nanosecond(self.last_modified_nanoseconds)
            .map_err(|error| {
                corrupt_state(
                    path.to_path_buf(),
                    format!("invalid last_modified_nanoseconds: {error}"),
                )
            })?;

        Ok(StoredObjectMetadata {
            bucket: BucketName::new(self.bucket),
            key: ObjectKey::new(self.key),
            etag: self.etag,
            content_length: self.content_length,
            content_type: self.content_type,
            last_modified,
            user_metadata: self.user_metadata,
        })
    }
}

fn object_metadata_from_path(path: &Path) -> Result<StoredObjectMetadata, StorageError> {
    let record: ObjectMetadataRecord = read_json(path)?;
    record.into_metadata(path)
}

fn etag_for_bytes(bytes: &[u8]) -> String {
    let digest = Md5::digest(bytes);
    format!("\"{}\"", lower_hex(&digest))
}

fn lower_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn encode_continuation_token(key: &ObjectKey) -> String {
    format!(
        "{CONTINUATION_TOKEN_PREFIX}{}",
        lower_hex(key.as_str().as_bytes())
    )
}

fn decode_continuation_token(token: &str) -> Result<ObjectKey, StorageError> {
    let Some(hex_key) = token.strip_prefix(CONTINUATION_TOKEN_PREFIX) else {
        return Err(invalid_continuation_token());
    };
    if hex_key.is_empty() || hex_key.len() % 2 != 0 || !hex_key.is_ascii() {
        return Err(invalid_continuation_token());
    }
    if !hex_key
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_continuation_token());
    }

    let mut key_bytes = Vec::with_capacity(hex_key.len() / 2);
    for pair in hex_key.as_bytes().chunks_exact(2) {
        key_bytes.push(hex_pair_to_byte(pair)?);
    }

    let key = String::from_utf8(key_bytes).map_err(|_| invalid_continuation_token())?;
    if key.is_empty() {
        return Err(invalid_continuation_token());
    }

    let key = ObjectKey::new(key);
    encode_object_key(&key).map_err(|_| invalid_continuation_token())?;
    Ok(key)
}

fn hex_pair_to_byte(pair: &[u8]) -> Result<u8, StorageError> {
    Ok(hex_nibble(pair[0])? << 4 | hex_nibble(pair[1])?)
}

fn hex_nibble(byte: u8) -> Result<u8, StorageError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(invalid_continuation_token()),
    }
}

fn invalid_continuation_token() -> StorageError {
    StorageError::InvalidArgument {
        message: "malformed ListObjectsV2 continuation token".to_owned(),
    }
}

fn invalid_list_objects_prefix() -> StorageError {
    StorageError::InvalidArgument {
        message: "invalid ListObjectsV2 prefix".to_owned(),
    }
}

fn invalid_list_objects_delimiter() -> StorageError {
    StorageError::InvalidArgument {
        message: "unsupported ListObjectsV2 delimiter".to_owned(),
    }
}

fn sorted_directory_entries(path: &Path) -> Result<Vec<fs::DirEntry>, StorageError> {
    require_storage_directory(path, "missing storage directory")?;
    let mut entries = fs::read_dir(path)
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .collect::<Result<Vec<_>, io::Error>>()
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

fn is_hidden_storage_entry(entry: &fs::DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .is_some_and(|file_name| file_name.starts_with('.'))
}

fn visible_storage_directory_path(
    entry: &fs::DirEntry,
    not_directory_message: &'static str,
) -> Result<Option<PathBuf>, StorageError> {
    if is_hidden_storage_entry(entry) {
        return Ok(None);
    }

    let path = entry.path();
    let Some(metadata) = storage_path_metadata(&path)? else {
        return Ok(None);
    };
    if !metadata.is_dir() {
        return Err(corrupt_state(path, not_directory_message));
    }

    Ok(Some(path))
}

fn directory_has_entries(path: &Path) -> Result<bool, StorageError> {
    Ok(sorted_directory_entries(path)?.into_iter().next().is_some())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, StorageError> {
    let bytes = read_bytes(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|source| corrupt_state(path.to_path_buf(), format!("invalid json: {source}")))
}

fn read_bytes(path: &Path) -> Result<Vec<u8>, StorageError> {
    require_storage_file(path, "missing storage file")?;
    fs::read(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_object_content_bytes(path: &Path) -> Result<Vec<u8>, StorageError> {
    require_storage_file(path, "missing object content")?;
    fs::read(path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            corrupt_state(path.to_path_buf(), "missing object content")
        } else {
            StorageError::Io {
                path: path.to_path_buf(),
                source,
            }
        }
    })
}

fn validate_object_content(
    content_path: &Path,
    metadata: &StoredObjectMetadata,
) -> Result<(), StorageError> {
    let bytes = read_object_content_bytes(content_path)?;
    validate_object_content_bytes(content_path, metadata, &bytes)
}

fn validate_object_content_bytes(
    content_path: &Path,
    metadata: &StoredObjectMetadata,
    bytes: &[u8],
) -> Result<(), StorageError> {
    if bytes.len() as u64 != metadata.content_length {
        return Err(corrupt_state(
            content_path.to_path_buf(),
            "object content length does not match metadata",
        ));
    }

    let actual_etag = etag_for_bytes(bytes);
    if actual_etag != metadata.etag {
        return Err(corrupt_state(
            content_path.to_path_buf(),
            "object content ETag does not match metadata",
        ));
    }

    Ok(())
}

fn write_json_atomically<T: Serialize>(path: &Path, value: &T) -> Result<(), StorageError> {
    let bytes = json_bytes(path, value)?;
    write_bytes_atomically(path, &bytes)
}

fn write_new_bucket_state_atomically(
    bucket_dir: &Path,
    record: &BucketRecord,
    bucket: &BucketName,
) -> Result<(), StorageError> {
    let temporary_bucket_dir = create_temporary_sibling_dir(bucket_dir)?;

    if let Err(error) = write_new_bucket_state(&temporary_bucket_dir, record) {
        remove_path_best_effort(&temporary_bucket_dir);
        return Err(error);
    }

    if let Err(error) = rename_path(&temporary_bucket_dir, bucket_dir) {
        remove_path_best_effort(&temporary_bucket_dir);
        if path_exists(bucket_dir)? {
            return Err(StorageError::BucketAlreadyExists {
                bucket: bucket.clone(),
            });
        }

        return Err(error);
    }

    Ok(())
}

fn write_new_bucket_state(bucket_dir: &Path, record: &BucketRecord) -> Result<(), StorageError> {
    if let Err(error) = write_json_atomically(&bucket_dir.join(BUCKET_METADATA_FILE), record) {
        remove_path_best_effort(bucket_dir);
        return Err(error);
    }

    if let Err(error) = create_dir_all(&bucket_dir.join(OBJECTS_DIR)) {
        remove_path_best_effort(bucket_dir);
        return Err(error);
    }
    sync_parent_dir_best_effort(&bucket_dir.join(OBJECTS_DIR));

    Ok(())
}

fn write_new_object_dir_atomically(
    object_dir: &Path,
    content: &[u8],
    metadata: &ObjectMetadataRecord,
) -> Result<(), StorageError> {
    let Some(shard_dir) = object_dir.parent() else {
        return Err(corrupt_state(
            object_dir.to_path_buf(),
            "object path has no shard directory",
        ));
    };

    create_dir_all(shard_dir)?;
    let temporary_object_dir = create_temporary_sibling_dir(object_dir)?;

    if let Err(error) = write_new_object_state(&temporary_object_dir, content, metadata) {
        remove_path_best_effort(&temporary_object_dir);
        remove_empty_dir_best_effort(shard_dir);
        return Err(error);
    }

    if let Err(error) = rename_path(&temporary_object_dir, object_dir) {
        remove_path_best_effort(&temporary_object_dir);
        remove_empty_dir_best_effort(shard_dir);
        return Err(error);
    }

    Ok(())
}

fn write_new_object_state(
    object_dir: &Path,
    content: &[u8],
    metadata: &ObjectMetadataRecord,
) -> Result<(), StorageError> {
    #[cfg(test)]
    if FAIL_NEXT_NEW_OBJECT_STATE_WRITE.replace(false) {
        return Err(StorageError::Io {
            path: object_dir.join(OBJECT_CONTENT_FILE),
            source: io::Error::other("forced new object write failure"),
        });
    }

    write_bytes_synced(&object_dir.join(OBJECT_CONTENT_FILE), content)?;
    let metadata_path = object_dir.join(OBJECT_METADATA_FILE);
    write_bytes_synced(&metadata_path, &json_bytes(&metadata_path, metadata)?)?;
    sync_parent_dir_best_effort(&metadata_path);
    Ok(())
}

#[cfg(test)]
fn fail_next_new_object_state_write_for_test() {
    FAIL_NEXT_NEW_OBJECT_STATE_WRITE.set(true);
}

fn write_object_files_atomically(
    object_dir: &Path,
    content: &[u8],
    metadata: &ObjectMetadataRecord,
) -> Result<(), StorageError> {
    let content_path = object_dir.join(OBJECT_CONTENT_FILE);
    let metadata_path = object_dir.join(OBJECT_METADATA_FILE);
    let content_temporary_path = write_temporary_sibling(&content_path, content)?;
    let metadata_bytes = match json_bytes(&metadata_path, metadata) {
        Ok(bytes) => bytes,
        Err(error) => {
            remove_path_best_effort(&content_temporary_path);
            return Err(error);
        }
    };
    let metadata_temporary_path = match write_temporary_sibling(&metadata_path, &metadata_bytes) {
        Ok(path) => path,
        Err(error) => {
            remove_path_best_effort(&content_temporary_path);
            return Err(error);
        }
    };

    let result = commit_object_files(
        &content_path,
        &content_temporary_path,
        &metadata_path,
        &metadata_temporary_path,
    );
    if result.is_err() {
        remove_path_best_effort(&content_temporary_path);
        remove_path_best_effort(&metadata_temporary_path);
    }
    result
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ObjectPathState {
    Missing,
    Committed,
}

fn object_path_state(object_dir: &Path) -> Result<ObjectPathState, StorageError> {
    let Some(object_dir_metadata) = storage_path_metadata(object_dir)? else {
        return Ok(ObjectPathState::Missing);
    };
    if !object_dir_metadata.is_dir() {
        return Err(corrupt_state(
            object_dir.to_path_buf(),
            "object path exists but is not a directory",
        ));
    }

    let content_path = object_dir.join(OBJECT_CONTENT_FILE);
    let metadata_path = object_dir.join(OBJECT_METADATA_FILE);
    let content_metadata = storage_path_metadata(&content_path)?;
    let object_metadata = storage_path_metadata(&metadata_path)?;
    if let (Some(content_metadata), Some(object_metadata)) = (content_metadata, object_metadata) {
        if !content_metadata.is_file() {
            return Err(corrupt_state(
                content_path,
                "object content path exists but is not a file",
            ));
        }

        if !object_metadata.is_file() {
            return Err(corrupt_state(
                metadata_path,
                "object metadata path exists but is not a file",
            ));
        }

        return Ok(ObjectPathState::Committed);
    }

    Err(corrupt_state(
        object_dir.to_path_buf(),
        "object directory is missing committed content or metadata",
    ))
}

fn lock_storage(root: &Path, operation: &str) -> Result<MutexGuard<'static, ()>, StorageError> {
    validate_storage_root(root)?;
    STORAGE_LOCK.lock().map_err(|_| {
        corrupt_state(
            root.to_path_buf(),
            format!("{operation} storage lock poisoned"),
        )
    })
}

fn validate_storage_root(root: &Path) -> Result<(), StorageError> {
    reject_existing_reparse_point_ancestors(root)?;

    let Some(metadata) = storage_path_metadata(root)? else {
        return Ok(());
    };

    if !metadata.is_dir() {
        return Err(corrupt_state(
            root.to_path_buf(),
            "storage root exists but is not a directory",
        ));
    }

    Ok(())
}

fn reject_existing_reparse_point_ancestors(path: &Path) -> Result<(), StorageError> {
    for ancestor in path.ancestors() {
        if ancestor.as_os_str().is_empty() {
            continue;
        }

        let metadata = match fs::symlink_metadata(ancestor) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(StorageError::Io {
                    path: ancestor.to_path_buf(),
                    source,
                });
            }
        };

        reject_reparse_point(ancestor, &metadata)?;
    }

    Ok(())
}

fn json_bytes<T: Serialize>(path: &Path, value: &T) -> Result<Vec<u8>, StorageError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|source| {
        corrupt_state(
            path.to_path_buf(),
            format!("could not serialize json: {source}"),
        )
    })?;
    Ok(bytes)
}

fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let temporary_path = write_temporary_sibling(path, bytes)?;
    if let Err(error) = replace_file_with_temporary(path, &temporary_path) {
        remove_path_best_effort(&temporary_path);
        return Err(error);
    }

    Ok(())
}

fn write_bytes_synced(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    file.write_all(bytes).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn write_temporary_sibling(path: &Path, bytes: &[u8]) -> Result<PathBuf, StorageError> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }

    let (temporary_path, mut file) = create_new_temporary_file(path, SIBLING_PATH_RETRY_LIMIT)?;
    file.write_all(bytes).map_err(|source| StorageError::Io {
        path: temporary_path.clone(),
        source,
    })?;
    file.sync_all().map_err(|source| StorageError::Io {
        path: temporary_path.clone(),
        source,
    })?;
    Ok(temporary_path)
}

fn commit_object_files(
    content_path: &Path,
    content_temporary_path: &Path,
    metadata_path: &Path,
    metadata_temporary_path: &Path,
) -> Result<(), StorageError> {
    let metadata_backup = move_existing_path_to_backup(metadata_path)?;
    let content_backup = match move_existing_path_to_backup(content_path) {
        Ok(backup) => backup,
        Err(error) => {
            restore_path_backup(metadata_path, metadata_backup);
            return Err(error);
        }
    };

    if let Err(error) = rename_path(content_temporary_path, content_path) {
        restore_path_backup(content_path, content_backup);
        restore_path_backup(metadata_path, metadata_backup);
        return Err(error);
    }

    if let Err(error) = rename_path(metadata_temporary_path, metadata_path) {
        remove_path_best_effort(content_path);
        restore_path_backup(content_path, content_backup);
        restore_path_backup(metadata_path, metadata_backup);
        return Err(error);
    }

    discard_path_backup(content_backup);
    discard_path_backup(metadata_backup);
    Ok(())
}

fn replace_file_with_temporary(path: &Path, temporary_path: &Path) -> Result<(), StorageError> {
    let backup = move_existing_path_to_backup(path)?;
    if let Err(error) = rename_path(temporary_path, path) {
        restore_path_backup(path, backup);
        return Err(error);
    }

    discard_path_backup(backup);
    Ok(())
}

fn move_existing_path_to_backup(path: &Path) -> Result<Option<PathBuf>, StorageError> {
    if !path_exists(path)? {
        return Ok(None);
    }

    move_existing_path_to_unused_backup(path, SIBLING_PATH_RETRY_LIMIT)
}

fn restore_path_backup(path: &Path, backup_path: Option<PathBuf>) {
    if let Some(backup_path) = backup_path {
        remove_path_best_effort(path);
        let _ = fs::rename(backup_path, path);
    }
}

fn discard_path_backup(backup_path: Option<PathBuf>) {
    if let Some(backup_path) = backup_path {
        remove_path_best_effort(&backup_path);
    }
}

fn rename_path(from: &Path, to: &Path) -> Result<(), StorageError> {
    let _ = storage_path_metadata(from)?;
    let _ = storage_path_metadata(to)?;
    fs::rename(from, to).map_err(|source| StorageError::Io {
        path: to.to_path_buf(),
        source,
    })?;
    sync_parent_dir_best_effort(to);
    Ok(())
}

fn sync_parent_dir_best_effort(path: &Path) {
    let Some(parent) = path.parent() else {
        return;
    };

    if let Ok(directory) = File::open(parent) {
        let _ = directory.sync_all();
    }
}

fn remove_path_best_effort(path: &Path) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };

    if is_reparse_point(&metadata) {
        let _ = fs::remove_file(path).or_else(|_| fs::remove_dir(path));
        return;
    }

    if metadata.is_dir() {
        let _ = fs::remove_dir_all(path);
        return;
    }

    let _ = fs::remove_file(path);
}

fn remove_new_object_dirs_best_effort(object_dir: &Path) {
    remove_path_best_effort(object_dir);
    if let Some(shard_dir) = object_dir.parent() {
        remove_empty_dir_best_effort(shard_dir);
    }
}

fn remove_empty_dir_best_effort(path: &Path) {
    let _ = fs::remove_dir(path);
}

fn create_temporary_sibling_dir(path: &Path) -> Result<PathBuf, StorageError> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }

    for attempt in 0..SIBLING_PATH_RETRY_LIMIT {
        let candidate = sibling_path(path, "tmp", attempt);
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(StorageError::Io {
                    path: candidate,
                    source,
                });
            }
        }
    }

    Err(sibling_collision_error(path, "temporary"))
}

fn create_new_temporary_file(path: &Path, attempts: u64) -> Result<(PathBuf, File), StorageError> {
    for attempt in 0..attempts {
        let candidate = sibling_path(path, "tmp", attempt);
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(StorageError::Io {
                    path: candidate,
                    source,
                });
            }
        }
    }

    Err(sibling_collision_error(path, "temporary"))
}

fn move_existing_path_to_unused_backup(
    path: &Path,
    attempts: u64,
) -> Result<Option<PathBuf>, StorageError> {
    for attempt in 0..attempts {
        let candidate = sibling_path(path, "bak", attempt);
        if path_exists(&candidate)? {
            continue;
        }

        match fs::rename(path, &candidate) {
            Ok(()) => {
                sync_parent_dir_best_effort(&candidate);
                return Ok(Some(candidate));
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(StorageError::Io {
                    path: candidate,
                    source,
                });
            }
        }
    }

    Err(sibling_collision_error(path, "backup"))
}

fn sibling_path(path: &Path, purpose: &str, attempt: u64) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("storage-file");
    if attempt == 0 {
        return path.with_file_name(format!(".{file_name}.{purpose}-{}", std::process::id(),));
    }

    let counter = TEMPORARY_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    path.with_file_name(format!(
        ".{file_name}.{purpose}-{}-{counter}",
        std::process::id(),
    ))
}

fn sibling_collision_error(path: &Path, purpose: &str) -> StorageError {
    StorageError::Io {
        path: path.to_path_buf(),
        source: io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("could not create unused {purpose} sibling path"),
        ),
    }
}

fn validate_bucket_record(
    bucket: &BucketName,
    metadata_path: &Path,
    record: &BucketRecord,
) -> Result<(), StorageError> {
    if !is_valid_s3_bucket_name(&record.bucket) {
        return Err(corrupt_state(
            metadata_path.to_path_buf(),
            "bucket metadata contains an invalid S3 bucket name",
        ));
    }

    if record.bucket != bucket.as_str() {
        return Err(corrupt_state(
            metadata_path.to_path_buf(),
            "bucket metadata does not match requested bucket",
        ));
    }

    Ok(())
}

fn validate_listed_bucket_record(
    bucket_dir: &Path,
    metadata_path: &Path,
    record: &BucketRecord,
) -> Result<(), StorageError> {
    if !is_valid_s3_bucket_name(&record.bucket) {
        return Err(corrupt_state(
            metadata_path.to_path_buf(),
            "bucket metadata contains an invalid S3 bucket name",
        ));
    }

    let bucket = BucketName::new(record.bucket.clone());
    let encoded_bucket = encode_bucket_name(&bucket);
    if bucket_dir.file_name() != Some(OsStr::new(encoded_bucket.as_path_component())) {
        return Err(corrupt_state(
            metadata_path.to_path_buf(),
            "bucket metadata does not match encoded bucket path",
        ));
    }

    Ok(())
}

fn storage_path_metadata(path: &Path) -> Result<Option<fs::Metadata>, StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            reject_reparse_point(path, &metadata)?;
            Ok(Some(metadata))
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(StorageError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn require_storage_directory(path: &Path, missing_message: &str) -> Result<(), StorageError> {
    let Some(metadata) = storage_path_metadata(path)? else {
        return Err(corrupt_state(path.to_path_buf(), missing_message));
    };

    if !metadata.is_dir() {
        return Err(corrupt_state(
            path.to_path_buf(),
            "storage path exists but is not a directory",
        ));
    }

    Ok(())
}

fn require_storage_file(path: &Path, missing_message: &str) -> Result<(), StorageError> {
    let Some(metadata) = storage_path_metadata(path)? else {
        return Err(corrupt_state(path.to_path_buf(), missing_message));
    };

    if !metadata.is_file() {
        return Err(corrupt_state(
            path.to_path_buf(),
            "storage path exists but is not a file",
        ));
    }

    Ok(())
}

fn reject_reparse_point(path: &Path, metadata: &fs::Metadata) -> Result<(), StorageError> {
    if is_reparse_point(metadata) {
        return Err(corrupt_state(
            path.to_path_buf(),
            "storage path is a symlink or reparse point",
        ));
    }

    Ok(())
}

fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink() || has_windows_reparse_point_attribute(metadata)
}

#[cfg(windows)]
fn has_windows_reparse_point_attribute(metadata: &fs::Metadata) -> bool {
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn has_windows_reparse_point_attribute(_metadata: &fs::Metadata) -> bool {
    false
}

fn path_exists(path: &Path) -> Result<bool, StorageError> {
    Ok(storage_path_metadata(path)?.is_some())
}

fn create_dir_all(path: &Path) -> Result<(), StorageError> {
    fs::create_dir_all(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    require_storage_directory(path, "missing storage directory")
}

fn remove_dir(path: &Path) -> Result<(), StorageError> {
    fs::remove_dir(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn remove_dir_all(path: &Path) -> Result<(), StorageError> {
    require_storage_directory(path, "missing storage directory")?;
    fs::remove_dir_all(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn corrupt_state(path: impl Into<PathBuf>, message: impl Into<String>) -> StorageError {
    StorageError::CorruptState {
        path: path.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        commit_object_files, create_temporary_sibling_dir,
        fail_next_new_object_state_write_for_test, move_existing_path_to_backup,
        replace_file_with_temporary, restore_path_backup, sibling_path, storage_path_metadata,
        write_new_bucket_state, write_temporary_sibling, BucketRecord, FilesystemStorage,
        StorageClock, StorageError, OBJECTS_DIR,
    };
    use crate::s3::bucket::BucketName;
    use crate::s3::object::ObjectKey;
    use crate::storage::key::encode_object_key;
    use crate::storage::{PutObjectRequest, Storage};
    use std::collections::BTreeMap;
    use std::fmt::{Display, Formatter};
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

    #[test]
    fn new_stores_root_without_normalizing() {
        let storage = FilesystemStorage::new("./s3lab-data");

        assert_eq!(storage.root(), Path::new("./s3lab-data"));
    }

    #[test]
    fn storage_can_be_cloned_without_changing_values() {
        let storage = FilesystemStorage::new("data");

        assert_eq!(storage.clone(), storage);
    }

    #[test]
    fn with_clock_stores_test_clock() {
        let storage = FilesystemStorage::with_clock("data", FixedClock(fixed_time()));

        assert_eq!(storage.clock.now_utc(), fixed_time());
    }

    #[test]
    fn object_commit_rolls_back_when_metadata_commit_fails() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let content_path = temp_dir.path().join("content.bin");
        let metadata_path = temp_dir.path().join("metadata.json");
        let content_temporary_path = temp_dir.path().join("content.tmp");
        let metadata_temporary_path = temp_dir.path().join("missing-metadata.tmp");
        fs::write(&content_path, b"old-content").expect("write old content");
        fs::write(&metadata_path, b"old-metadata").expect("write old metadata");
        fs::write(&content_temporary_path, b"new-content").expect("write new content");

        let error = commit_object_files(
            &content_path,
            &content_temporary_path,
            &metadata_path,
            &metadata_temporary_path,
        )
        .expect_err("missing metadata temp fails commit");

        assert!(matches!(error, StorageError::Io { .. }));
        assert_eq!(
            fs::read(&content_path).expect("read restored content"),
            b"old-content"
        );
        assert_eq!(
            fs::read(&metadata_path).expect("read restored metadata"),
            b"old-metadata"
        );
    }

    #[test]
    fn object_commit_restores_backups_when_content_temp_is_missing() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let content_path = temp_dir.path().join("content.bin");
        let metadata_path = temp_dir.path().join("metadata.json");
        let content_temporary_path = temp_dir.path().join("missing-content.tmp");
        let metadata_temporary_path = temp_dir.path().join("metadata.tmp");
        fs::write(&content_path, b"old-content").expect("write old content");
        fs::write(&metadata_path, b"old-metadata").expect("write old metadata");
        fs::write(&metadata_temporary_path, b"new-metadata").expect("write new metadata");

        let error = commit_object_files(
            &content_path,
            &content_temporary_path,
            &metadata_path,
            &metadata_temporary_path,
        )
        .expect_err("missing content temp fails commit");

        assert!(matches!(error, StorageError::Io { .. }));
        assert_eq!(
            fs::read(&content_path).expect("read restored content"),
            b"old-content"
        );
        assert_eq!(
            fs::read(&metadata_path).expect("read restored metadata"),
            b"old-metadata"
        );
        assert_eq!(
            fs::read(&metadata_temporary_path).expect("read uncommitted metadata temp"),
            b"new-metadata"
        );
    }

    #[test]
    fn replace_file_with_temporary_restores_backup_when_rename_fails() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let target_path = temp_dir.path().join("metadata.json");
        let temporary_path = temp_dir.path().join("missing-metadata.tmp");
        fs::write(&target_path, b"current").expect("write current target");

        let error = replace_file_with_temporary(&target_path, &temporary_path)
            .expect_err("file replacement fails with missing temporary file");

        assert!(matches!(error, StorageError::Io { .. }));
        assert_eq!(
            fs::read(&target_path).expect("read restored target"),
            b"current"
        );
    }

    #[test]
    fn write_temporary_sibling_retries_stale_pid_collision() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let target_path = temp_dir.path().join("metadata.json");
        let stale_temporary_path = sibling_path(&target_path, "tmp", 0);
        fs::write(&stale_temporary_path, b"stale").expect("write stale temporary sibling");

        let temporary_path =
            write_temporary_sibling(&target_path, b"new").expect("write temporary sibling");

        assert_ne!(temporary_path, stale_temporary_path);
        assert_eq!(
            fs::read(&temporary_path).expect("read retried temporary"),
            b"new"
        );
        assert_eq!(
            fs::read(&stale_temporary_path).expect("stale temporary remains untouched"),
            b"stale"
        );
    }

    #[test]
    fn create_temporary_sibling_dir_retries_stale_pid_collision() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let target_path = temp_dir.path().join("object-dir");
        let stale_temporary_path = sibling_path(&target_path, "tmp", 0);
        fs::create_dir(&stale_temporary_path).expect("create stale temporary directory");

        let temporary_path =
            create_temporary_sibling_dir(&target_path).expect("create temporary sibling dir");

        assert_ne!(temporary_path, stale_temporary_path);
        assert!(temporary_path.is_dir());
        assert!(stale_temporary_path.is_dir());
    }

    #[test]
    fn move_existing_path_to_backup_retries_stale_pid_collision() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let target_path = temp_dir.path().join("content.bin");
        let stale_backup_path = sibling_path(&target_path, "bak", 0);
        fs::write(&target_path, b"current").expect("write current target");
        fs::write(&stale_backup_path, b"stale").expect("write stale backup sibling");

        let backup_path = move_existing_path_to_backup(&target_path)
            .expect("move target to backup")
            .expect("target existed");

        assert_ne!(backup_path, stale_backup_path);
        assert!(!target_path.exists());
        assert_eq!(
            fs::read(&backup_path).expect("read retried backup"),
            b"current"
        );
        assert_eq!(
            fs::read(&stale_backup_path).expect("stale backup remains untouched"),
            b"stale"
        );
    }

    #[test]
    fn restore_path_backup_without_backup_leaves_existing_path_untouched() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let path = temp_dir.path().join("content.bin");
        fs::write(&path, b"current").expect("write current path");

        restore_path_backup(&path, None);

        assert_eq!(fs::read(path).expect("read current path"), b"current");
    }

    #[test]
    fn storage_path_metadata_accepts_regular_storage_files() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let path = temp_dir.path().join("content.bin");
        fs::write(&path, b"content").expect("write regular file");

        let metadata = storage_path_metadata(&path)
            .expect("regular file metadata succeeds")
            .expect("regular file exists");

        assert!(metadata.is_file());
    }

    #[test]
    fn storage_path_metadata_rejects_symlinks_when_platform_allows_them() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let target = temp_dir.path().join("target.bin");
        let link = temp_dir.path().join("content.bin");
        fs::write(&target, b"target").expect("write symlink target");
        if let Err(skip) = create_file_symlink_or_skip(&target, &link) {
            println!("{skip}");
            return;
        }

        let error = storage_path_metadata(&link).expect_err("symlink metadata is corrupt state");

        assert!(matches!(error, StorageError::CorruptState { .. }));
    }

    #[test]
    fn new_bucket_state_rolls_back_bucket_directory_when_objects_dir_create_fails() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let bucket_dir = temp_dir.path().join("example-bucket");
        fs::create_dir(&bucket_dir).expect("create bucket dir");
        fs::write(bucket_dir.join(OBJECTS_DIR), b"not a directory")
            .expect("create blocking objects file");

        let error = write_new_bucket_state(
            &bucket_dir,
            &BucketRecord {
                bucket: "example-bucket".to_owned(),
            },
        )
        .expect_err("blocked objects dir fails bucket state write");

        assert!(matches!(error, StorageError::Io { .. }));
        assert!(!bucket_dir.exists());
    }

    #[test]
    fn put_object_removes_new_object_directory_when_first_temp_write_fails() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let storage =
            FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");
        storage.create_bucket(&bucket).expect("create bucket");

        let encoded_key = encode_object_key(&key).expect("valid key");
        let object_dir = storage.object_dir(&bucket, &encoded_key);
        fail_next_new_object_state_write_for_test();

        let error = storage
            .put_object(PutObjectRequest {
                bucket,
                key,
                bytes: b"body".to_vec(),
                content_type: None,
                user_metadata: BTreeMap::new(),
            })
            .expect_err("blocked temp path fails put");

        assert!(matches!(error, StorageError::Io { .. }));
        assert!(!object_dir.exists());
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct FixedClock(OffsetDateTime);

    impl StorageClock for FixedClock {
        fn now_utc(&self) -> OffsetDateTime {
            self.0
        }
    }

    fn fixed_time() -> OffsetDateTime {
        PrimitiveDateTime::new(
            Date::from_calendar_date(2026, Month::May, 10).expect("valid test date"),
            Time::MIDNIGHT,
        )
        .assume_utc()
    }

    fn create_file_symlink_or_skip(target: &Path, link: &Path) -> Result<(), SymlinkTestSkipped> {
        try_create_file_symlink(target, link).map_err(|source| SymlinkTestSkipped {
            target: target.to_path_buf(),
            link: link.to_path_buf(),
            source,
        })
    }

    struct SymlinkTestSkipped {
        target: PathBuf,
        link: PathBuf,
        source: io::Error,
    }

    impl Display for SymlinkTestSkipped {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                formatter,
                "skipped storage metadata symlink safety test: symlink creation unavailable from {} to {}: {}",
                self.link.display(),
                self.target.display(),
                self.source
            )
        }
    }

    #[cfg(unix)]
    fn try_create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn try_create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_file(target, link)
    }
}
