// SPDX-License-Identifier: Apache-2.0

use super::key::{encode_bucket_name, encode_object_key, EncodedObjectKey};
use super::{
    BucketSummary, ListObjectsOptions, ObjectListing, PutObjectRequest, Storage, StorageError,
    StoredObject, StoredObjectMetadata, STORAGE_ROOT_DIR,
};
use crate::s3::bucket::BucketName;
use crate::s3::object::ObjectKey;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use time::OffsetDateTime;

const BUCKET_METADATA_FILE: &str = "bucket.json";
const OBJECTS_DIR: &str = "objects";
const OBJECT_CONTENT_FILE: &str = "content.bin";
const OBJECT_METADATA_FILE: &str = "metadata.json";
const OBJECT_METADATA_SCHEMA_VERSION: u32 = 1;
static TEMPORARY_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
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
    pub root: PathBuf,
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
}

impl<C: StorageClock> Storage for FilesystemStorage<C> {
    fn create_bucket(&self, bucket: &BucketName) -> Result<(), StorageError> {
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

            if !entry
                .file_type()
                .map_err(|source| StorageError::Io {
                    path: entry.path(),
                    source,
                })?
                .is_dir()
            {
                continue;
            }

            let metadata_path = entry.path().join(BUCKET_METADATA_FILE);
            if !path_exists(&metadata_path)? {
                return Err(corrupt_state(metadata_path, "missing bucket metadata"));
            }

            let record: BucketRecord = read_json(&metadata_path)?;
            validate_listed_bucket_record(&entry.path(), &metadata_path, &record)?;
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
        if path_exists(&objects_dir)? && directory_has_entries(&objects_dir)? {
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
            content_type: request.content_type,
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
        if options.continuation_token.is_some() {
            return Err(StorageError::InvalidArgument {
                message: "list_objects continuation tokens are not supported yet".to_owned(),
            });
        }

        self.ensure_bucket(bucket)?;

        let objects_dir = self.bucket_dir(bucket).join(OBJECTS_DIR);
        if !path_exists(&objects_dir)? {
            return Ok(ObjectListing {
                bucket: bucket.clone(),
                objects: Vec::new(),
                next_continuation_token: None,
            });
        }

        let mut objects = Vec::new();
        for shard_entry in sorted_directory_entries(&objects_dir)? {
            if is_hidden_storage_entry(&shard_entry) {
                continue;
            }

            if !shard_entry
                .file_type()
                .map_err(|source| StorageError::Io {
                    path: shard_entry.path(),
                    source,
                })?
                .is_dir()
            {
                continue;
            }

            for object_entry in sorted_directory_entries(&shard_entry.path())? {
                if is_hidden_storage_entry(&object_entry) {
                    continue;
                }

                if !object_entry
                    .file_type()
                    .map_err(|source| StorageError::Io {
                        path: object_entry.path(),
                        source,
                    })?
                    .is_dir()
                {
                    continue;
                }

                let metadata_path = object_entry.path().join(OBJECT_METADATA_FILE);
                let metadata = self.read_listed_object_metadata(bucket, &metadata_path)?;
                if options
                    .prefix
                    .as_ref()
                    .is_some_and(|prefix| !metadata.key.as_str().starts_with(prefix.as_str()))
                {
                    continue;
                }

                objects.push(metadata);
            }
        }

        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(ObjectListing {
            bucket: bucket.clone(),
            objects,
            next_continuation_token: None,
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
        if !path_exists(&object_dir)? {
            return Err(StorageError::NoSuchKey {
                bucket: bucket.clone(),
                key: key.clone(),
            });
        }

        let metadata_path = object_dir.join(OBJECT_METADATA_FILE);
        if !path_exists(&metadata_path)? {
            return Err(corrupt_state(metadata_path, "missing object metadata"));
        }

        let content_path = object_dir.join(OBJECT_CONTENT_FILE);
        if !path_exists(&content_path)? {
            return Err(corrupt_state(content_path, "missing object content"));
        }

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

fn sorted_directory_entries(path: &Path) -> Result<Vec<fs::DirEntry>, StorageError> {
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

fn directory_has_entries(path: &Path) -> Result<bool, StorageError> {
    Ok(sorted_directory_entries(path)?.into_iter().next().is_some())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, StorageError> {
    let bytes = read_bytes(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|source| corrupt_state(path.to_path_buf(), format!("invalid json: {source}")))
}

fn read_bytes(path: &Path) -> Result<Vec<u8>, StorageError> {
    fs::read(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_object_content_bytes(path: &Path) -> Result<Vec<u8>, StorageError> {
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
    let temporary_bucket_dir = temporary_sibling_path(bucket_dir);
    create_dir(&temporary_bucket_dir)?;

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
    let temporary_object_dir = temporary_sibling_path(object_dir);
    create_dir(&temporary_object_dir)?;

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
    if FAIL_NEXT_NEW_OBJECT_STATE_WRITE.with(|fail| fail.replace(false)) {
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
    FAIL_NEXT_NEW_OBJECT_STATE_WRITE.with(|fail| fail.set(true));
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
    if !path_exists(object_dir)? {
        return Ok(ObjectPathState::Missing);
    }

    if !object_dir.is_dir() {
        return Err(corrupt_state(
            object_dir.to_path_buf(),
            "object path exists but is not a directory",
        ));
    }

    let content_path = object_dir.join(OBJECT_CONTENT_FILE);
    let metadata_path = object_dir.join(OBJECT_METADATA_FILE);
    let content_exists = path_exists(&content_path)?;
    let metadata_exists = path_exists(&metadata_path)?;
    if content_exists && metadata_exists {
        if !content_path.is_file() {
            return Err(corrupt_state(
                content_path,
                "object content path exists but is not a file",
            ));
        }

        if !metadata_path.is_file() {
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
    STORAGE_LOCK.lock().map_err(|_| {
        corrupt_state(
            root.to_path_buf(),
            format!("{operation} storage lock poisoned"),
        )
    })
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

    let temporary_path = temporary_sibling_path(path);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary_path)
        .map_err(|source| StorageError::Io {
            path: temporary_path.clone(),
            source,
        })?;
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

    let backup_path = backup_sibling_path(path);
    rename_path(path, &backup_path)?;
    Ok(Some(backup_path))
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
    if path.is_dir() {
        let _ = fs::remove_dir_all(path);
    } else {
        let _ = fs::remove_file(path);
    }
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

fn temporary_sibling_path(path: &Path) -> PathBuf {
    sibling_path(path, "tmp")
}

fn backup_sibling_path(path: &Path) -> PathBuf {
    sibling_path(path, "bak")
}

fn sibling_path(path: &Path, purpose: &str) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("storage-file");
    let counter = TEMPORARY_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    path.with_file_name(format!(
        ".{file_name}.{purpose}-{}-{counter}",
        std::process::id(),
    ))
}

fn validate_bucket_record(
    bucket: &BucketName,
    metadata_path: &Path,
    record: &BucketRecord,
) -> Result<(), StorageError> {
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

fn path_exists(path: &Path) -> Result<bool, StorageError> {
    path.try_exists().map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn create_dir(path: &Path) -> Result<(), StorageError> {
    fs::create_dir(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn create_dir_all(path: &Path) -> Result<(), StorageError> {
    fs::create_dir_all(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn remove_dir(path: &Path) -> Result<(), StorageError> {
    fs::remove_dir(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn remove_dir_all(path: &Path) -> Result<(), StorageError> {
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
        commit_object_files, fail_next_new_object_state_write_for_test,
        replace_file_with_temporary, restore_path_backup, write_new_bucket_state, BucketRecord,
        FilesystemStorage, StorageClock, StorageError, OBJECTS_DIR,
    };
    use crate::s3::bucket::BucketName;
    use crate::s3::object::ObjectKey;
    use crate::storage::key::encode_object_key;
    use crate::storage::{PutObjectRequest, Storage};
    use std::collections::BTreeMap;
    use std::fs;
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

    #[test]
    fn new_stores_root_without_normalizing() {
        let storage = FilesystemStorage::new("./s3lab-data");

        assert_eq!(storage.root, std::path::PathBuf::from("./s3lab-data"));
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
    fn restore_path_backup_without_backup_leaves_existing_path_untouched() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let path = temp_dir.path().join("content.bin");
        fs::write(&path, b"current").expect("write current path");

        restore_path_backup(&path, None);

        assert_eq!(fs::read(path).expect("read current path"), b"current");
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
}
