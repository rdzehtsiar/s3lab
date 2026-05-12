// SPDX-License-Identifier: Apache-2.0

use super::journal::{
    Journal, JournalMutation, JournalObjectPut, JournalPhase, JournalRecord, EVENTS_DIR,
    JOURNAL_FILE_NAME,
};
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
use sha2::Sha256;
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

const BLOBS_DIR: &str = "blobs";
const BUCKET_METADATA_FILE: &str = "bucket.json";
const DIRTY_MUTATION_MARKER_FILE: &str = ".mutation-dirty";
const OBJECTS_DIR: &str = "objects";
const OBJECT_CONTENT_FILE: &str = "content.bin";
const OBJECT_METADATA_FILE: &str = "metadata.json";
const OBJECT_METADATA_SCHEMA_VERSION: u32 = 1;
const SNAPSHOT_MANIFEST_FILE: &str = "manifest.json";
const SNAPSHOT_MANIFEST_SCHEMA_VERSION: u32 = 1;
const SNAPSHOTS_DIR: &str = "snapshots";
const SNAPSHOT_STATE_DIRS: [&str; 3] = [STORAGE_ROOT_DIR, EVENTS_DIR, BLOBS_DIR];
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
    static FAIL_NEXT_STAGED_RESTORE_RENAME: Cell<bool> = const { Cell::new(false) };
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

    pub fn save_snapshot(&self, name: &str) -> Result<(), StorageError> {
        validate_snapshot_name(name)?;
        let _guard = lock_storage(&self.root, "save_snapshot")?;
        self.recover_if_dirty()?;

        let snapshot_dir = self.snapshot_dir(name);
        if path_exists(&snapshot_dir)? {
            return Err(snapshot_already_exists(name));
        }

        let temporary_snapshot_dir = create_temporary_sibling_dir(&snapshot_dir)?;
        if let Err(error) = self.copy_current_state_to_snapshot(&temporary_snapshot_dir) {
            remove_path_best_effort(&temporary_snapshot_dir);
            return Err(error);
        }

        if let Err(error) = rename_path(&temporary_snapshot_dir, &snapshot_dir) {
            remove_path_best_effort(&temporary_snapshot_dir);
            if path_exists(&snapshot_dir)? {
                return Err(snapshot_already_exists(name));
            }

            return Err(error);
        }

        Ok(())
    }

    pub fn restore_snapshot(&self, name: &str) -> Result<(), StorageError> {
        validate_snapshot_name(name)?;
        let _guard = lock_storage(&self.root, "restore_snapshot")?;

        let snapshot_dir = self.snapshot_dir(name);
        let Some(snapshot_metadata) = storage_path_metadata(&snapshot_dir)? else {
            return Err(snapshot_not_found(name));
        };
        if !snapshot_metadata.is_dir() {
            return Err(corrupt_state(
                snapshot_dir,
                "snapshot path exists but is not a directory",
            ));
        }

        let staged_paths = match self.stage_snapshot_restore(&snapshot_dir) {
            Ok(staged_paths) => staged_paths,
            Err(error) => {
                return Err(error);
            }
        };

        if let Err(error) = validate_staged_snapshot_contents(&staged_paths) {
            cleanup_staged_snapshot_paths(&staged_paths);
            return Err(error);
        }

        self.replace_current_state_with_staged_snapshot(staged_paths)?;

        remove_storage_path_if_exists(&self.dirty_marker_path())
    }

    pub fn reset(&self) -> Result<(), StorageError> {
        let _guard = lock_storage(&self.root, "reset")?;
        remove_storage_path_if_exists(&self.bucket_root())?;
        remove_storage_path_if_exists(&self.events_dir())?;
        remove_storage_path_if_exists(&self.blobs_dir())?;
        remove_storage_path_if_exists(&self.dirty_marker_path())?;
        sync_parent_dir_best_effort(&self.root);
        Ok(())
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
        self.recover_if_dirty()?;
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

        let record = BucketRecord {
            bucket: bucket.as_str().to_owned(),
        };
        self.commit_and_apply_mutation(JournalMutation::bucket_create(bucket), || {
            write_new_bucket_state_atomically(&bucket_dir, &record, bucket)
        })
    }

    fn list_buckets(&self) -> Result<Vec<BucketSummary>, StorageError> {
        let _guard = lock_storage(&self.root, "list_buckets")?;
        self.recover_if_dirty()?;
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
        self.recover_if_dirty()?;
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
        self.recover_if_dirty()?;
        self.ensure_bucket(bucket)?;

        let objects_dir = self.bucket_dir(bucket).join(OBJECTS_DIR);
        if path_exists(&objects_dir)?
            && self.bucket_has_visible_committed_objects(bucket, &objects_dir)?
        {
            return Err(StorageError::BucketNotEmpty {
                bucket: bucket.clone(),
            });
        }

        self.commit_and_apply_mutation(JournalMutation::bucket_delete(bucket), || {
            remove_dir_all(&self.bucket_dir(bucket))
        })
    }

    fn put_object(&self, request: PutObjectRequest) -> Result<StoredObjectMetadata, StorageError> {
        let _guard = lock_storage(&self.root, "put_object")?;
        self.recover_if_dirty()?;
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
        let content_sha256 = sha256_for_bytes(&request.bytes);
        self.store_blob(&content_sha256, &request.bytes)?;
        let mutation = JournalMutation::object_put(JournalObjectPut {
            bucket: request.bucket.as_str().to_owned(),
            key: request.key.as_str().to_owned(),
            content_length: metadata.content_length,
            content_sha256,
            etag: metadata.etag.clone(),
            content_type: metadata.content_type.clone(),
            last_modified_unix_seconds: metadata.last_modified.unix_timestamp(),
            last_modified_nanoseconds: metadata.last_modified.nanosecond(),
            user_metadata: metadata.user_metadata.clone(),
        });

        self.commit_and_apply_mutation(mutation, || {
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
        })
    }

    fn get_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObject, StorageError> {
        let _guard = lock_storage(&self.root, "get_object")?;
        self.recover_if_dirty()?;
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
        self.recover_if_dirty()?;
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
        self.recover_if_dirty()?;
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
        self.recover_if_dirty()?;
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
        self.recover_if_dirty()?;
        self.ensure_bucket(bucket)?;
        let encoded_key = encode_object_key(key)?;
        self.read_object_metadata(bucket, key, &encoded_key)?;

        let object_dir = self.object_dir(bucket, &encoded_key);
        self.commit_and_apply_mutation(JournalMutation::object_delete(bucket, key), || {
            remove_dir_all(&object_dir)?;

            let shard_dir = self
                .bucket_dir(bucket)
                .join(OBJECTS_DIR)
                .join(encoded_key.shard());
            if path_exists(&shard_dir)? && !directory_has_entries(&shard_dir)? {
                remove_dir(&shard_dir)?;
            }

            Ok(())
        })
    }
}

impl<C> FilesystemStorage<C> {
    fn bucket_root(&self) -> PathBuf {
        self.root.join(STORAGE_ROOT_DIR)
    }

    fn events_dir(&self) -> PathBuf {
        self.root.join(EVENTS_DIR)
    }

    fn blobs_dir(&self) -> PathBuf {
        self.root.join(BLOBS_DIR)
    }

    fn snapshot_root(&self) -> PathBuf {
        self.root.join(SNAPSHOTS_DIR)
    }

    fn snapshot_dir(&self, name: &str) -> PathBuf {
        self.snapshot_root().join(name)
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

    fn journal(&self) -> Journal {
        Journal::new(&self.root)
    }

    fn blob_path(&self, content_sha256: &str) -> Result<PathBuf, StorageError> {
        validate_content_sha256(content_sha256, &self.root)?;
        Ok(self
            .root
            .join(BLOBS_DIR)
            .join(&content_sha256[..2])
            .join(content_sha256))
    }

    fn dirty_marker_path(&self) -> PathBuf {
        self.root.join(DIRTY_MUTATION_MARKER_FILE)
    }

    fn copy_current_state_to_snapshot(&self, snapshot_dir: &Path) -> Result<(), StorageError> {
        let manifest = self.snapshot_manifest_for_current_state()?;
        for (source, target_name) in self.snapshot_state_sources() {
            if manifest
                .state
                .get(target_name)
                .is_some_and(|entry| entry.present)
            {
                copy_storage_dir_tree(&source, &snapshot_dir.join(target_name))?;
            }
        }

        write_snapshot_manifest(snapshot_dir, &manifest)?;
        Ok(())
    }

    fn stage_snapshot_restore(
        &self,
        snapshot_dir: &Path,
    ) -> Result<Vec<StagedSnapshotPath>, StorageError> {
        let manifest = read_validated_snapshot_manifest(snapshot_dir)?;
        let mut staged_paths = Vec::new();
        for target_name in SNAPSHOT_STATE_DIRS {
            let snapshot_child = snapshot_dir.join(target_name);
            let target = self.root.join(target_name);
            if !manifest
                .state
                .get(target_name)
                .is_some_and(|entry| entry.present)
            {
                staged_paths.push(StagedSnapshotPath {
                    target,
                    temporary: None,
                });
                continue;
            };

            let temporary = match create_temporary_sibling_dir(&target) {
                Ok(temporary) => temporary,
                Err(error) => {
                    cleanup_staged_snapshot_paths(&staged_paths);
                    return Err(error);
                }
            };
            if let Err(error) = copy_storage_dir_contents(&snapshot_child, &temporary) {
                remove_path_best_effort(&temporary);
                cleanup_staged_snapshot_paths(&staged_paths);
                return Err(error);
            }
            staged_paths.push(StagedSnapshotPath {
                target,
                temporary: Some(temporary),
            });
        }

        Ok(staged_paths)
    }

    fn replace_current_state_with_staged_snapshot(
        &self,
        staged_paths: Vec<StagedSnapshotPath>,
    ) -> Result<(), StorageError> {
        let mut backups = Vec::new();
        for staged_path in &staged_paths {
            let backup = match move_existing_path_to_backup(&staged_path.target) {
                Ok(backup) => backup,
                Err(error) => {
                    if let Err(rollback_error) = restore_snapshot_backups(backups) {
                        cleanup_staged_snapshot_paths(&staged_paths);
                        return Err(rollback_error);
                    }
                    cleanup_staged_snapshot_paths(&staged_paths);
                    return Err(error);
                }
            };

            if let Some(temporary) = &staged_path.temporary {
                if let Err(error) = rename_staged_snapshot_path(temporary, &staged_path.target) {
                    let current_restore = restore_snapshot_path_backup(&staged_path.target, backup);
                    let previous_restore = restore_snapshot_backups(backups);
                    cleanup_staged_snapshot_paths(&staged_paths);
                    current_restore?;
                    previous_restore?;
                    return Err(error);
                }
            }

            backups.push(SnapshotRestoreBackup {
                target: staged_path.target.clone(),
                backup,
            });
        }

        for backup in backups {
            discard_path_backup(backup.backup);
        }

        Ok(())
    }

    fn snapshot_manifest_for_current_state(&self) -> Result<SnapshotManifest, StorageError> {
        let mut state = BTreeMap::new();
        for (source, target_name) in self.snapshot_state_sources() {
            let present = match storage_path_metadata(&source)? {
                Some(metadata) if metadata.is_dir() => true,
                Some(_) => {
                    return Err(corrupt_state(
                        source,
                        "snapshot source path exists but is not a directory",
                    ));
                }
                None => false,
            };

            state.insert(target_name.to_owned(), SnapshotManifestEntry { present });
        }

        Ok(SnapshotManifest {
            schema_version: SNAPSHOT_MANIFEST_SCHEMA_VERSION,
            state,
        })
    }

    fn snapshot_state_sources(&self) -> [(PathBuf, &'static str); 3] {
        [
            (self.bucket_root(), STORAGE_ROOT_DIR),
            (self.events_dir(), EVENTS_DIR),
            (self.blobs_dir(), BLOBS_DIR),
        ]
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SnapshotRestoreBackup {
    target: PathBuf,
    backup: Option<PathBuf>,
}

fn restore_snapshot_backups(backups: Vec<SnapshotRestoreBackup>) -> Result<(), StorageError> {
    for backup in backups.into_iter().rev() {
        restore_snapshot_path_backup(&backup.target, backup.backup)?;
    }

    Ok(())
}

fn restore_snapshot_path_backup(
    path: &Path,
    backup_path: Option<PathBuf>,
) -> Result<(), StorageError> {
    remove_storage_path_if_exists(path)?;
    if let Some(backup_path) = backup_path {
        rename_path(&backup_path, path)?;
    }

    Ok(())
}

fn rename_staged_snapshot_path(from: &Path, to: &Path) -> Result<(), StorageError> {
    #[cfg(test)]
    if FAIL_NEXT_STAGED_RESTORE_RENAME.replace(false) {
        return Err(StorageError::Io {
            path: to.to_path_buf(),
            source: io::Error::other("forced staged restore rename failure"),
        });
    }

    rename_path(from, to)
}

impl<C> FilesystemStorage<C> {
    fn recover_if_dirty(&self) -> Result<(), StorageError> {
        let marker_path = self.dirty_marker_path();
        let Some(metadata) = storage_path_metadata(&marker_path)? else {
            return Ok(());
        };
        if !metadata.is_file() {
            return Err(corrupt_state(
                marker_path,
                "dirty mutation marker exists but is not a file",
            ));
        }

        self.recover_committed_journal_events()?;
        remove_file_if_exists(&marker_path)?;
        sync_parent_dir_best_effort(&marker_path);
        Ok(())
    }

    fn commit_and_apply_mutation<T>(
        &self,
        mutation: JournalMutation,
        apply: impl FnOnce() -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        self.write_dirty_marker()?;
        let journal = self.journal();
        let begin_sequence = match next_journal_sequence(&journal) {
            Ok(sequence) => sequence,
            Err(error) => {
                let _ = self.clear_dirty_marker();
                return Err(error);
            }
        };

        if let Err(error) = journal.append(&JournalRecord::begin(begin_sequence, mutation.clone()))
        {
            let _ = self.clear_dirty_marker();
            return Err(error);
        }

        let result = match apply() {
            Ok(result) => result,
            Err(error) => {
                let _ = self.clear_dirty_marker();
                return Err(error);
            }
        };

        journal.append(&JournalRecord::commit(begin_sequence + 1, mutation))?;
        self.clear_dirty_marker()?;
        Ok(result)
    }

    fn write_dirty_marker(&self) -> Result<(), StorageError> {
        write_bytes_atomically(&self.dirty_marker_path(), b"dirty\n")
    }

    fn clear_dirty_marker(&self) -> Result<(), StorageError> {
        let marker_path = self.dirty_marker_path();
        remove_file_if_exists(&marker_path)?;
        sync_parent_dir_best_effort(&marker_path);
        Ok(())
    }

    fn store_blob(&self, content_sha256: &str, bytes: &[u8]) -> Result<(), StorageError> {
        let blob_path = self.blob_path(content_sha256)?;
        if path_exists(&blob_path)? {
            let stored = read_bytes(&blob_path)?;
            validate_blob_bytes(&blob_path, content_sha256, bytes.len() as u64, &stored)?;
            return Ok(());
        }

        write_bytes_atomically(&blob_path, bytes)?;
        sync_parent_dir_best_effort(&blob_path);
        Ok(())
    }

    fn recover_committed_journal_events(&self) -> Result<(), StorageError> {
        let mut pending = None;
        for record in self.journal().read_records()? {
            match record.phase {
                JournalPhase::Begin => pending = Some(record.mutation),
                JournalPhase::Commit => {
                    if pending.as_ref() == Some(&record.mutation) {
                        self.recover_committed_mutation(&record.mutation)?;
                    }
                    pending = None;
                }
            }
        }

        Ok(())
    }

    fn recover_committed_mutation(&self, mutation: &JournalMutation) -> Result<(), StorageError> {
        match mutation {
            JournalMutation::BucketCreate { bucket } => {
                self.recover_bucket_create(&BucketName::new(bucket.clone()))
            }
            JournalMutation::BucketDelete { bucket } => {
                self.recover_bucket_delete(&BucketName::new(bucket.clone()))
            }
            JournalMutation::ObjectPut {
                bucket,
                key,
                content_length,
                content_sha256,
                etag,
                content_type,
                last_modified_unix_seconds,
                last_modified_nanoseconds,
                user_metadata,
            } => self.recover_object_put(RecoveredObjectPut {
                bucket: BucketName::new(bucket.clone()),
                key: ObjectKey::new(key.clone()),
                content_length: *content_length,
                content_sha256: content_sha256.clone(),
                etag: etag.clone(),
                content_type: content_type.clone(),
                last_modified: journal_last_modified(
                    *last_modified_unix_seconds,
                    *last_modified_nanoseconds,
                    &self.dirty_marker_path(),
                )?,
                user_metadata: user_metadata.clone(),
            }),
            JournalMutation::ObjectDelete { bucket, key } => self.recover_object_delete(
                &BucketName::new(bucket.clone()),
                &ObjectKey::new(key.clone()),
            ),
        }
    }

    fn recover_bucket_create(&self, bucket: &BucketName) -> Result<(), StorageError> {
        validate_recovered_bucket_name(bucket, &self.dirty_marker_path())?;
        create_dir_all(&self.bucket_root())?;
        let bucket_dir = self.bucket_dir(bucket);
        let record = BucketRecord {
            bucket: bucket.as_str().to_owned(),
        };

        let Some(metadata) = storage_path_metadata(&bucket_dir)? else {
            return write_new_bucket_state_atomically(&bucket_dir, &record, bucket);
        };
        if !metadata.is_dir() {
            return Err(corrupt_state(
                bucket_dir,
                "recovered bucket path exists but is not a directory",
            ));
        }

        let metadata_path = self.bucket_metadata_path(bucket);
        if path_exists(&metadata_path)? {
            let stored = self.read_bucket_record(bucket)?;
            validate_bucket_record(bucket, &metadata_path, &stored)?;
        } else {
            write_json_atomically(&metadata_path, &record)?;
        }

        let objects_dir = bucket_dir.join(OBJECTS_DIR);
        if path_exists(&objects_dir)? {
            require_storage_directory(&objects_dir, "missing bucket objects directory")?;
        } else {
            create_dir_all(&objects_dir)?;
        }
        sync_parent_dir_best_effort(&objects_dir);
        Ok(())
    }

    fn recover_bucket_delete(&self, bucket: &BucketName) -> Result<(), StorageError> {
        validate_recovered_bucket_name(bucket, &self.dirty_marker_path())?;
        let bucket_dir = self.bucket_dir(bucket);
        let Some(metadata) = storage_path_metadata(&bucket_dir)? else {
            return Ok(());
        };
        if !metadata.is_dir() {
            return Err(corrupt_state(
                bucket_dir,
                "recovered bucket path exists but is not a directory",
            ));
        }

        remove_dir_all(&bucket_dir)
    }

    fn recover_object_put(&self, recovered: RecoveredObjectPut) -> Result<(), StorageError> {
        validate_recovered_bucket_name(&recovered.bucket, &self.dirty_marker_path())?;
        let encoded_key = encode_object_key(&recovered.key)?;
        let blob_path = self.blob_path(&recovered.content_sha256)?;
        let bytes = read_bytes(&blob_path)?;
        validate_blob_bytes(
            &blob_path,
            &recovered.content_sha256,
            recovered.content_length,
            &bytes,
        )?;
        let actual_etag = etag_for_bytes(&bytes);
        if actual_etag != recovered.etag {
            return Err(corrupt_state(
                blob_path,
                "recovered blob ETag does not match journal object put",
            ));
        }

        self.recover_bucket_create(&recovered.bucket)?;
        let metadata = StoredObjectMetadata {
            bucket: recovered.bucket.clone(),
            key: recovered.key.clone(),
            etag: recovered.etag,
            content_length: recovered.content_length,
            content_type: recovered.content_type,
            last_modified: recovered.last_modified,
            user_metadata: recovered.user_metadata,
        };
        let record = ObjectMetadataRecord::from_metadata(&metadata);
        let object_dir = self.object_dir(&recovered.bucket, &encoded_key);
        match object_path_state(&object_dir) {
            Ok(ObjectPathState::Committed) => {
                let existing = self.read_object(&recovered.bucket, &recovered.key, &encoded_key)?;
                if object_matches_recovered_put(&existing, &metadata, &bytes) {
                    return Ok(());
                }

                write_object_files_atomically(&object_dir, &bytes, &record)
            }
            Ok(ObjectPathState::Missing) => {
                write_new_object_dir_atomically(&object_dir, &bytes, &record)
            }
            Err(StorageError::CorruptState { .. }) => {
                remove_new_object_dirs_best_effort(&object_dir);
                write_new_object_dir_atomically(&object_dir, &bytes, &record)
            }
            Err(error) => Err(error),
        }
    }

    fn recover_object_delete(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<(), StorageError> {
        validate_recovered_bucket_name(bucket, &self.dirty_marker_path())?;
        let bucket_dir = self.bucket_dir(bucket);
        if !path_exists(&bucket_dir)? {
            return Ok(());
        }

        let encoded_key = encode_object_key(key)?;
        let object_dir = self.object_dir(bucket, &encoded_key);
        if let Some(metadata) = storage_path_metadata(&object_dir)? {
            if !metadata.is_dir() {
                return Err(corrupt_state(
                    object_dir,
                    "recovered object delete path exists but is not a directory",
                ));
            }
            remove_dir_all(&object_dir)?;
        }

        let shard_dir = bucket_dir.join(OBJECTS_DIR).join(encoded_key.shard());
        if path_exists(&shard_dir)? && !directory_has_entries(&shard_dir)? {
            remove_dir(&shard_dir)?;
        }

        Ok(())
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

#[derive(Debug, Clone, Eq, PartialEq)]
struct StagedSnapshotPath {
    target: PathBuf,
    temporary: Option<PathBuf>,
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
#[serde(deny_unknown_fields)]
struct SnapshotManifest {
    schema_version: u32,
    state: BTreeMap<String, SnapshotManifestEntry>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotManifestEntry {
    present: bool,
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

struct RecoveredObjectPut {
    bucket: BucketName,
    key: ObjectKey,
    content_length: u64,
    content_sha256: String,
    etag: String,
    content_type: Option<String>,
    last_modified: OffsetDateTime,
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

fn sha256_for_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    lower_hex(&digest)
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

fn next_journal_sequence(journal: &Journal) -> Result<u64, StorageError> {
    Ok(journal
        .read_records()?
        .last()
        .map_or(1, |record| record.sequence + 1))
}

fn validate_recovered_bucket_name(bucket: &BucketName, path: &Path) -> Result<(), StorageError> {
    if is_valid_s3_bucket_name(bucket.as_str()) {
        return Ok(());
    }

    Err(corrupt_state(
        path.to_path_buf(),
        "journal contains an invalid bucket name",
    ))
}

fn validate_content_sha256(content_sha256: &str, path: &Path) -> Result<(), StorageError> {
    if is_valid_content_sha256(content_sha256) {
        return Ok(());
    }

    Err(corrupt_state(
        path.to_path_buf(),
        "journal contains an invalid content sha256",
    ))
}

fn is_valid_content_sha256(content_sha256: &str) -> bool {
    content_sha256.len() == 64 && content_sha256.bytes().all(is_lower_hex_byte)
}

fn is_lower_hex_byte(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn validate_blob_bytes(
    path: &Path,
    expected_sha256: &str,
    expected_length: u64,
    bytes: &[u8],
) -> Result<(), StorageError> {
    if bytes.len() as u64 != expected_length {
        return Err(corrupt_state(
            path.to_path_buf(),
            "blob content length does not match journal object put",
        ));
    }

    let actual_sha256 = sha256_for_bytes(bytes);
    if actual_sha256 != expected_sha256 {
        return Err(corrupt_state(
            path.to_path_buf(),
            "blob content sha256 does not match journal object put",
        ));
    }

    Ok(())
}

fn object_matches_recovered_put(
    existing: &StoredObject,
    recovered: &StoredObjectMetadata,
    recovered_bytes: &[u8],
) -> bool {
    existing.bytes == recovered_bytes
        && existing.metadata.bucket == recovered.bucket
        && existing.metadata.key == recovered.key
        && existing.metadata.etag == recovered.etag
        && existing.metadata.content_length == recovered.content_length
        && existing.metadata.content_type == recovered.content_type
        && existing.metadata.last_modified == recovered.last_modified
        && existing.metadata.user_metadata == recovered.user_metadata
}

fn journal_last_modified(
    unix_seconds: i64,
    nanoseconds: u32,
    path: &Path,
) -> Result<OffsetDateTime, StorageError> {
    let base_time = OffsetDateTime::from_unix_timestamp(unix_seconds).map_err(|source| {
        corrupt_state(
            path.to_path_buf(),
            format!("invalid journal object put last_modified_unix_seconds: {source}"),
        )
    })?;

    base_time.replace_nanosecond(nanoseconds).map_err(|source| {
        corrupt_state(
            path.to_path_buf(),
            format!("invalid journal object put last_modified_nanoseconds: {source}"),
        )
    })
}

fn validate_snapshot_name(name: &str) -> Result<(), StorageError> {
    let has_valid_chars = name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
    if !name.is_empty() && name != "." && name != ".." && has_valid_chars {
        return Ok(());
    }

    Err(StorageError::InvalidArgument {
        message: format!(
            "invalid snapshot name {name:?}; use a non-empty name containing only ASCII letters, digits, '_', '-', or '.', and do not use '.' or '..'"
        ),
    })
}

fn snapshot_already_exists(name: &str) -> StorageError {
    StorageError::InvalidArgument {
        message: format!("snapshot already exists: {name}"),
    }
}

fn snapshot_not_found(name: &str) -> StorageError {
    StorageError::InvalidArgument {
        message: format!("snapshot does not exist: {name}"),
    }
}

fn write_snapshot_manifest(
    snapshot_dir: &Path,
    manifest: &SnapshotManifest,
) -> Result<(), StorageError> {
    write_json_atomically(&snapshot_dir.join(SNAPSHOT_MANIFEST_FILE), manifest)
}

fn read_validated_snapshot_manifest(snapshot_dir: &Path) -> Result<SnapshotManifest, StorageError> {
    let manifest_path = snapshot_dir.join(SNAPSHOT_MANIFEST_FILE);
    let Some(manifest_metadata) = storage_path_metadata(&manifest_path)? else {
        return Err(corrupt_state(manifest_path, "missing snapshot manifest"));
    };
    if !manifest_metadata.is_file() {
        return Err(corrupt_state(
            manifest_path,
            "snapshot manifest path exists but is not a file",
        ));
    }

    let manifest: SnapshotManifest = read_json(&manifest_path)?;
    validate_snapshot_manifest(snapshot_dir, &manifest)?;
    Ok(manifest)
}

fn validate_snapshot_manifest(
    snapshot_dir: &Path,
    manifest: &SnapshotManifest,
) -> Result<(), StorageError> {
    if manifest.schema_version != SNAPSHOT_MANIFEST_SCHEMA_VERSION {
        return Err(corrupt_state(
            snapshot_dir.join(SNAPSHOT_MANIFEST_FILE),
            "unsupported snapshot manifest schema version",
        ));
    }

    for required_name in SNAPSHOT_STATE_DIRS {
        if !manifest.state.contains_key(required_name) {
            return Err(corrupt_state(
                snapshot_dir.join(SNAPSHOT_MANIFEST_FILE),
                format!("snapshot manifest is missing state entry: {required_name}"),
            ));
        }
    }
    for state_name in manifest.state.keys() {
        if !is_snapshot_state_dir_name(state_name) {
            return Err(corrupt_state(
                snapshot_dir.join(SNAPSHOT_MANIFEST_FILE),
                format!("snapshot manifest contains unknown state entry: {state_name}"),
            ));
        }
    }

    for entry in sorted_directory_entries(snapshot_dir)? {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            return Err(corrupt_state(
                entry.path(),
                "snapshot top-level path is not valid UTF-8",
            ));
        };
        if file_name != SNAPSHOT_MANIFEST_FILE && !is_snapshot_state_dir_name(file_name) {
            return Err(corrupt_state(
                entry.path(),
                "snapshot contains unknown top-level path",
            ));
        }
    }

    for state_name in SNAPSHOT_STATE_DIRS {
        let snapshot_child = snapshot_dir.join(state_name);
        let child_metadata = storage_path_metadata(&snapshot_child)?;
        let manifest_entry = manifest
            .state
            .get(state_name)
            .expect("required snapshot state entries were checked above");
        match (manifest_entry.present, child_metadata) {
            (true, Some(metadata)) if metadata.is_dir() => {}
            (true, Some(_)) => {
                return Err(corrupt_state(
                    snapshot_child,
                    "snapshot state path exists but is not a directory",
                ));
            }
            (true, None) => {
                return Err(corrupt_state(
                    snapshot_child,
                    "snapshot manifest declares state path but it is missing",
                ));
            }
            (false, Some(_)) => {
                return Err(corrupt_state(
                    snapshot_child,
                    "snapshot manifest declares state path absent but it exists",
                ));
            }
            (false, None) => {}
        }
    }

    Ok(())
}

fn is_snapshot_state_dir_name(name: &str) -> bool {
    SNAPSHOT_STATE_DIRS.contains(&name)
}

fn copy_storage_dir_tree(source: &Path, destination: &Path) -> Result<(), StorageError> {
    create_dir_all(destination)?;
    copy_storage_dir_contents(source, destination)
}

fn copy_storage_dir_contents(source: &Path, destination: &Path) -> Result<(), StorageError> {
    require_storage_directory(source, "missing snapshot source directory")?;
    require_storage_directory(destination, "missing snapshot destination directory")?;

    for entry in sorted_directory_entries(source)? {
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let Some(metadata) = storage_path_metadata(&source_path)? else {
            continue;
        };

        if metadata.is_dir() {
            copy_storage_dir_tree(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            copy_storage_file(&source_path, &destination_path)?;
        } else {
            return Err(corrupt_state(
                source_path,
                "snapshot source path is neither a file nor a directory",
            ));
        }
    }

    Ok(())
}

fn copy_storage_file(source: &Path, destination: &Path) -> Result<(), StorageError> {
    let bytes = read_bytes(source)?;
    write_bytes_synced(destination, &bytes)
}

fn cleanup_staged_snapshot_paths(staged_paths: &[StagedSnapshotPath]) {
    for staged_path in staged_paths {
        if let Some(temporary) = &staged_path.temporary {
            remove_path_best_effort(temporary);
        }
    }
}

fn validate_staged_snapshot_contents(
    staged_paths: &[StagedSnapshotPath],
) -> Result<(), StorageError> {
    if let Some(bucket_root) = staged_snapshot_temporary_path(staged_paths, STORAGE_ROOT_DIR) {
        validate_bucket_root_contents(bucket_root)?;
    }

    let blobs_dir = staged_snapshot_temporary_path(staged_paths, BLOBS_DIR);
    if let Some(events_dir) = staged_snapshot_temporary_path(staged_paths, EVENTS_DIR) {
        validate_snapshot_events_contents(events_dir, blobs_dir)?;
    }

    if let Some(blobs_dir) = blobs_dir {
        validate_snapshot_blob_contents(blobs_dir)?;
    }

    Ok(())
}

fn staged_snapshot_temporary_path<'a>(
    staged_paths: &'a [StagedSnapshotPath],
    target_name: &str,
) -> Option<&'a Path> {
    staged_paths
        .iter()
        .find(|staged_path| staged_path.target.file_name() == Some(OsStr::new(target_name)))
        .and_then(|staged_path| staged_path.temporary.as_deref())
}

fn validate_bucket_root_contents(bucket_root: &Path) -> Result<(), StorageError> {
    require_storage_directory(bucket_root, "missing snapshot buckets directory")?;
    for bucket_entry in sorted_directory_entries(bucket_root)? {
        if is_hidden_storage_entry(&bucket_entry) {
            continue;
        }

        let bucket_path = bucket_entry.path();
        let Some(bucket_metadata) = storage_path_metadata(&bucket_path)? else {
            continue;
        };
        if !bucket_metadata.is_dir() {
            continue;
        }

        let metadata_path = bucket_path.join(BUCKET_METADATA_FILE);
        if !path_exists(&metadata_path)? {
            return Err(corrupt_state(metadata_path, "missing bucket metadata"));
        }

        let record: BucketRecord = read_json(&metadata_path)?;
        validate_listed_bucket_record(&bucket_path, &metadata_path, &record)?;
        let bucket = BucketName::new(record.bucket);
        let objects_dir = bucket_path.join(OBJECTS_DIR);
        require_storage_directory(&objects_dir, "missing bucket objects directory")?;
        validate_bucket_objects(&bucket, bucket_root, &objects_dir)?;
    }

    Ok(())
}

fn validate_bucket_objects(
    bucket: &BucketName,
    bucket_root: &Path,
    objects_dir: &Path,
) -> Result<(), StorageError> {
    for shard_entry in sorted_directory_entries(objects_dir)? {
        let Some(shard_path) = visible_storage_directory_path(
            &shard_entry,
            "visible object shard path exists but is not a directory",
        )?
        else {
            continue;
        };

        for object_entry in sorted_directory_entries(&shard_path)? {
            let Some(object_path) = visible_storage_directory_path(
                &object_entry,
                "visible object path exists but is not a directory",
            )?
            else {
                continue;
            };

            validate_bucket_object(bucket, bucket_root, &object_path)?;
        }
    }

    Ok(())
}

fn validate_bucket_object(
    bucket: &BucketName,
    bucket_root: &Path,
    object_path: &Path,
) -> Result<(), StorageError> {
    object_path_state(object_path)?;
    let metadata_path = object_path.join(OBJECT_METADATA_FILE);
    let metadata = object_metadata_from_path(&metadata_path)?;
    if metadata.bucket != *bucket {
        return Err(corrupt_state(
            metadata_path,
            "object metadata bucket does not match containing bucket",
        ));
    }

    let encoded_key = encode_object_key(&metadata.key).map_err(|_| {
        corrupt_state(
            metadata_path.clone(),
            "object metadata contains an invalid key",
        )
    })?;
    let expected_metadata_path = bucket_root
        .join(encode_bucket_name(bucket).as_path_component())
        .join(OBJECTS_DIR)
        .join(encoded_key.shard())
        .join(encoded_key.as_path_component())
        .join(OBJECT_METADATA_FILE);
    if expected_metadata_path != metadata_path {
        return Err(corrupt_state(
            metadata_path,
            "object metadata is stored under the wrong hashed path",
        ));
    }

    validate_object_content(&object_path.join(OBJECT_CONTENT_FILE), &metadata)
}

fn validate_snapshot_events_contents(
    events_dir: &Path,
    blobs_dir: Option<&Path>,
) -> Result<(), StorageError> {
    require_storage_directory(events_dir, "missing snapshot events directory")?;
    let journal_path = events_dir.join(JOURNAL_FILE_NAME);
    if !path_exists(&journal_path)? {
        return Ok(());
    }

    let records = Journal::at_path(&journal_path).read_records()?;
    validate_snapshot_journal_mutation_semantics(&journal_path, blobs_dir, &records)?;
    Ok(())
}

fn validate_snapshot_journal_mutation_semantics(
    journal_path: &Path,
    blobs_dir: Option<&Path>,
    records: &[JournalRecord],
) -> Result<(), StorageError> {
    let mut pending = None;
    for record in records {
        match record.phase {
            JournalPhase::Begin => pending = Some(&record.mutation),
            JournalPhase::Commit => {
                if pending == Some(&record.mutation) {
                    validate_committed_snapshot_journal_mutation(
                        journal_path,
                        blobs_dir,
                        &record.mutation,
                    )?;
                }
                pending = None;
            }
        }
    }

    Ok(())
}

fn validate_committed_snapshot_journal_mutation(
    journal_path: &Path,
    blobs_dir: Option<&Path>,
    mutation: &JournalMutation,
) -> Result<(), StorageError> {
    match mutation {
        JournalMutation::BucketCreate { bucket } | JournalMutation::BucketDelete { bucket } => {
            validate_journal_bucket_name(bucket, journal_path)
        }
        JournalMutation::ObjectDelete { bucket, key } => {
            validate_journal_bucket_name(bucket, journal_path)?;
            validate_journal_object_key(key, journal_path)
        }
        JournalMutation::ObjectPut {
            bucket,
            key,
            content_length,
            content_sha256,
            etag,
            last_modified_unix_seconds,
            last_modified_nanoseconds,
            ..
        } => {
            validate_journal_bucket_name(bucket, journal_path)?;
            validate_journal_object_key(key, journal_path)?;
            validate_content_sha256(content_sha256, journal_path)?;
            journal_last_modified(
                *last_modified_unix_seconds,
                *last_modified_nanoseconds,
                journal_path,
            )?;
            validate_journal_object_put_blob(
                journal_path,
                blobs_dir,
                content_sha256,
                *content_length,
                etag,
            )
        }
    }
}

fn validate_journal_bucket_name(bucket: &str, journal_path: &Path) -> Result<(), StorageError> {
    if is_valid_s3_bucket_name(bucket) {
        return Ok(());
    }

    Err(corrupt_state(
        journal_path.to_path_buf(),
        "journal contains an invalid bucket name",
    ))
}

fn validate_journal_object_key(key: &str, journal_path: &Path) -> Result<(), StorageError> {
    encode_object_key(&ObjectKey::new(key.to_owned())).map_err(|_| {
        corrupt_state(
            journal_path.to_path_buf(),
            "journal contains an invalid object key",
        )
    })?;
    Ok(())
}

fn validate_journal_object_put_blob(
    journal_path: &Path,
    blobs_dir: Option<&Path>,
    content_sha256: &str,
    content_length: u64,
    etag: &str,
) -> Result<(), StorageError> {
    let blobs_dir = blobs_dir.ok_or_else(|| {
        corrupt_state(
            journal_path.to_path_buf(),
            "journal object put references a blob but snapshot blobs directory is missing",
        )
    })?;
    let blob_path = blobs_dir.join(&content_sha256[..2]).join(content_sha256);
    require_storage_file(
        &blob_path,
        "journal object put references a missing snapshot blob",
    )?;
    let bytes = fs::read(&blob_path).map_err(|source| StorageError::Io {
        path: blob_path.clone(),
        source,
    })?;
    validate_blob_bytes(&blob_path, content_sha256, content_length, &bytes)?;

    let actual_etag = etag_for_bytes(&bytes);
    if actual_etag == etag {
        return Ok(());
    }

    Err(corrupt_state(
        blob_path,
        "snapshot blob ETag does not match journal object put",
    ))
}

fn validate_snapshot_blob_contents(blobs_dir: &Path) -> Result<(), StorageError> {
    require_storage_directory(blobs_dir, "missing snapshot blobs directory")?;
    for shard_entry in sorted_directory_entries(blobs_dir)? {
        let shard_path = shard_entry.path();
        let Some(shard_metadata) = storage_path_metadata(&shard_path)? else {
            continue;
        };
        if !shard_metadata.is_dir() {
            return Err(corrupt_state(
                shard_path,
                "snapshot blob shard path exists but is not a directory",
            ));
        }

        let shard_name = blob_path_component(&shard_path, "snapshot blob shard path")?;
        validate_blob_shard_name(shard_name, &shard_path)?;
        validate_snapshot_blob_shard(&shard_path, shard_name)?;
    }

    Ok(())
}

fn validate_snapshot_blob_shard(shard_path: &Path, shard_name: &str) -> Result<(), StorageError> {
    for blob_entry in sorted_directory_entries(shard_path)? {
        let blob_path = blob_entry.path();
        let Some(blob_metadata) = storage_path_metadata(&blob_path)? else {
            continue;
        };
        if !blob_metadata.is_file() {
            return Err(corrupt_state(
                blob_path,
                "snapshot blob path exists but is not a file",
            ));
        }

        let blob_name = blob_path_component(&blob_path, "snapshot blob path")?;
        validate_snapshot_blob_name(blob_name, shard_name, &blob_path)?;
        let bytes = read_bytes(&blob_path)?;
        validate_blob_content_sha256(&blob_path, blob_name, &bytes)?;
    }

    Ok(())
}

fn blob_path_component<'a>(path: &'a Path, path_kind: &str) -> Result<&'a str, StorageError> {
    path.file_name().and_then(OsStr::to_str).ok_or_else(|| {
        corrupt_state(
            path.to_path_buf(),
            format!("{path_kind} is not valid UTF-8"),
        )
    })
}

fn validate_blob_shard_name(shard_name: &str, path: &Path) -> Result<(), StorageError> {
    if shard_name.len() == 2 && shard_name.bytes().all(is_lower_hex_byte) {
        return Ok(());
    }

    Err(corrupt_state(
        path.to_path_buf(),
        "snapshot blob shard name is not a two-character lowercase hex prefix",
    ))
}

fn validate_snapshot_blob_name(
    blob_name: &str,
    shard_name: &str,
    path: &Path,
) -> Result<(), StorageError> {
    if is_valid_content_sha256(blob_name) && blob_name.starts_with(shard_name) {
        return Ok(());
    }

    Err(corrupt_state(
        path.to_path_buf(),
        "snapshot blob file name does not match its hashed storage path",
    ))
}

fn validate_blob_content_sha256(
    path: &Path,
    expected_sha256: &str,
    bytes: &[u8],
) -> Result<(), StorageError> {
    let actual_sha256 = sha256_for_bytes(bytes);
    if actual_sha256 == expected_sha256 {
        return Ok(());
    }

    Err(corrupt_state(
        path.to_path_buf(),
        "snapshot blob content sha256 does not match blob file name",
    ))
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

#[cfg(test)]
fn fail_next_staged_restore_rename_for_test() {
    FAIL_NEXT_STAGED_RESTORE_RENAME.set(true);
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

fn remove_file_if_exists(path: &Path) -> Result<(), StorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(StorageError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn remove_storage_path_if_exists(path: &Path) -> Result<(), StorageError> {
    let Some(metadata) = storage_path_metadata(path)? else {
        return Ok(());
    };

    if metadata.is_dir() {
        fs::remove_dir_all(path).map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    } else if metadata.is_file() {
        fs::remove_file(path).map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    } else {
        return Err(corrupt_state(
            path.to_path_buf(),
            "storage path is neither a file nor a directory",
        ));
    }

    sync_parent_dir_best_effort(path);
    Ok(())
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
        fail_next_new_object_state_write_for_test, fail_next_staged_restore_rename_for_test,
        move_existing_path_to_backup, replace_file_with_temporary, restore_path_backup,
        sha256_for_bytes, sibling_path, storage_path_metadata, write_new_bucket_state,
        write_temporary_sibling, BucketRecord, FilesystemStorage, StorageClock, StorageError,
        OBJECTS_DIR,
    };
    use crate::s3::bucket::BucketName;
    use crate::s3::object::ObjectKey;
    use crate::storage::journal::{
        Journal, JournalMutation, JournalObjectPut, JournalPhase, JournalRecord,
    };
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
    fn restore_snapshot_restores_current_state_when_staged_rename_fails() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let storage =
            FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");
        storage.create_bucket(&bucket).expect("create bucket");
        storage
            .put_object(PutObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                bytes: b"snapshot".to_vec(),
                content_type: None,
                user_metadata: BTreeMap::new(),
            })
            .expect("put snapshot object");
        storage.save_snapshot("baseline").expect("save snapshot");
        storage
            .put_object(PutObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                bytes: b"current".to_vec(),
                content_type: None,
                user_metadata: BTreeMap::new(),
            })
            .expect("put current object");

        fail_next_staged_restore_rename_for_test();
        let error = storage
            .restore_snapshot("baseline")
            .expect_err("forced staged restore rename fails");

        assert!(matches!(error, StorageError::Io { .. }));
        assert_eq!(
            storage
                .get_object_bytes(&bucket, &key)
                .expect("current state remains readable"),
            b"current"
        );
        assert!(temp_dir.path().join("snapshots").join("baseline").is_dir());
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

    #[test]
    fn mutations_append_begin_commit_journal_records_and_store_blobs() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let storage =
            FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");

        storage.create_bucket(&bucket).expect("create bucket");
        storage
            .put_object(PutObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                bytes: b"body".to_vec(),
                content_type: Some("text/plain".to_owned()),
                user_metadata: BTreeMap::from([("owner".to_owned(), "local".to_owned())]),
            })
            .expect("put object");
        storage.delete_object(&bucket, &key).expect("delete object");
        storage.delete_bucket(&bucket).expect("delete bucket");

        let records = Journal::new(temp_dir.path())
            .read_records()
            .expect("read journal");
        assert_eq!(records.len(), 8);
        assert!(records
            .chunks_exact(2)
            .all(|pair| pair[0].phase == JournalPhase::Begin
                && pair[1].phase == JournalPhase::Commit
                && pair[0].mutation == pair[1].mutation));
        assert!(matches!(
            &records[0].mutation,
            JournalMutation::BucketCreate { bucket: recorded } if recorded == "example-bucket"
        ));
        assert!(matches!(
            &records[2].mutation,
            JournalMutation::ObjectPut {
                bucket: recorded_bucket,
                key: recorded_key,
                content_sha256,
                content_type,
                last_modified_unix_seconds,
                last_modified_nanoseconds,
                user_metadata,
                ..
            } if recorded_bucket == "example-bucket"
                && recorded_key == "object.txt"
                && content_sha256 == &sha256_for_bytes(b"body")
                && content_type.as_deref() == Some("text/plain")
                && *last_modified_unix_seconds == fixed_time().unix_timestamp()
                && *last_modified_nanoseconds == fixed_time().nanosecond()
                && user_metadata.get("owner").map(String::as_str) == Some("local")
        ));
        assert!(storage
            .blob_path(&sha256_for_bytes(b"body"))
            .expect("blob path")
            .is_file());
    }

    #[test]
    fn dirty_marker_recovers_committed_object_put_from_journal_and_blob() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let storage =
            FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");
        storage.create_bucket(&bucket).expect("create bucket");
        storage
            .put_object(PutObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                bytes: b"body".to_vec(),
                content_type: None,
                user_metadata: BTreeMap::new(),
            })
            .expect("put object");
        let encoded_key = encode_object_key(&key).expect("valid key");
        let object_dir = storage.object_dir(&bucket, &encoded_key);
        fs::remove_dir_all(&object_dir).expect("simulate incomplete materialized object put");
        storage.write_dirty_marker().expect("write dirty marker");

        let bytes = storage
            .get_object_bytes(&bucket, &key)
            .expect("read recovered object");

        assert_eq!(bytes, b"body");
        assert!(object_dir.join("content.bin").is_file());
        assert_eq!(
            storage
                .get_object_metadata(&bucket, &key)
                .expect("read recovered metadata")
                .last_modified,
            fixed_time()
        );
        assert!(!storage.dirty_marker_path().exists());
    }

    #[test]
    fn failed_object_put_apply_is_not_committed_or_replayed() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let storage =
            FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");
        storage.create_bucket(&bucket).expect("create bucket");
        fail_next_new_object_state_write_for_test();

        let error = storage
            .put_object(PutObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                bytes: b"body".to_vec(),
                content_type: None,
                user_metadata: BTreeMap::new(),
            })
            .expect_err("forced object write fails");

        assert!(matches!(error, StorageError::Io { .. }));
        let records = Journal::new(temp_dir.path())
            .read_records()
            .expect("read journal");
        assert!(records
            .iter()
            .any(|record| record.phase == JournalPhase::Begin
                && matches!(
                    &record.mutation,
                    JournalMutation::ObjectPut {
                        bucket: recorded_bucket,
                        key: recorded_key,
                        ..
                    } if recorded_bucket == "example-bucket" && recorded_key == "object.txt"
                )));
        assert!(!records
            .iter()
            .any(|record| record.phase == JournalPhase::Commit
                && matches!(
                    &record.mutation,
                    JournalMutation::ObjectPut {
                        bucket: recorded_bucket,
                        key: recorded_key,
                        ..
                    } if recorded_bucket == "example-bucket" && recorded_key == "object.txt"
                )));
        storage.write_dirty_marker().expect("write dirty marker");

        let error = storage
            .get_object_metadata(&bucket, &key)
            .expect_err("failed put is not recovered from begin-only journal");

        assert!(matches!(error, StorageError::NoSuchKey { .. }));
        assert!(!storage.dirty_marker_path().exists());
    }

    #[test]
    fn dirty_marker_does_not_apply_begin_only_object_put() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let storage =
            FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");
        storage.create_bucket(&bucket).expect("create bucket");
        storage
            .store_blob(&sha256_for_bytes(b"body"), b"body")
            .expect("store blob");
        append_begin_only_object_put(temp_dir.path(), &bucket, &key, b"body");
        storage.write_dirty_marker().expect("write dirty marker");

        let error = storage
            .get_object_metadata(&bucket, &key)
            .expect_err("begin-only object put is not applied");

        assert!(matches!(error, StorageError::NoSuchKey { .. }));
        assert!(!storage.dirty_marker_path().exists());
    }

    #[test]
    fn committed_object_delete_recovery_keeps_dirty_marker_when_delete_cannot_apply() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let storage =
            FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");
        storage.create_bucket(&bucket).expect("create bucket");
        let encoded_key = encode_object_key(&key).expect("valid key");
        let object_dir = storage.object_dir(&bucket, &encoded_key);
        fs::create_dir_all(object_dir.parent().expect("object shard dir"))
            .expect("create object shard dir");
        fs::write(&object_dir, b"not a directory").expect("create blocking object path");
        append_committed_object_delete(temp_dir.path(), &bucket, &key);
        storage.write_dirty_marker().expect("write dirty marker");

        let error = storage
            .bucket_exists(&bucket)
            .expect_err("failed delete recovery stops operation");

        assert!(matches!(error, StorageError::CorruptState { .. }));
        assert!(storage.dirty_marker_path().exists());
        assert!(object_dir.is_file());
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

    fn append_begin_only_object_put(
        root: &Path,
        bucket: &BucketName,
        key: &ObjectKey,
        bytes: &[u8],
    ) {
        let journal = Journal::new(root);
        let next_sequence = journal
            .read_records()
            .expect("read records")
            .last()
            .map_or(1, |record| record.sequence + 1);
        journal
            .append(&JournalRecord::begin(
                next_sequence,
                JournalMutation::object_put(JournalObjectPut {
                    bucket: bucket.as_str().to_owned(),
                    key: key.as_str().to_owned(),
                    content_length: bytes.len() as u64,
                    content_sha256: sha256_for_bytes(bytes),
                    etag: super::etag_for_bytes(bytes),
                    content_type: Some(crate::storage::DEFAULT_OBJECT_CONTENT_TYPE.to_owned()),
                    last_modified_unix_seconds: fixed_time().unix_timestamp(),
                    last_modified_nanoseconds: fixed_time().nanosecond(),
                    user_metadata: BTreeMap::new(),
                }),
            ))
            .expect("append begin-only object put");
    }

    fn append_committed_object_delete(root: &Path, bucket: &BucketName, key: &ObjectKey) {
        let journal = Journal::new(root);
        let next_sequence = journal
            .read_records()
            .expect("read records")
            .last()
            .map_or(1, |record| record.sequence + 1);
        let mutation = JournalMutation::object_delete(bucket, key);
        journal
            .append(&JournalRecord::begin(next_sequence, mutation.clone()))
            .expect("append object delete begin");
        journal
            .append(&JournalRecord::commit(next_sequence + 1, mutation))
            .expect("append object delete commit");
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
