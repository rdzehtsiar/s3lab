// SPDX-License-Identifier: Apache-2.0

use super::StorageError;
use crate::s3::bucket::BucketName;
use crate::s3::object::ObjectKey;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

pub const JOURNAL_SCHEMA_VERSION: u32 = 1;
pub const EVENTS_DIR: &str = "events";
pub const JOURNAL_FILE_NAME: &str = "journal.jsonl";

static JOURNAL_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Journal {
    path: PathBuf,
}

impl Journal {
    pub fn new(storage_root: impl Into<PathBuf>) -> Self {
        Self {
            path: storage_root.into().join(EVENTS_DIR).join(JOURNAL_FILE_NAME),
        }
    }

    pub(crate) fn at_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, record: &JournalRecord) -> Result<(), StorageError> {
        let _guard = lock_journal(&self.path)?;
        record.validate_schema_version(&self.path)?;

        let next_sequence = self.next_sequence()?;
        if record.sequence != next_sequence {
            return Err(StorageError::InvalidArgument {
                message: format!(
                    "journal record sequence {} does not match next sequence {next_sequence}",
                    record.sequence
                ),
            });
        }

        let parent = self
            .path
            .parent()
            .ok_or_else(|| StorageError::InvalidArgument {
                message: format!(
                    "journal path has no parent directory: {}",
                    self.path.display()
                ),
            })?;
        create_dir_all(parent)?;

        let mut file = open_append_file(&self.path)?;
        let mut line = serde_json::to_vec(record).map_err(|source| StorageError::CorruptState {
            path: self.path.clone(),
            message: format!("could not serialize journal record: {source}"),
        })?;
        line.push(b'\n');
        file.write_all(&line).map_err(|source| StorageError::Io {
            path: self.path.clone(),
            source,
        })?;
        file.sync_all().map_err(|source| StorageError::Io {
            path: self.path.clone(),
            source,
        })
    }

    pub fn read_records(&self) -> Result<Vec<JournalRecord>, StorageError> {
        let _guard = lock_journal(&self.path)?;
        self.read_records_unlocked()
    }

    fn next_sequence(&self) -> Result<u64, StorageError> {
        Ok(self
            .read_records_unlocked()?
            .last()
            .map_or(1, |record| record.sequence + 1))
    }

    fn read_records_unlocked(&self) -> Result<Vec<JournalRecord>, StorageError> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(StorageError::Io {
                    path: self.path.clone(),
                    source,
                });
            }
        };

        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        if !bytes.ends_with(b"\n") {
            return Err(corrupt_journal(
                &self.path,
                "journal record is not newline terminated; file may be partially written",
            ));
        }

        let mut records = Vec::new();
        let mut expected_sequence = 1;
        for (line_index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }

            let record: JournalRecord =
                serde_json::from_slice(line).map_err(|source| StorageError::CorruptState {
                    path: self.path.clone(),
                    message: format!("invalid JSON at journal line {}: {source}", line_index + 1),
                })?;
            record.validate_schema_version(&self.path)?;
            if record.sequence != expected_sequence {
                return Err(corrupt_journal(
                    &self.path,
                    format!(
                        "journal line {} has sequence {}, expected {expected_sequence}",
                        line_index + 1,
                        record.sequence
                    ),
                ));
            }

            expected_sequence += 1;
            records.push(record);
        }

        Ok(records)
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct JournalRecord {
    pub schema_version: u32,
    pub sequence: u64,
    pub phase: JournalPhase,
    pub mutation: JournalMutation,
}

impl JournalRecord {
    pub fn begin(sequence: u64, mutation: JournalMutation) -> Self {
        Self {
            schema_version: JOURNAL_SCHEMA_VERSION,
            sequence,
            phase: JournalPhase::Begin,
            mutation,
        }
    }

    pub fn commit(sequence: u64, mutation: JournalMutation) -> Self {
        Self {
            schema_version: JOURNAL_SCHEMA_VERSION,
            sequence,
            phase: JournalPhase::Commit,
            mutation,
        }
    }

    fn validate_schema_version(&self, path: &Path) -> Result<(), StorageError> {
        if self.schema_version == JOURNAL_SCHEMA_VERSION {
            return Ok(());
        }

        Err(corrupt_journal(
            path,
            format!(
                "unsupported journal schema version {}; supported version is {JOURNAL_SCHEMA_VERSION}",
                self.schema_version
            ),
        ))
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalPhase {
    Begin,
    Commit,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JournalMutation {
    BucketCreate {
        bucket: String,
    },
    BucketDelete {
        bucket: String,
    },
    ObjectPut {
        bucket: String,
        key: String,
        content_length: u64,
        content_sha256: String,
        etag: String,
        content_type: Option<String>,
        last_modified_unix_seconds: i64,
        last_modified_nanoseconds: u32,
        user_metadata: BTreeMap<String, String>,
    },
    ObjectDelete {
        bucket: String,
        key: String,
    },
}

impl JournalMutation {
    pub fn bucket_create(bucket: &BucketName) -> Self {
        Self::BucketCreate {
            bucket: bucket.as_str().to_owned(),
        }
    }

    pub fn bucket_delete(bucket: &BucketName) -> Self {
        Self::BucketDelete {
            bucket: bucket.as_str().to_owned(),
        }
    }

    pub fn object_put(
        bucket: &BucketName,
        key: &ObjectKey,
        content_length: u64,
        content_sha256: impl Into<String>,
        etag: impl Into<String>,
        content_type: Option<String>,
        last_modified_unix_seconds: i64,
        last_modified_nanoseconds: u32,
        user_metadata: BTreeMap<String, String>,
    ) -> Self {
        Self::ObjectPut {
            bucket: bucket.as_str().to_owned(),
            key: key.as_str().to_owned(),
            content_length,
            content_sha256: content_sha256.into(),
            etag: etag.into(),
            content_type,
            last_modified_unix_seconds,
            last_modified_nanoseconds,
            user_metadata,
        }
    }

    pub fn object_delete(bucket: &BucketName, key: &ObjectKey) -> Self {
        Self::ObjectDelete {
            bucket: bucket.as_str().to_owned(),
            key: key.as_str().to_owned(),
        }
    }
}

fn lock_journal(path: &Path) -> Result<MutexGuard<'static, ()>, StorageError> {
    JOURNAL_LOCK.lock().map_err(|_| StorageError::Io {
        path: path.to_path_buf(),
        source: io::Error::other("journal lock poisoned"),
    })
}

fn create_dir_all(path: &Path) -> Result<(), StorageError> {
    fs::create_dir_all(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn open_append_file(path: &Path) -> Result<File, StorageError> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn corrupt_journal(path: &Path, message: impl Into<String>) -> StorageError {
    StorageError::CorruptState {
        path: path.to_path_buf(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Journal, JournalMutation, JournalPhase, JournalRecord, EVENTS_DIR, JOURNAL_FILE_NAME,
    };
    use crate::s3::bucket::BucketName;
    use crate::s3::object::ObjectKey;
    use crate::storage::StorageError;
    use std::collections::BTreeMap;
    use std::fs;

    #[test]
    fn journal_path_lives_under_events_directory() {
        let journal = Journal::new("state");

        assert_eq!(
            journal.path(),
            std::path::Path::new("state")
                .join(EVENTS_DIR)
                .join(JOURNAL_FILE_NAME)
        );
    }

    #[test]
    fn append_and_read_round_trip_all_mutation_records() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let journal = Journal::new(temp_dir.path());
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("prefix/object.txt");
        let records = vec![
            JournalRecord::begin(1, JournalMutation::bucket_create(&bucket)),
            JournalRecord::commit(2, JournalMutation::bucket_create(&bucket)),
            JournalRecord::begin(
                3,
                JournalMutation::object_put(
                    &bucket,
                    &key,
                    11,
                    "b94d27b9934d3e08a52e52d7da7dabfadeadf2c7f99a9c720f7d30a85e9e0ff",
                    "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"",
                    Some("text/plain".to_owned()),
                    1_778_400_000,
                    123,
                    BTreeMap::from([
                        ("a-key".to_owned(), "first".to_owned()),
                        ("z-key".to_owned(), "last".to_owned()),
                    ]),
                ),
            ),
            JournalRecord::commit(
                4,
                JournalMutation::object_put(
                    &bucket,
                    &key,
                    11,
                    "b94d27b9934d3e08a52e52d7da7dabfadeadf2c7f99a9c720f7d30a85e9e0ff",
                    "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"",
                    Some("text/plain".to_owned()),
                    1_778_400_000,
                    123,
                    BTreeMap::from([
                        ("a-key".to_owned(), "first".to_owned()),
                        ("z-key".to_owned(), "last".to_owned()),
                    ]),
                ),
            ),
            JournalRecord::begin(5, JournalMutation::object_delete(&bucket, &key)),
            JournalRecord::commit(6, JournalMutation::object_delete(&bucket, &key)),
            JournalRecord::begin(7, JournalMutation::bucket_delete(&bucket)),
            JournalRecord::commit(8, JournalMutation::bucket_delete(&bucket)),
        ];

        for record in &records {
            journal.append(record).expect("append record");
        }

        assert_eq!(journal.read_records().expect("read records"), records);
    }

    #[test]
    fn append_writes_deterministic_jsonl_in_sequence_order() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let journal = Journal::new(temp_dir.path());
        let bucket = BucketName::new("example-bucket");

        journal
            .append(&JournalRecord::begin(
                1,
                JournalMutation::bucket_create(&bucket),
            ))
            .expect("append begin");
        journal
            .append(&JournalRecord::commit(
                2,
                JournalMutation::bucket_create(&bucket),
            ))
            .expect("append commit");

        assert_eq!(
            fs::read_to_string(journal.path()).expect("read journal"),
            "{\"schema_version\":1,\"sequence\":1,\"phase\":\"begin\",\"mutation\":{\"type\":\"bucket_create\",\"bucket\":\"example-bucket\"}}\n\
             {\"schema_version\":1,\"sequence\":2,\"phase\":\"commit\",\"mutation\":{\"type\":\"bucket_create\",\"bucket\":\"example-bucket\"}}\n"
        );
        assert_eq!(
            journal
                .read_records()
                .expect("read records")
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn append_rejects_non_next_sequence() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let journal = Journal::new(temp_dir.path());
        let bucket = BucketName::new("example-bucket");

        let error = journal
            .append(&JournalRecord::begin(
                2,
                JournalMutation::bucket_create(&bucket),
            ))
            .expect_err("sequence must start at one");

        assert!(matches!(error, StorageError::InvalidArgument { .. }));
        assert!(error.to_string().contains("next sequence 1"));
    }

    #[test]
    fn read_rejects_non_contiguous_sequence_ordering() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let journal = Journal::new(temp_dir.path());
        fs::create_dir_all(journal.path().parent().expect("journal parent"))
            .expect("create parent");
        fs::write(
            journal.path(),
            "{\"schema_version\":1,\"sequence\":1,\"phase\":\"begin\",\"mutation\":{\"type\":\"bucket_create\",\"bucket\":\"example-bucket\"}}\n\
             {\"schema_version\":1,\"sequence\":3,\"phase\":\"commit\",\"mutation\":{\"type\":\"bucket_create\",\"bucket\":\"example-bucket\"}}\n",
        )
        .expect("write journal");

        let error = journal
            .read_records()
            .expect_err("sequence gap is corrupt journal state");

        assert!(matches!(error, StorageError::CorruptState { .. }));
        assert!(error.to_string().contains("expected 2"));
    }

    #[test]
    fn read_rejects_unsupported_schema_version() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let journal = Journal::new(temp_dir.path());
        fs::create_dir_all(journal.path().parent().expect("journal parent"))
            .expect("create parent");
        fs::write(
            journal.path(),
            "{\"schema_version\":2,\"sequence\":1,\"phase\":\"begin\",\"mutation\":{\"type\":\"bucket_create\",\"bucket\":\"example-bucket\"}}\n",
        )
        .expect("write journal");

        let error = journal
            .read_records()
            .expect_err("unsupported schema version fails");

        assert!(matches!(error, StorageError::CorruptState { .. }));
        assert!(error
            .to_string()
            .contains("unsupported journal schema version 2"));
    }

    #[test]
    fn read_rejects_invalid_json_record() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let journal = Journal::new(temp_dir.path());
        fs::create_dir_all(journal.path().parent().expect("journal parent"))
            .expect("create parent");
        fs::write(journal.path(), b"{not-json}\n").expect("write invalid journal");

        let error = journal.read_records().expect_err("invalid JSON fails");

        assert!(matches!(error, StorageError::CorruptState { .. }));
        assert!(error.to_string().contains("invalid JSON at journal line 1"));
    }

    #[test]
    fn read_rejects_partial_or_truncated_record() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let journal = Journal::new(temp_dir.path());
        fs::create_dir_all(journal.path().parent().expect("journal parent"))
            .expect("create parent");
        fs::write(
            journal.path(),
            b"{\"schema_version\":1,\"sequence\":1,\"phase\":\"begin\"",
        )
        .expect("write truncated journal");

        let error = journal
            .read_records()
            .expect_err("truncated journal record fails");

        assert!(matches!(error, StorageError::CorruptState { .. }));
        assert!(error.to_string().contains("not newline terminated"));
    }

    #[test]
    fn journal_record_constructors_set_current_schema_and_phase() {
        let bucket = BucketName::new("example-bucket");

        assert_eq!(
            JournalRecord::begin(1, JournalMutation::bucket_create(&bucket)),
            JournalRecord {
                schema_version: 1,
                sequence: 1,
                phase: JournalPhase::Begin,
                mutation: JournalMutation::BucketCreate {
                    bucket: "example-bucket".to_owned(),
                },
            }
        );
        assert_eq!(
            JournalRecord::commit(2, JournalMutation::bucket_delete(&bucket)).phase,
            JournalPhase::Commit
        );
    }
}
