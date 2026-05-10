// SPDX-License-Identifier: Apache-2.0

use s3lab::s3::bucket::BucketName;
use s3lab::s3::object::ObjectKey;
use s3lab::storage::fs::{FilesystemStorage, StorageClock};
use s3lab::storage::key::{encode_bucket_name, encode_object_key};
use s3lab::storage::{
    ListObjectsOptions, PutObjectRequest, Storage, StorageError, StoredObjectMetadata,
    STORAGE_ROOT_DIR,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

#[test]
fn list_buckets_returns_bucket_names_sorted_by_original_name() {
    let (_temp_dir, storage) = storage();
    storage
        .create_bucket(&BucketName::new("z-bucket"))
        .expect("create z bucket");
    storage
        .create_bucket(&BucketName::new("a-bucket"))
        .expect("create a bucket");
    storage
        .create_bucket(&BucketName::new("m-bucket"))
        .expect("create m bucket");

    let buckets = storage.list_buckets().expect("list buckets");

    assert_eq!(
        buckets
            .iter()
            .map(|bucket| bucket.name.as_str())
            .collect::<Vec<_>>(),
        ["a-bucket", "m-bucket", "z-bucket"]
    );
}

#[test]
fn duplicate_bucket_create_returns_bucket_already_exists() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");

    storage.create_bucket(&bucket).expect("create bucket");
    let error = storage.create_bucket(&bucket).expect_err("duplicate fails");

    assert!(matches!(
        error,
        StorageError::BucketAlreadyExists { bucket: failed_bucket }
            if failed_bucket == bucket
    ));
}

#[test]
fn list_buckets_ignores_hidden_incomplete_bucket_dirs() {
    let (temp_dir, storage) = storage();
    let bucket_root = temp_dir.path().join(STORAGE_ROOT_DIR);
    fs::create_dir_all(bucket_root.join(".bucket-staging.tmp-1"))
        .expect("create hidden incomplete bucket dir");
    storage
        .create_bucket(&BucketName::new("example-bucket"))
        .expect("create bucket");

    let buckets = storage.list_buckets().expect("list buckets");

    assert_eq!(
        buckets
            .iter()
            .map(|bucket| bucket.name.as_str())
            .collect::<Vec<_>>(),
        ["example-bucket"]
    );
}

#[test]
fn list_buckets_ignores_non_directory_entries_under_bucket_root() {
    let (temp_dir, storage) = storage();
    let bucket_root = temp_dir.path().join(STORAGE_ROOT_DIR);
    fs::create_dir_all(&bucket_root).expect("create bucket root");
    fs::write(bucket_root.join("not-a-bucket"), b"plain file").expect("write non-bucket file");
    storage
        .create_bucket(&BucketName::new("example-bucket"))
        .expect("create bucket");

    let buckets = storage.list_buckets().expect("list buckets");

    assert_eq!(
        buckets
            .iter()
            .map(|bucket| bucket.name.as_str())
            .collect::<Vec<_>>(),
        ["example-bucket"]
    );
}

#[test]
fn bucket_exists_returns_false_for_missing_bucket() {
    let (_temp_dir, storage) = storage();

    let exists = storage
        .bucket_exists(&BucketName::new("missing-bucket"))
        .expect("check missing bucket");

    assert!(!exists);
}

#[test]
fn bucket_exists_rejects_mismatched_bucket_metadata() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    write_bucket_metadata(temp_dir.path(), &bucket, r#"{"bucket":"other-bucket"}"#);

    let error = storage
        .bucket_exists(&bucket)
        .expect_err("mismatched bucket metadata fails");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn list_buckets_rejects_mismatched_bucket_metadata_path() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    write_bucket_metadata(temp_dir.path(), &bucket, r#"{"bucket":"other-bucket"}"#);

    let error = storage
        .list_buckets()
        .expect_err("mismatched bucket metadata path fails");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn bucket_metadata_invalid_json_is_corrupt_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    write_bucket_metadata(temp_dir.path(), &bucket, "not json");

    let exists_error = storage
        .bucket_exists(&bucket)
        .expect_err("bucket_exists rejects invalid metadata");
    let list_error = storage
        .list_buckets()
        .expect_err("list_buckets rejects invalid metadata");

    assert!(matches!(exists_error, StorageError::CorruptState { .. }));
    assert!(matches!(list_error, StorageError::CorruptState { .. }));
}

#[test]
fn create_bucket_rejects_existing_bucket_directory_without_metadata() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    fs::create_dir_all(bucket_dir(temp_dir.path(), &bucket)).expect("create corrupt bucket dir");

    let error = storage
        .create_bucket(&bucket)
        .expect_err("corrupt existing bucket dir fails create");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn list_buckets_reports_missing_bucket_metadata_as_corrupt_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    fs::remove_file(bucket_dir(temp_dir.path(), &bucket).join("bucket.json"))
        .expect("remove bucket metadata");

    let error = storage
        .list_buckets()
        .expect_err("missing bucket metadata fails");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn delete_bucket_returns_no_such_bucket_for_missing_bucket() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("missing-bucket");

    let error = storage
        .delete_bucket(&bucket)
        .expect_err("delete missing bucket fails");

    assert!(matches!(
        error,
        StorageError::NoSuchBucket { bucket: failed_bucket }
            if failed_bucket == bucket
    ));
}

#[test]
fn delete_bucket_fails_when_bucket_contains_objects() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, "object.txt", b"body"))
        .expect("put object");

    let error = storage
        .delete_bucket(&bucket)
        .expect_err("non-empty bucket delete fails");

    assert!(matches!(
        error,
        StorageError::BucketNotEmpty { bucket: failed_bucket }
            if failed_bucket == bucket
    ));
}

#[test]
fn delete_object_removes_object_state_and_allows_bucket_delete() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("nested/object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    let object_paths = object_paths(temp_dir.path(), &bucket, &key);

    storage
        .delete_object(&bucket, &key)
        .expect("delete existing object");

    assert!(!object_paths.content.is_file());
    assert!(!object_paths.metadata.is_file());
    assert_eq!(
        object_keys(
            &storage
                .list_objects(&bucket, ListObjectsOptions::default())
                .expect("list after delete")
                .objects
        ),
        Vec::<&str>::new()
    );

    let reopened =
        FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
    let missing_error = reopened
        .get_object_metadata(&bucket, &key)
        .expect_err("deleted object stays deleted after reopen");
    assert!(matches!(missing_error, StorageError::NoSuchKey { .. }));

    reopened
        .delete_bucket(&bucket)
        .expect("delete empty bucket");
    assert!(!bucket_dir(temp_dir.path(), &bucket).exists());
}

#[test]
fn delete_object_prunes_empty_shard_directory() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    let shard_dir = object_paths(temp_dir.path(), &bucket, &key)
        .dir
        .parent()
        .expect("object dir has shard parent")
        .to_path_buf();

    storage.delete_object(&bucket, &key).expect("delete object");

    assert!(!shard_dir.exists());
}

#[test]
fn delete_object_preserves_shard_directory_with_sibling_object() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let first_key = ObjectKey::new("first.txt");
    let first_shard = encode_object_key(&first_key)
        .expect("valid first key")
        .shard()
        .to_owned();
    let sibling_key = (0..10_000)
        .map(|index| ObjectKey::new(format!("sibling-{index}.txt")))
        .find(|candidate| {
            encode_object_key(candidate)
                .expect("valid sibling key")
                .shard()
                == first_shard
        })
        .expect("find key in same shard");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, first_key.as_str(), b"first"))
        .expect("put first object");
    storage
        .put_object(put_request(&bucket, sibling_key.as_str(), b"sibling"))
        .expect("put sibling object");
    let shard_dir = object_paths(temp_dir.path(), &bucket, &first_key)
        .dir
        .parent()
        .expect("object dir has shard parent")
        .to_path_buf();

    storage
        .delete_object(&bucket, &first_key)
        .expect("delete first object");

    assert!(shard_dir.is_dir());
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &sibling_key)
            .expect("read sibling object"),
        b"sibling"
    );
}

#[test]
fn put_object_persists_bytes_and_metadata() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");

    let stored = storage
        .put_object(PutObjectRequest {
            bucket: bucket.clone(),
            key: ObjectKey::new("prefix/example.txt"),
            bytes: b"hello".to_vec(),
            content_type: Some("text/plain".to_owned()),
            user_metadata: BTreeMap::from([
                ("z-key".to_owned(), "last".to_owned()),
                ("a-key".to_owned(), "first".to_owned()),
            ]),
        })
        .expect("put object");

    assert_metadata_matches(&stored, &bucket, "prefix/example.txt", 5);
    assert_eq!(stored.content_type.as_deref(), Some("text/plain"));
    assert_eq!(stored.last_modified, fixed_time());
    assert_eq!(
        stored.user_metadata.keys().collect::<Vec<_>>(),
        vec!["a-key", "z-key"]
    );
    assert_eq!(
        storage
            .get_object_metadata(&bucket, &ObjectKey::new("prefix/example.txt"))
            .expect("get metadata"),
        stored
    );
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &ObjectKey::new("prefix/example.txt"))
            .expect("get bytes"),
        b"hello"
    );
    assert_eq!(
        storage
            .get_object(&bucket, &ObjectKey::new("prefix/example.txt"))
            .expect("get object"),
        s3lab::storage::StoredObject {
            metadata: stored,
            bytes: b"hello".to_vec(),
        }
    );
}

#[test]
fn missing_object_content_is_corrupt_state_for_metadata_reads() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    fs::remove_file(object_paths(temp_dir.path(), &bucket, &key).content)
        .expect("remove object content");

    let metadata_error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("metadata rejects missing content");
    let list_error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list rejects missing content");
    let bytes_error = storage
        .get_object_bytes(&bucket, &key)
        .expect_err("bytes rejects missing content");

    assert!(matches!(metadata_error, StorageError::CorruptState { .. }));
    assert!(matches!(list_error, StorageError::CorruptState { .. }));
    assert!(matches!(bytes_error, StorageError::CorruptState { .. }));
}

#[test]
fn list_objects_reports_missing_object_metadata_as_corrupt_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    fs::remove_file(object_paths(temp_dir.path(), &bucket, &key).metadata)
        .expect("remove object metadata");

    let error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list rejects missing object metadata");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn truncated_object_content_is_corrupt_state_for_reads_and_listing() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    fs::write(object_paths(temp_dir.path(), &bucket, &key).content, b"bo")
        .expect("truncate object content");

    let metadata_error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("metadata rejects truncated content");
    let list_error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list rejects truncated content");
    let bytes_error = storage
        .get_object_bytes(&bucket, &key)
        .expect_err("bytes rejects truncated content");

    assert!(matches!(metadata_error, StorageError::CorruptState { .. }));
    assert!(matches!(list_error, StorageError::CorruptState { .. }));
    assert!(matches!(bytes_error, StorageError::CorruptState { .. }));
}

#[test]
fn modified_object_content_is_corrupt_state_for_reads_and_listing() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    fs::write(
        object_paths(temp_dir.path(), &bucket, &key).content,
        b"xxxx",
    )
    .expect("modify object content without changing length");

    let metadata_error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("metadata rejects modified content");
    let list_error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list rejects modified content");
    let bytes_error = storage
        .get_object_bytes(&bucket, &key)
        .expect_err("bytes rejects modified content");

    assert!(matches!(metadata_error, StorageError::CorruptState { .. }));
    assert!(matches!(list_error, StorageError::CorruptState { .. }));
    assert!(matches!(bytes_error, StorageError::CorruptState { .. }));
}

#[test]
fn unsupported_object_metadata_schema_version_is_corrupt_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    rewrite_object_metadata_field(
        temp_dir.path(),
        &bucket,
        &key,
        "schema_version",
        serde_json::json!(2),
    );

    let error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("unsupported schema fails metadata read");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn invalid_object_last_modified_seconds_are_corrupt_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    rewrite_object_metadata_field(
        temp_dir.path(),
        &bucket,
        &key,
        "last_modified_unix_seconds",
        serde_json::json!(i64::MAX),
    );

    let error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("invalid timestamp seconds fail metadata read");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn invalid_object_last_modified_nanoseconds_are_corrupt_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    rewrite_object_metadata_field(
        temp_dir.path(),
        &bucket,
        &key,
        "last_modified_nanoseconds",
        serde_json::json!(1_000_000_000_u64),
    );

    let error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("invalid timestamp nanoseconds fail metadata read");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn put_object_overwrites_existing_object() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, "object.txt", b"first"))
        .expect("put first object");

    let overwritten = storage
        .put_object(PutObjectRequest {
            bucket: bucket.clone(),
            key: ObjectKey::new("object.txt"),
            bytes: b"second".to_vec(),
            content_type: Some("application/octet-stream".to_owned()),
            user_metadata: BTreeMap::from([("version".to_owned(), "2".to_owned())]),
        })
        .expect("overwrite object");

    assert_metadata_matches(&overwritten, &bucket, "object.txt", 6);
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &ObjectKey::new("object.txt"))
            .expect("get overwritten bytes"),
        b"second"
    );
    assert_eq!(
        storage
            .get_object_metadata(&bucket, &ObjectKey::new("object.txt"))
            .expect("get overwritten metadata")
            .user_metadata["version"],
        "2"
    );
}

#[test]
fn put_object_rejects_existing_partial_object_directories() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");

    for (key, partial_file) in [
        ("missing-metadata.txt", "content"),
        ("missing-content.txt", "metadata"),
    ] {
        let key = ObjectKey::new(key);
        let object_paths = object_paths(temp_dir.path(), &bucket, &key);
        fs::create_dir_all(&object_paths.dir).expect("create partial object dir");
        match partial_file {
            "content" => fs::write(&object_paths.content, b"body").expect("write content"),
            "metadata" => fs::write(&object_paths.metadata, b"{}").expect("write metadata"),
            _ => unreachable!("test case uses known partial file names"),
        }

        let error = storage
            .put_object(put_request(&bucket, key.as_str(), b"new body"))
            .expect_err("partial existing object dir fails put");

        assert!(matches!(error, StorageError::CorruptState { .. }));
        assert!(
            object_paths.dir.exists(),
            "corrupt object state should be preserved for diagnosis"
        );
    }
}

#[test]
fn concurrent_same_object_overwrites_are_serialized() {
    let (_temp_dir, storage) = storage();
    let storage = Arc::new(storage);
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");

    let bodies = (0..24)
        .map(|index| format!("body-{index:02}-{}", "x".repeat(4096)).into_bytes())
        .collect::<Vec<_>>();
    let barrier = Arc::new(Barrier::new(bodies.len()));
    let handles = bodies
        .iter()
        .cloned()
        .map(|body| {
            let storage = Arc::clone(&storage);
            let bucket = bucket.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                storage
                    .put_object(put_request(&bucket, "object.txt", &body))
                    .expect("concurrent put");
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().expect("thread joins");
    }

    let key = ObjectKey::new("object.txt");
    let bytes = storage
        .get_object_bytes(&bucket, &key)
        .expect("get final object bytes");
    let metadata = storage
        .get_object_metadata(&bucket, &key)
        .expect("get final object metadata");
    let listing = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect("list final object");

    assert!(bodies.iter().any(|body| body == &bytes));
    assert_eq!(metadata.content_length, bytes.len() as u64);
    assert_eq!(listing.objects.len(), 1);
    assert_eq!(listing.objects[0].content_length, bytes.len() as u64);
}

#[test]
fn concurrent_puts_and_deletes_do_not_leave_corrupt_object_state() {
    let (_temp_dir, storage) = storage();
    let storage = Arc::new(storage);
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"seed"))
        .expect("put seed object");

    let thread_count = 16;
    let barrier = Arc::new(Barrier::new(thread_count));
    let handles = (0..thread_count)
        .map(|index| {
            let storage = Arc::clone(&storage);
            let bucket = bucket.clone();
            let key = key.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                if index % 2 == 0 {
                    let body = format!("body-{index:02}-{}", "x".repeat(2048));
                    storage
                        .put_object(put_request(&bucket, key.as_str(), body.as_bytes()))
                        .expect("concurrent put");
                    return;
                }

                match storage.delete_object(&bucket, &key) {
                    Ok(()) | Err(StorageError::NoSuchKey { .. }) => {}
                    Err(error) => panic!("unexpected delete error: {error}"),
                }
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().expect("thread joins");
    }

    let listing = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect("list final objects");
    assert!(listing.objects.len() <= 1);

    match storage.get_object_bytes(&bucket, &key) {
        Ok(bytes) => {
            let metadata = storage
                .get_object_metadata(&bucket, &key)
                .expect("metadata for existing object");
            assert_eq!(metadata.content_length, bytes.len() as u64);
            assert_eq!(listing.objects.len(), 1);
            assert_eq!(listing.objects[0].content_length, bytes.len() as u64);
        }
        Err(StorageError::NoSuchKey { .. }) => assert!(listing.objects.is_empty()),
        Err(error) => panic!("unexpected final read error: {error}"),
    }
}

#[test]
fn list_objects_returns_keys_sorted_by_original_key() {
    let (_temp_dir, storage) = storage_with_objects(["z.txt", "a.txt", "nested/m.txt"]);

    let listing = storage
        .list_objects(
            &BucketName::new("example-bucket"),
            ListObjectsOptions::default(),
        )
        .expect("list objects");

    assert_eq!(
        object_keys(&listing.objects),
        ["a.txt", "nested/m.txt", "z.txt"]
    );
    assert_eq!(listing.next_continuation_token, None);
}

#[test]
fn list_objects_ignores_non_directory_entries_under_objects_and_shards() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");

    let objects_dir = bucket_dir(temp_dir.path(), &bucket).join("objects");
    fs::write(objects_dir.join("not-a-shard"), b"plain file").expect("write non-shard file");
    let shard_dir = object_paths(temp_dir.path(), &bucket, &key)
        .dir
        .parent()
        .expect("object dir has shard parent")
        .to_path_buf();
    fs::write(shard_dir.join("not-an-object"), b"plain file").expect("write non-object file");

    let listing = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect("list objects");

    assert_eq!(object_keys(&listing.objects), ["object.txt"]);
}

#[test]
fn list_objects_filters_by_prefix() {
    let (_temp_dir, storage) = storage_with_objects(["images/a.png", "images/b.png", "logs/a.txt"]);

    let listing = storage
        .list_objects(
            &BucketName::new("example-bucket"),
            ListObjectsOptions {
                prefix: Some(ObjectKey::new("images/")),
                ..ListObjectsOptions::default()
            },
        )
        .expect("list objects with prefix");

    assert_eq!(
        object_keys(&listing.objects),
        ["images/a.png", "images/b.png"]
    );
    assert!(!listing.is_truncated);
}

#[test]
fn list_objects_ignores_hidden_incomplete_shards_and_object_dirs() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");

    let objects_dir = bucket_dir(temp_dir.path(), &bucket).join("objects");
    fs::create_dir_all(objects_dir.join(".hidden-shard").join("incomplete-object"))
        .expect("create hidden incomplete shard");
    fs::create_dir_all(
        objects_dir
            .join(encode_object_key(&key).expect("valid key").shard())
            .join(".hidden-object.tmp-1"),
    )
    .expect("create hidden incomplete object dir");

    let listing = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect("list objects");

    assert_eq!(object_keys(&listing.objects), ["object.txt"]);
}

#[test]
fn hidden_staged_object_dir_is_not_visible_as_missing_key_or_listing_corruption() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");

    let encoded_key = encode_object_key(&key).expect("valid key");
    fs::create_dir_all(
        bucket_dir(temp_dir.path(), &bucket)
            .join("objects")
            .join(encoded_key.shard())
            .join(".hidden-object.tmp-1"),
    )
    .expect("create hidden staged object dir");

    let metadata_error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("hidden staged object is not committed");
    let listing = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect("list ignores hidden staged object");

    assert!(matches!(metadata_error, StorageError::NoSuchKey { .. }));
    assert!(listing.objects.is_empty());
}

#[test]
fn safe_odd_keys_round_trip_without_becoming_filesystem_paths() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    let keys = [
        "folder/file.txt",
        "spaces in name.txt",
        "unicodé/雪.txt",
        "../traversal-looking/../../object.txt",
        ".",
        "..",
        "con",
    ];

    for key in keys {
        storage
            .put_object(put_request(&bucket, key, key.as_bytes()))
            .expect("put odd key");
        assert_eq!(
            storage
                .get_object_bytes(&bucket, &ObjectKey::new(key))
                .expect("get odd key"),
            key.as_bytes()
        );
    }

    let path_components = filesystem_path_components(temp_dir.path());
    for raw_segment in [
        "folder",
        "file.txt",
        "spaces in name.txt",
        "unicodé",
        "雪.txt",
        "traversal-looking",
        "object.txt",
        "con",
    ] {
        assert!(
            !path_components
                .iter()
                .any(|component| component == raw_segment),
            "raw object key segment appeared on disk: {raw_segment}"
        );
    }
}

#[test]
fn raw_key_segments_are_not_used_in_object_paths() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("../raw directory/space file.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");

    let encoded_bucket = encode_bucket_name(&bucket);
    let encoded_key = encode_object_key(&key).expect("valid key");
    let expected_object_dir = temp_dir
        .path()
        .join(STORAGE_ROOT_DIR)
        .join(encoded_bucket.as_path_component())
        .join("objects")
        .join(encoded_key.shard())
        .join(encoded_key.as_path_component());

    assert!(expected_object_dir.join("content.bin").is_file());
    assert!(expected_object_dir.join("metadata.json").is_file());
    assert!(!filesystem_path_components(temp_dir.path())
        .iter()
        .any(|component| component == "raw directory" || component == "space file.txt"));
}

#[test]
fn missing_bucket_returns_no_such_bucket() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("missing-bucket");

    let list_error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list missing bucket fails");
    let put_error = storage
        .put_object(put_request(&bucket, "object.txt", b"body"))
        .expect_err("put missing bucket fails");

    assert!(matches!(list_error, StorageError::NoSuchBucket { .. }));
    assert!(matches!(put_error, StorageError::NoSuchBucket { .. }));
}

#[test]
fn missing_key_returns_no_such_key() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("missing.txt");
    storage.create_bucket(&bucket).expect("create bucket");

    let metadata_error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("missing metadata fails");
    let object_error = storage
        .get_object(&bucket, &key)
        .expect_err("missing object fails");
    let bytes_error = storage
        .get_object_bytes(&bucket, &key)
        .expect_err("missing bytes fails");
    let delete_error = storage
        .delete_object(&bucket, &key)
        .expect_err("missing delete fails");

    assert!(matches!(metadata_error, StorageError::NoSuchKey { .. }));
    assert!(matches!(object_error, StorageError::NoSuchKey { .. }));
    assert!(matches!(bytes_error, StorageError::NoSuchKey { .. }));
    assert!(matches!(delete_error, StorageError::NoSuchKey { .. }));
}

#[test]
fn storage_reopens_from_same_root() {
    let temp_dir = TempDir::new().expect("temp dir");
    let root = temp_dir.path().to_path_buf();
    let bucket = BucketName::new("example-bucket");

    {
        let storage = FilesystemStorage::with_clock(root.clone(), FixedClock(fixed_time()));
        storage.create_bucket(&bucket).expect("create bucket");
        storage
            .put_object(put_request(&bucket, "object.txt", b"persisted"))
            .expect("put object");
    }

    let reopened = FilesystemStorage::with_clock(root, FixedClock(fixed_time()));
    assert!(reopened.bucket_exists(&bucket).expect("bucket exists"));
    assert_eq!(
        reopened
            .get_object_bytes(&bucket, &ObjectKey::new("object.txt"))
            .expect("get reopened object"),
        b"persisted"
    );
}

#[test]
fn list_objects_paginates_with_tokens_for_next_unreturned_key() {
    let (_temp_dir, storage) = storage_with_objects(["z.txt", "a.txt", "m.txt"]);
    let bucket = BucketName::new("example-bucket");

    let first_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: None,
                continuation_token: None,
                max_keys: 2,
            },
        )
        .expect("list first page");

    assert_eq!(object_keys(&first_page.objects), ["a.txt", "m.txt"]);
    assert_eq!(first_page.max_keys, 2);
    assert!(first_page.is_truncated);
    let expected_token = continuation_token_for("z.txt");
    assert_eq!(
        first_page.next_continuation_token.as_deref(),
        Some(expected_token.as_str())
    );

    let second_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: None,
                continuation_token: first_page.next_continuation_token,
                max_keys: 2,
            },
        )
        .expect("list second page");

    assert_eq!(object_keys(&second_page.objects), ["z.txt"]);
    assert!(!second_page.is_truncated);
    assert_eq!(second_page.next_continuation_token, None);
}

#[test]
fn list_objects_paginates_after_prefix_filtering() {
    let (_temp_dir, storage) = storage_with_objects(["images/a.png", "logs/a.txt", "images/b.png"]);
    let bucket = BucketName::new("example-bucket");

    let first_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: Some(ObjectKey::new("images/")),
                continuation_token: None,
                max_keys: 1,
            },
        )
        .expect("list first prefixed page");

    assert_eq!(object_keys(&first_page.objects), ["images/a.png"]);
    assert!(first_page.is_truncated);
    let expected_token = continuation_token_for("images/b.png");
    assert_eq!(
        first_page.next_continuation_token.as_deref(),
        Some(expected_token.as_str())
    );

    let second_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: Some(ObjectKey::new("images/")),
                continuation_token: first_page.next_continuation_token,
                max_keys: 1,
            },
        )
        .expect("list second prefixed page");

    assert_eq!(object_keys(&second_page.objects), ["images/b.png"]);
    assert!(!second_page.is_truncated);
}

#[test]
fn list_objects_max_keys_zero_returns_no_objects_without_continuation() {
    let (_temp_dir, storage) = storage_with_objects(["b.txt", "a.txt"]);
    let bucket = BucketName::new("example-bucket");

    let listing = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: None,
                continuation_token: None,
                max_keys: 0,
            },
        )
        .expect("list zero max keys");

    assert!(listing.objects.is_empty());
    assert_eq!(listing.max_keys, 0);
    assert!(!listing.is_truncated);
    assert_eq!(listing.next_continuation_token, None);
}

#[test]
fn list_objects_rejects_malformed_continuation_tokens() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");

    for token in [
        "token",
        "s3lab-v1:",
        "s3lab-v1:0",
        "s3lab-v1:5A2E747874",
        "s3lab-v1:gg",
        "s3lab-v1:c328",
    ] {
        let error = storage
            .list_objects(
                &bucket,
                ListObjectsOptions {
                    prefix: None,
                    continuation_token: Some(token.to_owned()),
                    max_keys: 1000,
                },
            )
            .expect_err("malformed continuation token rejected");

        assert!(
            matches!(error, StorageError::InvalidArgument { .. }),
            "token should be invalid: {token}"
        );
    }
}

#[test]
fn list_objects_resumes_at_next_key_when_token_key_was_deleted() {
    let (_temp_dir, storage) = storage_with_objects(["a.txt", "b.txt", "c.txt"]);
    let bucket = BucketName::new("example-bucket");

    storage
        .delete_object(&bucket, &ObjectKey::new("b.txt"))
        .expect("delete token key");

    let listing = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: None,
                continuation_token: Some(continuation_token_for("b.txt")),
                max_keys: 1000,
            },
        )
        .expect("list after deleted token key");

    assert_eq!(object_keys(&listing.objects), ["c.txt"]);
    assert!(!listing.is_truncated);
}

fn storage() -> (TempDir, FilesystemStorage<FixedClock>) {
    let temp_dir = TempDir::new().expect("temp dir");
    let storage =
        FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
    (temp_dir, storage)
}

fn storage_with_objects<const N: usize>(
    keys: [&str; N],
) -> (TempDir, FilesystemStorage<FixedClock>) {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    for key in keys {
        storage
            .put_object(put_request(&bucket, key, key.as_bytes()))
            .expect("put object");
    }
    (temp_dir, storage)
}

fn put_request(bucket: &BucketName, key: &str, bytes: &[u8]) -> PutObjectRequest {
    PutObjectRequest {
        bucket: bucket.clone(),
        key: ObjectKey::new(key),
        bytes: bytes.to_vec(),
        content_type: None,
        user_metadata: BTreeMap::new(),
    }
}

fn assert_metadata_matches(
    metadata: &StoredObjectMetadata,
    bucket: &BucketName,
    key: &str,
    content_length: u64,
) {
    assert_eq!(&metadata.bucket, bucket);
    assert_eq!(metadata.key.as_str(), key);
    assert_eq!(metadata.content_length, content_length);
    assert!(metadata.etag.starts_with('"'));
    assert!(metadata.etag.ends_with('"'));
}

fn object_keys(metadata: &[StoredObjectMetadata]) -> Vec<&str> {
    metadata
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>()
}

fn continuation_token_for(key: &str) -> String {
    format!("s3lab-v1:{}", lower_hex(key.as_bytes()))
}

fn lower_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn filesystem_path_components(root: &Path) -> Vec<String> {
    let mut components = Vec::new();
    collect_path_components(root, &mut components);
    components.sort();
    components
}

fn collect_path_components(path: &Path, components: &mut Vec<String>) {
    for entry in fs::read_dir(path).expect("read directory") {
        let entry = entry.expect("read entry");
        components.push(entry.file_name().to_string_lossy().into_owned());
        if entry.file_type().expect("entry type").is_dir() {
            collect_path_components(&entry.path(), components);
        }
    }
}

fn write_bucket_metadata(root: &Path, bucket: &BucketName, json: &str) {
    fs::write(bucket_dir(root, bucket).join("bucket.json"), json).expect("write bucket metadata");
}

fn rewrite_object_metadata_field(
    root: &Path,
    bucket: &BucketName,
    key: &ObjectKey,
    field: &str,
    value: serde_json::Value,
) {
    let metadata_path = object_paths(root, bucket, key).metadata;
    let mut metadata = serde_json::from_slice::<serde_json::Value>(
        &fs::read(&metadata_path).expect("read object metadata"),
    )
    .expect("parse object metadata");
    metadata
        .as_object_mut()
        .expect("object metadata is a json object")
        .insert(field.to_owned(), value);
    fs::write(
        metadata_path,
        serde_json::to_vec_pretty(&metadata).expect("serialize object metadata"),
    )
    .expect("write object metadata");
}

fn bucket_dir(root: &Path, bucket: &BucketName) -> PathBuf {
    root.join(STORAGE_ROOT_DIR)
        .join(encode_bucket_name(bucket).as_path_component())
}

fn object_paths(root: &Path, bucket: &BucketName, key: &ObjectKey) -> ObjectPaths {
    let encoded_key = encode_object_key(key).expect("valid key");
    let object_dir = bucket_dir(root, bucket)
        .join("objects")
        .join(encoded_key.shard())
        .join(encoded_key.as_path_component());
    ObjectPaths {
        dir: object_dir.clone(),
        content: object_dir.join("content.bin"),
        metadata: object_dir.join("metadata.json"),
    }
}

struct ObjectPaths {
    dir: PathBuf,
    content: PathBuf,
    metadata: PathBuf,
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
        Time::from_hms_nano(12, 34, 56, 123_456_789).expect("valid test time"),
    )
    .assume_utc()
}
