// SPDX-License-Identifier: Apache-2.0

use md5::Md5;
use s3lab::s3::bucket::BucketName;
use s3lab::s3::object::{ObjectKey, MAX_OBJECT_KEY_UTF8_BYTES};
use s3lab::storage::fs::{FilesystemStorage, StorageClock};
use s3lab::storage::{
    CompleteMultipartUploadRequest, CompletedMultipartPart, CreateMultipartUploadRequest,
    ListObjectsOptions, ObjectListingEntry, PutObjectRequest, Storage, StorageError,
    StoredObjectMetadata, UploadPartRequest, STORAGE_ROOT_DIR,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io;
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
fn create_bucket_rejects_invalid_bucket_names_without_storing_them() {
    for name in [
        "ab",
        "Uppercase",
        "bad_bucket",
        "bucket..name",
        "bucket.-name",
        "bucket-.name",
        "192.168.0.1",
        "bucket/name",
        "xn--bucket",
        "sthree-bucket",
        "amzn-s3-demo-bucket",
        "bucket-s3alias",
        "bucket--ol-s3",
        "bucket.mrap",
        "bucket--x-s3",
        "bucket--table-s3",
    ] {
        let (temp_dir, storage) = storage();
        let bucket = BucketName::new(name);

        let error = storage
            .create_bucket(&bucket)
            .expect_err("invalid bucket create fails");

        assert!(matches!(
            error,
            StorageError::InvalidBucketName { bucket: failed_bucket }
                if failed_bucket == name
        ));
        assert!(
            !temp_dir.path().join(STORAGE_ROOT_DIR).exists(),
            "invalid bucket should not create storage root: {name}"
        );
    }
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
fn list_buckets_rejects_invalid_bucket_name_metadata_even_when_path_matches() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("bad_bucket");
    write_bucket_state(temp_dir.path(), &bucket);

    let error = storage
        .list_buckets()
        .expect_err("invalid persisted bucket name fails");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn direct_bucket_operations_reject_invalid_bucket_name_metadata_even_when_path_matches() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("bad_bucket");

    for operation in ["bucket_exists", "delete_bucket", "put_object"] {
        write_bucket_state(temp_dir.path(), &bucket);
        let error = match operation {
            "bucket_exists" => storage
                .bucket_exists(&bucket)
                .map(|_| ())
                .expect_err("bucket_exists rejects invalid persisted bucket name"),
            "delete_bucket" => storage
                .delete_bucket(&bucket)
                .expect_err("delete_bucket rejects invalid persisted bucket name"),
            "put_object" => storage
                .put_object(put_request(&bucket, "object.txt", b"body"))
                .map(|_| ())
                .expect_err("put_object rejects invalid persisted bucket name"),
            _ => unreachable!("covered operation"),
        };

        assert!(
            matches!(error, StorageError::CorruptState { .. }),
            "{operation} should report invalid persisted bucket name as corrupt state"
        );
    }
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
fn create_bucket_rejects_existing_bucket_directory_with_mismatched_metadata() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    fs::create_dir_all(bucket_dir(temp_dir.path(), &bucket)).expect("create bucket dir");
    write_bucket_metadata(temp_dir.path(), &bucket, r#"{"bucket":"other-bucket"}"#);

    let error = storage
        .create_bucket(&bucket)
        .expect_err("mismatched existing bucket metadata fails create");

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
fn delete_bucket_ignores_hidden_leftovers_and_empty_shard_dirs() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");

    let objects_dir = bucket_dir(temp_dir.path(), &bucket).join("objects");
    fs::create_dir_all(
        objects_dir
            .join(".staged-shard.tmp-1")
            .join("incomplete-object"),
    )
    .expect("create hidden staged shard");
    fs::create_dir_all(objects_dir.join("ab")).expect("create empty visible shard");
    fs::create_dir_all(objects_dir.join("cd").join(".object-dir.tmp-1"))
        .expect("create hidden staged object dir");
    fs::write(objects_dir.join(".metadata.json.bak-1"), b"backup")
        .expect("write hidden backup file");

    assert!(storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect("hidden leftovers are not listed")
        .objects
        .is_empty());

    storage
        .delete_bucket(&bucket)
        .expect("delete bucket with only hidden leftovers");

    assert!(!bucket_dir(temp_dir.path(), &bucket).exists());
}

#[test]
fn delete_bucket_preserves_bucket_not_empty_for_committed_object_with_hidden_leftovers() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, "object.txt", b"body"))
        .expect("put object");

    let objects_dir = bucket_dir(temp_dir.path(), &bucket).join("objects");
    fs::create_dir_all(
        objects_dir
            .join(".staged-shard.tmp-1")
            .join("incomplete-object"),
    )
    .expect("create hidden staged shard");
    fs::create_dir_all(objects_dir.join("ab")).expect("create empty visible shard");

    let error = storage
        .delete_bucket(&bucket)
        .expect_err("committed object keeps bucket non-empty");

    assert!(matches!(
        error,
        StorageError::BucketNotEmpty { bucket: failed_bucket }
            if failed_bucket == bucket
    ));
}

#[test]
fn delete_bucket_reports_visible_object_metadata_corruption() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");
    fs::remove_file(object_paths(temp_dir.path(), &bucket, &key).metadata)
        .expect("remove visible object metadata");

    let error = storage
        .delete_bucket(&bucket)
        .expect_err("visible object metadata corruption is not ignored");

    assert!(matches!(error, StorageError::CorruptState { .. }));
    assert!(bucket_dir(temp_dir.path(), &bucket).exists());
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
    let first_shard = object_key_shard(&first_key);
    let sibling_key = (0..10_000)
        .map(|index| ObjectKey::new(format!("sibling-{index}.txt")))
        .find(|candidate| object_key_shard(candidate) == first_shard)
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
fn put_object_accepts_path_like_key_at_utf8_byte_limit() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = "nested/".to_owned() + &"a".repeat(MAX_OBJECT_KEY_UTF8_BYTES - "nested/".len());
    storage.create_bucket(&bucket).expect("create bucket");

    let metadata = storage
        .put_object(put_request(&bucket, &key, b"body"))
        .expect("put key at byte limit");

    assert_metadata_matches(&metadata, &bucket, &key, 4);
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &ObjectKey::new(&key))
            .expect("get object at byte limit"),
        b"body"
    );
}

#[test]
fn put_object_rejects_keys_over_utf8_byte_limit_without_storing_them() {
    for key in [
        "a".repeat(MAX_OBJECT_KEY_UTF8_BYTES + 1),
        "é".repeat((MAX_OBJECT_KEY_UTF8_BYTES / 2) + 1),
    ] {
        let (_temp_dir, storage) = storage();
        let bucket = BucketName::new("example-bucket");
        storage.create_bucket(&bucket).expect("create bucket");

        let error = storage
            .put_object(put_request(&bucket, &key, b"body"))
            .expect_err("oversized key is invalid");

        assert!(matches!(error, StorageError::InvalidObjectKey { .. }));
        assert!(
            storage
                .list_objects(&bucket, ListObjectsOptions::default())
                .expect("list objects after rejected put")
                .objects
                .is_empty(),
            "oversized key should not create a listed object"
        );
    }
}

#[test]
fn put_object_rejects_xml_invalid_control_characters_without_storing_them() {
    for key in ["prefix/\0object.txt", "prefix/\u{1F}object.txt"] {
        let (_temp_dir, storage) = storage();
        let bucket = BucketName::new("example-bucket");
        storage.create_bucket(&bucket).expect("create bucket");

        let error = storage
            .put_object(put_request(&bucket, key, b"body"))
            .expect_err("XML-invalid key is invalid");

        assert!(matches!(error, StorageError::InvalidObjectKey { .. }));
        assert!(
            storage
                .list_objects(&bucket, ListObjectsOptions::default())
                .expect("list objects after rejected put")
                .objects
                .is_empty(),
            "XML-invalid key should not create a listed object"
        );
    }
}

#[test]
fn put_and_list_objects_accept_carriage_return_in_keys() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = "prefix/\robject.txt";
    storage.create_bucket(&bucket).expect("create bucket");

    let metadata = storage
        .put_object(put_request(&bucket, key, b"body"))
        .expect("put object with CR key");

    assert_metadata_matches(&metadata, &bucket, key, 4);
    let listing = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect("list object with CR key");
    assert_eq!(object_keys(&listing.objects), [key]);
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &ObjectKey::new(key))
            .expect("get object with CR key"),
        b"body"
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
fn list_objects_rejects_object_metadata_with_wrong_bucket_identity() {
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
        "bucket",
        serde_json::json!("other-bucket"),
    );

    let error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list rejects object metadata with wrong bucket");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn list_objects_rejects_object_metadata_with_wrong_key_path_identity() {
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
        "key",
        serde_json::json!("other-object.txt"),
    );

    let error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list rejects object metadata stored under the wrong key path");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn direct_object_reads_reject_metadata_with_wrong_requested_identity() {
    for (field, value) in [
        ("bucket", serde_json::json!("other-bucket")),
        ("key", serde_json::json!("other-object.txt")),
    ] {
        let (temp_dir, storage) = storage();
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");
        storage.create_bucket(&bucket).expect("create bucket");
        storage
            .put_object(put_request(&bucket, key.as_str(), b"body"))
            .expect("put object");
        rewrite_object_metadata_field(temp_dir.path(), &bucket, &key, field, value);

        let object_error = storage
            .get_object(&bucket, &key)
            .expect_err("direct object read rejects mismatched metadata identity");
        let metadata_error = storage
            .get_object_metadata(&bucket, &key)
            .expect_err("direct metadata read rejects mismatched metadata identity");

        assert!(
            matches!(object_error, StorageError::CorruptState { .. }),
            "{field} mismatch should corrupt direct object read"
        );
        assert!(
            matches!(metadata_error, StorageError::CorruptState { .. }),
            "{field} mismatch should corrupt direct metadata read"
        );
    }
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
fn symlink_object_content_is_corrupt_state_for_reads_writes_and_deletes() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");

    let object_paths = object_paths(temp_dir.path(), &bucket, &key);
    let external_content = temp_dir.path().join("outside-content.bin");
    fs::write(&external_content, b"body").expect("write external content");
    fs::remove_file(&object_paths.content).expect("remove stored content");
    if let Err(skip) = create_file_symlink_or_skip(&external_content, &object_paths.content) {
        println!("{skip}");
        return;
    }

    let metadata_error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("metadata rejects symlinked content");
    let list_error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list rejects symlinked content");
    let put_error = storage
        .put_object(put_request(&bucket, key.as_str(), b"new body"))
        .expect_err("put rejects symlinked existing content");
    let delete_error = storage
        .delete_object(&bucket, &key)
        .expect_err("delete rejects symlinked object content");

    assert!(matches!(metadata_error, StorageError::CorruptState { .. }));
    assert!(matches!(list_error, StorageError::CorruptState { .. }));
    assert!(matches!(put_error, StorageError::CorruptState { .. }));
    assert!(matches!(delete_error, StorageError::CorruptState { .. }));
    assert_eq!(
        fs::read(&external_content).expect("read external content"),
        b"body"
    );
}

#[test]
fn symlink_object_directory_is_corrupt_state_for_reads_writes_and_deletes() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");

    let object_paths = object_paths(temp_dir.path(), &bucket, &key);
    let external_object_dir = temp_dir.path().join("outside-object-dir");
    fs::rename(&object_paths.dir, &external_object_dir).expect("move object dir outside storage");
    if let Err(skip) = create_dir_symlink_or_skip(&external_object_dir, &object_paths.dir) {
        println!("{skip}");
        return;
    }

    let bytes_error = storage
        .get_object_bytes(&bucket, &key)
        .expect_err("bytes read rejects symlinked object dir");
    let list_error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list rejects symlinked object dir");
    let put_error = storage
        .put_object(put_request(&bucket, key.as_str(), b"new body"))
        .expect_err("put rejects symlinked object dir");
    let delete_error = storage
        .delete_object(&bucket, &key)
        .expect_err("delete rejects symlinked object dir");

    assert!(matches!(bytes_error, StorageError::CorruptState { .. }));
    assert!(matches!(list_error, StorageError::CorruptState { .. }));
    assert!(matches!(put_error, StorageError::CorruptState { .. }));
    assert!(matches!(delete_error, StorageError::CorruptState { .. }));
    assert!(external_object_dir.join("content.bin").is_file());
    assert!(external_object_dir.join("metadata.json").is_file());
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
fn create_multipart_upload_persists_initiation_metadata_with_deterministic_id() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("large.bin");
    storage.create_bucket(&bucket).expect("create bucket");

    let upload = storage
        .create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            user_metadata: BTreeMap::from([("owner".to_owned(), "local".to_owned())]),
        })
        .expect("create multipart upload");

    assert_eq!(upload.bucket, bucket);
    assert_eq!(upload.key, key);
    assert!(upload.upload_id.starts_with("upload-"));
    assert!(upload.upload_id.ends_with("-000000000001"));
    assert_eq!(upload.initiated, fixed_time());
    assert_eq!(
        upload.content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(upload.user_metadata["owner"], "local");
    assert!(filesystem_path_components(temp_dir.path())
        .iter()
        .any(|component| component == "multipart"));

    let reopened =
        FilesystemStorage::with_clock(temp_dir.path().to_path_buf(), FixedClock(fixed_time()));
    assert_eq!(
        reopened
            .list_parts(&bucket, &ObjectKey::new("large.bin"), &upload.upload_id)
            .expect("persisted multipart upload is readable")
            .upload,
        upload
    );
}

#[test]
fn upload_part_overwrites_and_list_parts_returns_sorted_parts() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("large.bin");
    storage.create_bucket(&bucket).expect("create bucket");
    let upload = storage
        .create_multipart_upload(create_multipart_request(&bucket, key.as_str()))
        .expect("create multipart upload");

    let part_three = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            3,
            b"three",
        ))
        .expect("upload part 3");
    let part_one = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            1,
            b"one",
        ))
        .expect("upload part 1");
    let overwritten = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            3,
            b"THREE",
        ))
        .expect("overwrite part 3");

    assert_ne!(part_three.etag, overwritten.etag);
    assert_eq!(overwritten.content_length, 5);
    assert_eq!(
        storage
            .list_parts(&bucket, &key, &upload.upload_id)
            .expect("list parts")
            .parts
            .iter()
            .map(|part| (part.part_number, part.etag.as_str(), part.content_length))
            .collect::<Vec<_>>(),
        vec![
            (1, part_one.etag.as_str(), 3),
            (3, overwritten.etag.as_str(), 5)
        ]
    );
    assert!(blob_path(temp_dir.path(), b"one").is_file());
    assert!(blob_path(temp_dir.path(), b"THREE").is_file());
}

#[test]
fn complete_multipart_upload_creates_normal_object_and_removes_active_upload() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("large.bin");
    storage.create_bucket(&bucket).expect("create bucket");
    let upload = storage
        .create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            user_metadata: BTreeMap::from([("owner".to_owned(), "local".to_owned())]),
        })
        .expect("create multipart upload");
    let first = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            1,
            b"hello ",
        ))
        .expect("upload first part");
    let second = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            2,
            b"world",
        ))
        .expect("upload second part");

    let completed = storage
        .complete_multipart_upload(CompleteMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload.upload_id.clone(),
            parts: vec![
                completed_part(1, &first.etag),
                completed_part(2, &second.etag),
            ],
        })
        .expect("complete multipart upload");

    assert_metadata_matches(&completed, &bucket, key.as_str(), 11);
    assert_eq!(
        completed.etag,
        multipart_etag_for([b"hello ".as_slice(), b"world".as_slice()])
    );
    assert_eq!(
        completed.content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(completed.user_metadata["owner"], "local");
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &key)
            .expect("completed object is readable"),
        b"hello world"
    );
    assert!(matches!(
        storage
            .list_parts(&bucket, &key, &upload.upload_id)
            .expect_err("completed upload is removed"),
        StorageError::InvalidArgument { .. }
    ));
}

#[test]
fn completed_multipart_object_detects_same_length_content_corruption() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("large.bin");
    storage.create_bucket(&bucket).expect("create bucket");
    let upload = storage
        .create_multipart_upload(create_multipart_request(&bucket, key.as_str()))
        .expect("create multipart upload");
    let first = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            1,
            b"hello ",
        ))
        .expect("upload first part");
    let second = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            2,
            b"world",
        ))
        .expect("upload second part");
    storage
        .complete_multipart_upload(CompleteMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload.upload_id,
            parts: vec![
                completed_part(1, &first.etag),
                completed_part(2, &second.etag),
            ],
        })
        .expect("complete multipart upload");

    fs::write(
        object_paths(temp_dir.path(), &bucket, &key).content,
        b"HELLO WORLD",
    )
    .expect("mutate completed object content to same length");

    let object_error = storage
        .get_object(&bucket, &key)
        .expect_err("object read rejects same-length corruption");
    let metadata_error = storage
        .get_object_metadata(&bucket, &key)
        .expect_err("metadata read rejects same-length corruption");
    let list_error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("list rejects same-length corruption");

    assert!(matches!(object_error, StorageError::CorruptState { .. }));
    assert!(matches!(metadata_error, StorageError::CorruptState { .. }));
    assert!(matches!(list_error, StorageError::CorruptState { .. }));
}

#[test]
fn multipart_upload_rejects_invalid_part_number() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("large.bin");
    storage.create_bucket(&bucket).expect("create bucket");
    let upload = storage
        .create_multipart_upload(create_multipart_request(&bucket, key.as_str()))
        .expect("create multipart upload");

    let error = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            0,
            b"part",
        ))
        .expect_err("part number zero is invalid");

    assert!(matches!(error, StorageError::InvalidArgument { .. }));
}

#[test]
fn complete_multipart_upload_rejects_missing_wrong_duplicate_and_unsorted_parts() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("large.bin");
    storage.create_bucket(&bucket).expect("create bucket");
    let upload = storage
        .create_multipart_upload(create_multipart_request(&bucket, key.as_str()))
        .expect("create multipart upload");
    let first = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            1,
            b"one",
        ))
        .expect("upload first part");
    let second = storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            2,
            b"two",
        ))
        .expect("upload second part");

    for parts in [
        vec![completed_part(3, "\"missing\"")],
        vec![completed_part(1, "\"bad-etag\"")],
        vec![
            completed_part(1, &first.etag),
            completed_part(1, &first.etag),
        ],
        vec![
            completed_part(2, &second.etag),
            completed_part(1, &first.etag),
        ],
    ] {
        let error = storage
            .complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload.upload_id.clone(),
                parts,
            })
            .expect_err("invalid completed part list fails");

        assert!(matches!(error, StorageError::InvalidArgument { .. }));
        assert!(matches!(
            storage
                .get_object_metadata(&bucket, &key)
                .expect_err("failed completion does not create object"),
            StorageError::NoSuchKey { .. }
        ));
    }
}

#[test]
fn abort_multipart_upload_removes_active_upload_without_creating_object() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("large.bin");
    storage.create_bucket(&bucket).expect("create bucket");
    let upload = storage
        .create_multipart_upload(create_multipart_request(&bucket, key.as_str()))
        .expect("create multipart upload");
    storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            1,
            b"part",
        ))
        .expect("upload part");

    storage
        .abort_multipart_upload(&bucket, &key, &upload.upload_id)
        .expect("abort multipart upload");

    assert!(matches!(
        storage
            .list_parts(&bucket, &key, &upload.upload_id)
            .expect_err("aborted upload is removed"),
        StorageError::InvalidArgument { .. }
    ));
    assert!(matches!(
        storage
            .get_object_metadata(&bucket, &key)
            .expect_err("abort does not create object"),
        StorageError::NoSuchKey { .. }
    ));
}

#[test]
fn active_multipart_uploads_do_not_affect_object_list_read_or_head() {
    let (_temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("large.bin");
    storage.create_bucket(&bucket).expect("create bucket");
    let upload = storage
        .create_multipart_upload(create_multipart_request(&bucket, key.as_str()))
        .expect("create multipart upload");
    storage
        .upload_part(upload_part_request(
            &bucket,
            &key,
            &upload.upload_id,
            1,
            b"part",
        ))
        .expect("upload part");

    assert!(storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect("list objects")
        .objects
        .is_empty());
    assert!(matches!(
        storage
            .get_object_bytes(&bucket, &key)
            .expect_err("active upload is not readable as object"),
        StorageError::NoSuchKey { .. }
    ));
    assert!(matches!(
        storage
            .get_object_metadata(&bucket, &key)
            .expect_err("active upload has no object metadata"),
        StorageError::NoSuchKey { .. }
    ));
}

#[test]
fn delete_bucket_with_active_multipart_upload_is_deterministic() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("large.bin");
    storage.create_bucket(&bucket).expect("create bucket");
    let upload = storage
        .create_multipart_upload(create_multipart_request(&bucket, key.as_str()))
        .expect("create multipart upload");

    let error = storage
        .delete_bucket(&bucket)
        .expect_err("active multipart upload keeps bucket non-empty");

    assert!(matches!(
        error,
        StorageError::BucketNotEmpty { bucket: failed_bucket }
            if failed_bucket == bucket
    ));
    assert!(bucket_dir(temp_dir.path(), &bucket).is_dir());

    storage
        .abort_multipart_upload(&bucket, &key, &upload.upload_id)
        .expect("abort multipart upload");
    storage
        .delete_bucket(&bucket)
        .expect("bucket delete succeeds after abort");
    assert!(!bucket_dir(temp_dir.path(), &bucket).exists());
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
fn list_objects_reports_visible_non_directory_entries_under_objects_as_corrupt_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"body"))
        .expect("put object");

    let objects_dir = bucket_dir(temp_dir.path(), &bucket).join("objects");
    fs::write(objects_dir.join("not-a-shard"), b"plain file").expect("write non-shard file");

    let error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("visible non-directory under objects is corrupt state");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn list_objects_reports_visible_non_directory_entries_under_shards_as_corrupt_state() {
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
    fs::write(shard_dir.join("not-an-object"), b"plain file").expect("write non-object file");

    let error = storage
        .list_objects(&bucket, ListObjectsOptions::default())
        .expect_err("visible non-directory under shard is corrupt state");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn delete_bucket_reports_visible_non_directory_entries_under_objects_as_corrupt_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");

    let objects_dir = bucket_dir(temp_dir.path(), &bucket).join("objects");
    fs::write(objects_dir.join("not-a-shard"), b"plain file").expect("write non-shard file");

    let error = storage
        .delete_bucket(&bucket)
        .expect_err("visible non-directory under objects is corrupt state");

    assert!(matches!(error, StorageError::CorruptState { .. }));
    assert!(bucket_dir(temp_dir.path(), &bucket).exists());
}

#[test]
fn delete_bucket_reports_visible_non_directory_entries_under_shards_as_corrupt_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");

    let objects_dir = bucket_dir(temp_dir.path(), &bucket).join("objects");
    let shard_dir = objects_dir.join("ab");
    fs::create_dir_all(&shard_dir).expect("create visible shard");
    fs::write(shard_dir.join("not-an-object"), b"plain file").expect("write non-object file");

    let error = storage
        .delete_bucket(&bucket)
        .expect_err("visible non-directory under shard is corrupt state");

    assert!(matches!(error, StorageError::CorruptState { .. }));
    assert!(bucket_dir(temp_dir.path(), &bucket).exists());
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
fn list_objects_rejects_xml_invalid_prefix() {
    let (_temp_dir, storage) = storage_with_objects(["images/a.png", "logs/a.txt"]);
    let bucket = BucketName::new("example-bucket");

    for prefix in ["images/\0", "images/\u{1F}"] {
        let error = storage
            .list_objects(
                &bucket,
                ListObjectsOptions {
                    prefix: Some(ObjectKey::new(prefix)),
                    ..ListObjectsOptions::default()
                },
            )
            .expect_err("XML-invalid prefix is rejected");

        assert!(matches!(error, StorageError::InvalidArgument { .. }));
        assert!(error.to_string().contains("prefix"));
    }
}

#[test]
fn list_objects_accepts_carriage_return_prefix() {
    let (_temp_dir, storage) = storage_with_objects(["images/\ra.png", "images/b.png"]);
    let bucket = BucketName::new("example-bucket");

    let listing = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: Some(ObjectKey::new("images/\r")),
                ..ListObjectsOptions::default()
            },
        )
        .expect("list objects with CR prefix");

    assert_eq!(object_keys(&listing.objects), ["images/\ra.png"]);
}

#[test]
fn list_objects_groups_common_prefixes_with_prefix_and_delimiter() {
    let (_temp_dir, storage) = storage_with_objects([
        "photos/z.txt",
        "photos/2026/a.jpg",
        "logs/a.txt",
        "photos/root.txt",
        "photos/2025/a.jpg",
        "photos/2026/b.jpg",
    ]);
    let bucket = BucketName::new("example-bucket");

    let listing = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: Some(ObjectKey::new("photos/")),
                delimiter: Some("/".to_owned()),
                ..ListObjectsOptions::default()
            },
        )
        .expect("list objects with prefix and delimiter");

    assert_eq!(
        listing_entry_markers(&listing.entries),
        [
            "prefix:photos/2025/",
            "prefix:photos/2026/",
            "object:photos/root.txt",
            "object:photos/z.txt",
        ]
        .map(str::to_owned)
    );
    assert_eq!(
        object_keys(&listing.objects),
        ["photos/root.txt", "photos/z.txt"]
    );
    assert_eq!(
        object_key_values(&listing.common_prefixes),
        ["photos/2025/", "photos/2026/"]
    );
    assert!(!listing.is_truncated);
}

#[test]
fn list_objects_sorts_and_deduplicates_common_prefixes() {
    let (_temp_dir, storage) =
        storage_with_objects(["b/2.txt", "a", "a/2.txt", "b/1.txt", "a/1.txt"]);
    let bucket = BucketName::new("example-bucket");

    let listing = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                delimiter: Some("/".to_owned()),
                ..ListObjectsOptions::default()
            },
        )
        .expect("list objects with delimiter");

    assert_eq!(
        listing_entry_markers(&listing.entries),
        ["object:a", "prefix:a/", "prefix:b/"].map(str::to_owned)
    );
    assert_eq!(object_keys(&listing.objects), ["a"]);
    assert_eq!(object_key_values(&listing.common_prefixes), ["a/", "b/"]);
}

#[test]
fn list_objects_delimiter_pagination_counts_prefixes_and_objects() {
    let (_temp_dir, storage) = storage_with_objects(["d/1.txt", "a.txt", "c.txt", "b/1.txt"]);
    let bucket = BucketName::new("example-bucket");

    let first_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                delimiter: Some("/".to_owned()),
                continuation_token: None,
                max_keys: 2,
                ..ListObjectsOptions::default()
            },
        )
        .expect("list first delimiter page");

    assert_eq!(
        listing_entry_markers(&first_page.entries),
        ["object:a.txt", "prefix:b/"].map(str::to_owned)
    );
    assert_eq!(
        first_page.objects.len() + first_page.common_prefixes.len(),
        2
    );
    assert!(first_page.is_truncated);
    assert_eq!(
        first_page.next_continuation_token.as_deref(),
        Some(continuation_token_for("c.txt").as_str())
    );

    let second_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                delimiter: Some("/".to_owned()),
                continuation_token: first_page.next_continuation_token,
                max_keys: 2,
                ..ListObjectsOptions::default()
            },
        )
        .expect("list second delimiter page");

    assert_eq!(
        listing_entry_markers(&second_page.entries),
        ["object:c.txt", "prefix:d/"].map(str::to_owned)
    );
    assert!(!second_page.is_truncated);
}

#[test]
fn list_objects_max_keys_zero_with_delimiter_returns_next_visible_entry_token() {
    let (_temp_dir, storage) = storage_with_objects(["b.txt", "a/1.txt"]);
    let bucket = BucketName::new("example-bucket");

    let first_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                delimiter: Some("/".to_owned()),
                continuation_token: None,
                max_keys: 0,
                ..ListObjectsOptions::default()
            },
        )
        .expect("list first zero max keys delimiter page");

    assert!(first_page.entries.is_empty());
    assert!(first_page.objects.is_empty());
    assert!(first_page.common_prefixes.is_empty());
    assert!(first_page.is_truncated);
    assert_eq!(
        first_page.next_continuation_token.as_deref(),
        Some(continuation_token_for("a/").as_str())
    );

    let second_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                delimiter: Some("/".to_owned()),
                continuation_token: first_page.next_continuation_token,
                max_keys: 0,
                ..ListObjectsOptions::default()
            },
        )
        .expect("list second zero max keys delimiter page");

    assert!(second_page.entries.is_empty());
    assert!(second_page.is_truncated);
    assert_eq!(
        second_page.next_continuation_token.as_deref(),
        Some(continuation_token_for("b.txt").as_str())
    );
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
            .join(object_key_shard(&key))
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

    fs::create_dir_all(
        bucket_dir(temp_dir.path(), &bucket)
            .join("objects")
            .join(object_key_shard(&key))
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

    let expected_object_dir = temp_dir
        .path()
        .join(STORAGE_ROOT_DIR)
        .join(encoded_bucket_path_component(&bucket))
        .join("objects")
        .join(object_key_shard(&key))
        .join(encoded_object_key_path_component(&key));

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
fn snapshot_restore_returns_deterministic_saved_state_and_keeps_storage_usable() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, "b.txt", b"baseline-b"))
        .expect("put baseline b");
    storage
        .put_object(put_request(&bucket, "a.txt", b"baseline-a"))
        .expect("put baseline a");

    storage.save_snapshot("baseline.v1").expect("save snapshot");
    let baseline_journal =
        fs::read(temp_dir.path().join("events").join("journal.jsonl")).expect("read journal");

    storage
        .put_object(put_request(&bucket, "b.txt", b"changed-b"))
        .expect("overwrite object after snapshot");
    storage
        .create_bucket(&BucketName::new("other-bucket"))
        .expect("create extra bucket after snapshot");

    storage
        .restore_snapshot("baseline.v1")
        .expect("restore snapshot");

    assert_eq!(
        bucket_names(&storage.list_buckets().expect("list restored buckets")),
        ["example-bucket"]
    );
    assert_eq!(
        object_keys(
            &storage
                .list_objects(&bucket, ListObjectsOptions::default())
                .expect("list restored objects")
                .objects
        ),
        ["a.txt", "b.txt"]
    );
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &ObjectKey::new("b.txt"))
            .expect("read restored object"),
        b"baseline-b"
    );
    assert_eq!(
        fs::read(temp_dir.path().join("events").join("journal.jsonl"))
            .expect("read restored journal"),
        baseline_journal
    );

    let first_restore_components = filesystem_path_components(temp_dir.path());
    storage
        .put_object(put_request(&bucket, "c.txt", b"new-after-restore"))
        .expect("storage remains writable after restore");
    storage
        .restore_snapshot("baseline.v1")
        .expect("restore snapshot again");

    assert_eq!(
        filesystem_path_components(temp_dir.path()),
        first_restore_components
    );
    assert!(matches!(
        storage
            .get_object_metadata(&bucket, &ObjectKey::new("c.txt"))
            .expect_err("second restore removes post-restore object"),
        StorageError::NoSuchKey { .. }
    ));
}

#[test]
fn save_snapshot_rejects_overwriting_existing_snapshot() {
    let (_temp_dir, storage) = storage();

    storage.save_snapshot("baseline").expect("save snapshot");
    let error = storage
        .save_snapshot("baseline")
        .expect_err("snapshot overwrite is rejected");

    assert!(matches!(error, StorageError::InvalidArgument { .. }));
    assert!(error.to_string().contains("snapshot already exists"));
}

#[test]
fn restore_snapshot_reports_missing_snapshot_as_invalid_argument() {
    let (_temp_dir, storage) = storage();

    let error = storage
        .restore_snapshot("missing")
        .expect_err("missing snapshot fails");

    assert!(matches!(error, StorageError::InvalidArgument { .. }));
    assert!(error.to_string().contains("snapshot does not exist"));
}

#[test]
fn restore_snapshot_rejects_incomplete_snapshot_before_current_state_changes() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"snapshot-body"))
        .expect("put snapshot object");
    storage.save_snapshot("baseline").expect("save snapshot");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"current-body"))
        .expect("put current object");
    fs::remove_dir_all(
        temp_dir
            .path()
            .join("snapshots")
            .join("baseline")
            .join(STORAGE_ROOT_DIR),
    )
    .expect("remove manifest-declared snapshot buckets");

    let error = storage
        .restore_snapshot("baseline")
        .expect_err("incomplete snapshot is rejected");

    assert!(matches!(error, StorageError::CorruptState { .. }));
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &key)
            .expect("current object remains readable"),
        b"current-body"
    );
    assert_eq!(
        bucket_names(&storage.list_buckets().expect("list current buckets")),
        ["example-bucket"]
    );
}

#[test]
fn restore_snapshot_rejects_internally_corrupt_snapshot_before_current_state_changes() {
    for corruption in [
        SnapshotCorruption::MissingBucketMetadata,
        SnapshotCorruption::CorruptBucketMetadata,
        SnapshotCorruption::MissingObjectMetadata,
        SnapshotCorruption::CorruptObjectMetadata,
        SnapshotCorruption::MissingObjectContent,
        SnapshotCorruption::CorruptObjectContent,
    ] {
        let (temp_dir, storage) = storage();
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");
        storage.create_bucket(&bucket).expect("create bucket");
        storage
            .put_object(put_request(&bucket, key.as_str(), b"snapshot-body"))
            .expect("put snapshot object");
        storage.save_snapshot("baseline").expect("save snapshot");
        storage
            .put_object(put_request(&bucket, key.as_str(), b"current-body"))
            .expect("put current object");
        storage
            .create_bucket(&BucketName::new("current-only"))
            .expect("create current-only bucket");

        let snapshot_root = temp_dir.path().join("snapshots").join("baseline");
        corruption.apply(&snapshot_root, &bucket, &key);

        let error = storage
            .restore_snapshot("baseline")
            .expect_err("internally corrupt snapshot is rejected");

        assert!(
            matches!(error, StorageError::CorruptState { .. }),
            "{corruption:?} should be reported as corrupt state: {error}"
        );
        assert_eq!(
            storage
                .get_object_bytes(&bucket, &key)
                .expect("current object remains readable"),
            b"current-body",
            "{corruption:?} should not replace current object state"
        );
        assert_eq!(
            bucket_names(&storage.list_buckets().expect("list current buckets")),
            ["current-only", "example-bucket"],
            "{corruption:?} should not replace current bucket state"
        );
    }
}

#[test]
fn restore_snapshot_rejects_corrupt_journal_before_current_state_changes() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"snapshot-body"))
        .expect("put snapshot object");
    storage.save_snapshot("baseline").expect("save snapshot");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"current-body"))
        .expect("put current object");
    storage
        .create_bucket(&BucketName::new("current-only"))
        .expect("create current-only bucket");
    fs::write(
        temp_dir
            .path()
            .join("snapshots")
            .join("baseline")
            .join("events")
            .join("journal.jsonl"),
        b"{not-json\n",
    )
    .expect("write corrupt snapshot journal");

    let error = storage
        .restore_snapshot("baseline")
        .expect_err("corrupt snapshot journal is rejected");

    assert!(matches!(error, StorageError::CorruptState { .. }));
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &key)
            .expect("current object remains readable"),
        b"current-body"
    );
    assert_eq!(
        bucket_names(&storage.list_buckets().expect("list current buckets")),
        ["current-only", "example-bucket"]
    );
}

#[test]
fn restore_snapshot_rejects_semantically_corrupt_journal_before_current_state_changes() {
    for corruption in [
        JournalSemanticCorruption::InvalidCommittedBucketName,
        JournalSemanticCorruption::InvalidCommittedObjectKey,
    ] {
        let (temp_dir, storage) = storage();
        let bucket = BucketName::new("example-bucket");
        let key = ObjectKey::new("object.txt");
        storage.create_bucket(&bucket).expect("create bucket");
        storage
            .put_object(put_request(&bucket, key.as_str(), b"snapshot-body"))
            .expect("put snapshot object");
        storage.save_snapshot("baseline").expect("save snapshot");
        storage
            .put_object(put_request(&bucket, key.as_str(), b"current-body"))
            .expect("put current object");
        storage
            .create_bucket(&BucketName::new("current-only"))
            .expect("create current-only bucket");

        let snapshot_root = temp_dir.path().join("snapshots").join("baseline");
        corruption.apply(&snapshot_root);

        let error = storage
            .restore_snapshot("baseline")
            .expect_err("semantically corrupt snapshot journal is rejected");

        assert!(
            matches!(error, StorageError::CorruptState { .. }),
            "{corruption:?} should be reported as corrupt state: {error}"
        );
        assert_eq!(
            storage
                .get_object_bytes(&bucket, &key)
                .expect("current object remains readable"),
            b"current-body",
            "{corruption:?} should not replace current object state"
        );
        assert_eq!(
            bucket_names(&storage.list_buckets().expect("list current buckets")),
            ["current-only", "example-bucket"],
            "{corruption:?} should not replace current bucket state"
        );
    }
}

#[test]
fn restore_snapshot_rejects_missing_journal_object_put_blob_before_current_state_changes() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"snapshot-body"))
        .expect("put snapshot object");
    storage.save_snapshot("baseline").expect("save snapshot");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"current-body"))
        .expect("put current object");
    storage
        .create_bucket(&BucketName::new("current-only"))
        .expect("create current-only bucket");
    fs::remove_file(snapshot_blob_path(
        &temp_dir.path().join("snapshots").join("baseline"),
        b"snapshot-body",
    ))
    .expect("remove snapshot object-put blob");

    let error = storage
        .restore_snapshot("baseline")
        .expect_err("snapshot with missing object-put blob is rejected");

    assert!(matches!(error, StorageError::CorruptState { .. }));
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &key)
            .expect("current object remains readable"),
        b"current-body"
    );
    assert_eq!(
        bucket_names(&storage.list_buckets().expect("list current buckets")),
        ["current-only", "example-bucket"]
    );
}

#[test]
fn restore_snapshot_rejects_corrupt_blob_before_current_state_changes() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    let key = ObjectKey::new("object.txt");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"snapshot-body"))
        .expect("put snapshot object");
    storage.save_snapshot("baseline").expect("save snapshot");
    storage
        .put_object(put_request(&bucket, key.as_str(), b"current-body"))
        .expect("put current object");
    storage
        .create_bucket(&BucketName::new("current-only"))
        .expect("create current-only bucket");
    fs::write(
        snapshot_blob_path(
            &temp_dir.path().join("snapshots").join("baseline"),
            b"snapshot-body",
        ),
        b"corrupt-snapshot-blob",
    )
    .expect("write corrupt snapshot blob");

    let error = storage
        .restore_snapshot("baseline")
        .expect_err("corrupt snapshot blob is rejected");

    assert!(matches!(error, StorageError::CorruptState { .. }));
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &key)
            .expect("current object remains readable"),
        b"current-body"
    );
    assert_eq!(
        bucket_names(&storage.list_buckets().expect("list current buckets")),
        ["current-only", "example-bucket"]
    );
}

#[test]
fn restore_empty_snapshot_uses_manifest_to_delete_current_state() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.save_snapshot("empty").expect("save empty snapshot");
    let manifest = fs::read_to_string(
        temp_dir
            .path()
            .join("snapshots")
            .join("empty")
            .join("manifest.json"),
    )
    .expect("read empty snapshot manifest");
    assert!(manifest.contains(r#""schema_version": 1"#));
    assert!(manifest.contains(r#""buckets""#));
    assert!(manifest.contains(r#""present": false"#));

    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, "object.txt", b"current-body"))
        .expect("put current object");

    storage
        .restore_snapshot("empty")
        .expect("restore empty snapshot");

    assert!(storage.list_buckets().expect("list buckets").is_empty());
    assert!(!temp_dir.path().join(STORAGE_ROOT_DIR).exists());
    assert!(!temp_dir.path().join("events").exists());
    assert!(!temp_dir.path().join("blobs").exists());
}

#[test]
fn reset_removes_current_state_but_preserves_snapshots() {
    let (temp_dir, storage) = storage();
    let bucket = BucketName::new("example-bucket");
    storage.create_bucket(&bucket).expect("create bucket");
    storage
        .put_object(put_request(&bucket, "object.txt", b"snapshot-body"))
        .expect("put object");
    storage.save_snapshot("baseline").expect("save snapshot");
    fs::write(temp_dir.path().join(".mutation-dirty"), b"dirty\n").expect("write dirty marker");

    storage.reset().expect("reset storage");

    assert!(!temp_dir.path().join(STORAGE_ROOT_DIR).exists());
    assert!(!temp_dir.path().join("events").exists());
    assert!(!temp_dir.path().join("blobs").exists());
    assert!(!temp_dir.path().join(".mutation-dirty").exists());
    assert!(temp_dir.path().join("snapshots").join("baseline").is_dir());

    storage
        .restore_snapshot("baseline")
        .expect("restore preserved snapshot");
    assert_eq!(
        storage
            .get_object_bytes(&bucket, &ObjectKey::new("object.txt"))
            .expect("read restored object"),
        b"snapshot-body"
    );
}

#[test]
fn snapshot_names_are_validated_before_save_or_restore() {
    let (_temp_dir, storage) = storage();

    for name in [
        "",
        ".",
        "..",
        "bad/name",
        "bad\\name",
        "bad name",
        "bad:name",
        "caf\u{00e9}",
    ] {
        let save_error = storage
            .save_snapshot(name)
            .expect_err("invalid snapshot name fails save");
        let restore_error = storage
            .restore_snapshot(name)
            .expect_err("invalid snapshot name fails restore");

        assert!(
            matches!(save_error, StorageError::InvalidArgument { .. }),
            "save should reject invalid snapshot name: {name:?}"
        );
        assert!(
            save_error.to_string().contains("invalid snapshot name"),
            "save error should explain invalid name: {name:?}"
        );
        assert!(
            matches!(restore_error, StorageError::InvalidArgument { .. }),
            "restore should reject invalid snapshot name: {name:?}"
        );
        assert!(
            restore_error.to_string().contains("invalid snapshot name"),
            "restore error should explain invalid name: {name:?}"
        );
    }
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
                delimiter: None,
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
                delimiter: None,
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
                delimiter: None,
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
                delimiter: None,
                continuation_token: first_page.next_continuation_token,
                max_keys: 1,
            },
        )
        .expect("list second prefixed page");

    assert_eq!(object_keys(&second_page.objects), ["images/b.png"]);
    assert!(!second_page.is_truncated);
}

#[test]
fn list_objects_max_keys_zero_returns_truncated_token_when_matching_objects_exist() {
    let (_temp_dir, storage) = storage_with_objects(["b.txt", "a.txt"]);
    let bucket = BucketName::new("example-bucket");

    let listing = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 0,
            },
        )
        .expect("list zero max keys");

    assert!(listing.objects.is_empty());
    assert_eq!(listing.max_keys, 0);
    assert!(listing.is_truncated);
    assert_eq!(
        listing.next_continuation_token.as_deref(),
        Some(continuation_token_for("a.txt").as_str())
    );
}

#[test]
fn list_objects_max_keys_zero_continuation_token_advances_to_next_key() {
    let (_temp_dir, storage) = storage_with_objects(["c.txt", "a.txt", "b.txt"]);
    let bucket = BucketName::new("example-bucket");

    let first_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 0,
            },
        )
        .expect("list first zero max keys page");

    assert!(first_page.objects.is_empty());
    assert!(first_page.is_truncated);
    assert_eq!(
        first_page.next_continuation_token.as_deref(),
        Some(continuation_token_for("a.txt").as_str())
    );

    let second_page = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: None,
                delimiter: None,
                continuation_token: first_page.next_continuation_token,
                max_keys: 0,
            },
        )
        .expect("list second zero max keys page");

    assert!(second_page.objects.is_empty());
    assert!(second_page.is_truncated);
    assert_eq!(
        second_page.next_continuation_token.as_deref(),
        Some(continuation_token_for("b.txt").as_str())
    );
}

#[test]
fn list_objects_max_keys_zero_is_not_truncated_without_matching_objects() {
    let (_temp_dir, storage) = storage_with_objects(["b.txt", "a.txt"]);
    let bucket = BucketName::new("example-bucket");

    let listing = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                prefix: Some(ObjectKey::new("missing/")),
                delimiter: None,
                continuation_token: None,
                max_keys: 0,
            },
        )
        .expect("list zero max keys with non-matching prefix");

    assert!(listing.objects.is_empty());
    assert_eq!(listing.max_keys, 0);
    assert!(!listing.is_truncated);
    assert_eq!(listing.next_continuation_token, None);
}

#[test]
fn list_objects_rejects_non_slash_delimiter() {
    let (_temp_dir, storage) = storage_with_objects(["a-b.txt"]);
    let bucket = BucketName::new("example-bucket");

    let error = storage
        .list_objects(
            &bucket,
            ListObjectsOptions {
                delimiter: Some("-".to_owned()),
                ..ListObjectsOptions::default()
            },
        )
        .expect_err("non-slash delimiter is unsupported");

    assert!(matches!(error, StorageError::InvalidArgument { .. }));
}

#[test]
fn public_operations_reject_symlink_root_when_platform_allows_it() {
    let temp_dir = TempDir::new().expect("temp dir");
    let target = temp_dir.path().join("target-root");
    let link = temp_dir.path().join("linked-root");
    fs::create_dir(&target).expect("create root symlink target");
    if let Err(skip) = create_dir_symlink_or_skip(&target, &link) {
        println!("{skip}");
        return;
    }

    let storage = FilesystemStorage::with_clock(link, FixedClock(fixed_time()));
    let error = storage
        .list_buckets()
        .expect_err("symlink root is rejected before storage access");

    assert!(matches!(error, StorageError::CorruptState { .. }));
}

#[test]
fn public_operations_reject_symlink_root_ancestor_when_platform_allows_it() {
    let temp_dir = TempDir::new().expect("temp dir");
    let target = temp_dir.path().join("target-root");
    let link = temp_dir.path().join("linked-root");
    let root = link.join("nested-root");
    fs::create_dir(&target).expect("create root symlink target");
    if let Err(skip) = create_dir_symlink_or_skip(&target, &link) {
        println!("{skip}");
        return;
    }

    let storage = FilesystemStorage::with_clock(root, FixedClock(fixed_time()));
    let error = storage
        .create_bucket(&BucketName::new("example-bucket"))
        .expect_err("symlink root ancestor is rejected before storage access");

    assert!(matches!(error, StorageError::CorruptState { .. }));
    assert!(
        !target.join("nested-root").exists(),
        "storage creation must not follow a symlinked root ancestor"
    );
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
                    delimiter: None,
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
                delimiter: None,
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

fn create_file_symlink_or_skip(target: &Path, link: &Path) -> Result<(), SymlinkTestSkipped> {
    try_create_file_symlink(target, link).map_err(|source| SymlinkTestSkipped {
        test: "object content symlink safety",
        target: target.to_path_buf(),
        link: link.to_path_buf(),
        source,
    })
}

fn create_dir_symlink_or_skip(target: &Path, link: &Path) -> Result<(), SymlinkTestSkipped> {
    try_create_dir_symlink(target, link).map_err(|source| SymlinkTestSkipped {
        test: "object directory symlink safety",
        target: target.to_path_buf(),
        link: link.to_path_buf(),
        source,
    })
}

struct SymlinkTestSkipped {
    test: &'static str,
    target: PathBuf,
    link: PathBuf,
    source: io::Error,
}

impl Display for SymlinkTestSkipped {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "skipped {} test: symlink creation unavailable from {} to {}: {}",
            self.test,
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

#[cfg(unix)]
fn try_create_dir_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn try_create_dir_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
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

fn create_multipart_request(bucket: &BucketName, key: &str) -> CreateMultipartUploadRequest {
    CreateMultipartUploadRequest {
        bucket: bucket.clone(),
        key: ObjectKey::new(key),
        content_type: None,
        user_metadata: BTreeMap::new(),
    }
}

fn upload_part_request(
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &str,
    part_number: u32,
    bytes: &[u8],
) -> UploadPartRequest {
    UploadPartRequest {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.to_owned(),
        part_number,
        bytes: bytes.to_vec(),
    }
}

fn completed_part(part_number: u32, etag: &str) -> CompletedMultipartPart {
    CompletedMultipartPart {
        part_number,
        etag: etag.to_owned(),
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

fn bucket_names(buckets: &[s3lab::storage::BucketSummary]) -> Vec<&str> {
    buckets
        .iter()
        .map(|bucket| bucket.name.as_str())
        .collect::<Vec<_>>()
}

fn object_key_values(keys: &[ObjectKey]) -> Vec<&str> {
    keys.iter().map(ObjectKey::as_str).collect::<Vec<_>>()
}

fn listing_entry_markers(entries: &[ObjectListingEntry]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| match entry {
            ObjectListingEntry::Object(object) => format!("object:{}", object.key.as_str()),
            ObjectListingEntry::CommonPrefix(prefix) => format!("prefix:{}", prefix.as_str()),
        })
        .collect()
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

fn write_bucket_state(root: &Path, bucket: &BucketName) {
    let bucket_dir = bucket_dir(root, bucket);
    fs::create_dir_all(bucket_dir.join("objects")).expect("create bucket state");
    write_bucket_metadata(
        root,
        bucket,
        &format!(r#"{{"bucket":"{}"}}"#, bucket.as_str()),
    );
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
        .join(encoded_bucket_path_component(bucket))
}

fn object_paths(root: &Path, bucket: &BucketName, key: &ObjectKey) -> ObjectPaths {
    let object_dir = bucket_dir(root, bucket)
        .join("objects")
        .join(object_key_shard(key))
        .join(encoded_object_key_path_component(key));
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

#[derive(Debug, Clone, Copy)]
enum SnapshotCorruption {
    MissingBucketMetadata,
    CorruptBucketMetadata,
    MissingObjectMetadata,
    CorruptObjectMetadata,
    MissingObjectContent,
    CorruptObjectContent,
}

impl SnapshotCorruption {
    fn apply(self, snapshot_root: &Path, bucket: &BucketName, key: &ObjectKey) {
        let bucket_metadata = bucket_dir(snapshot_root, bucket).join("bucket.json");
        let object_paths = object_paths(snapshot_root, bucket, key);
        match self {
            Self::MissingBucketMetadata => {
                fs::remove_file(bucket_metadata).expect("remove snapshot bucket metadata");
            }
            Self::CorruptBucketMetadata => {
                fs::write(bucket_metadata, b"{not-json")
                    .expect("write corrupt snapshot bucket metadata");
            }
            Self::MissingObjectMetadata => {
                fs::remove_file(object_paths.metadata).expect("remove snapshot object metadata");
            }
            Self::CorruptObjectMetadata => {
                fs::write(object_paths.metadata, b"{not-json")
                    .expect("write corrupt snapshot object metadata");
            }
            Self::MissingObjectContent => {
                fs::remove_file(object_paths.content).expect("remove snapshot object content");
            }
            Self::CorruptObjectContent => {
                fs::write(object_paths.content, b"corrupt-body")
                    .expect("write corrupt snapshot object content");
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum JournalSemanticCorruption {
    InvalidCommittedBucketName,
    InvalidCommittedObjectKey,
}

impl JournalSemanticCorruption {
    fn apply(self, snapshot_root: &Path) {
        rewrite_snapshot_journal(snapshot_root, |line_index, record| match self {
            Self::InvalidCommittedBucketName if line_index < 2 => {
                record["mutation"]["bucket"] = serde_json::json!("Invalid_Bucket");
            }
            Self::InvalidCommittedObjectKey if (2..4).contains(&line_index) => {
                record["mutation"]["key"] = serde_json::json!("");
            }
            _ => {}
        });
    }
}

fn rewrite_snapshot_journal(
    snapshot_root: &Path,
    mut rewrite: impl FnMut(usize, &mut serde_json::Value),
) {
    let journal_path = snapshot_root.join("events").join("journal.jsonl");
    let journal = fs::read_to_string(&journal_path).expect("read snapshot journal");
    let mut rewritten = Vec::new();
    for (line_index, line) in journal.lines().enumerate() {
        let mut record =
            serde_json::from_str::<serde_json::Value>(line).expect("parse journal record");
        rewrite(line_index, &mut record);
        rewritten.extend(serde_json::to_vec(&record).expect("serialize journal record"));
        rewritten.push(b'\n');
    }
    fs::write(journal_path, rewritten).expect("write rewritten snapshot journal");
}

fn encoded_bucket_path_component(bucket: &BucketName) -> String {
    format!("bucket-{}", sha256_lower_hex(bucket.as_str()))
}

fn encoded_object_key_path_component(key: &ObjectKey) -> String {
    format!("key-{}", sha256_lower_hex(key.as_str()))
}

fn object_key_shard(key: &ObjectKey) -> String {
    sha256_lower_hex(key.as_str())[..2].to_owned()
}

fn snapshot_blob_path(snapshot_root: &Path, bytes: &[u8]) -> PathBuf {
    let content_sha256 = sha256_lower_hex_bytes(bytes);
    snapshot_root
        .join("blobs")
        .join(&content_sha256[..2])
        .join(content_sha256)
}

fn blob_path(root: &Path, bytes: &[u8]) -> PathBuf {
    let content_sha256 = sha256_lower_hex_bytes(bytes);
    root.join("blobs")
        .join(&content_sha256[..2])
        .join(content_sha256)
}

fn multipart_etag_for<const N: usize>(parts: [&[u8]; N]) -> String {
    let mut joined_digests = Vec::new();
    for part in parts {
        joined_digests.extend_from_slice(&Md5::digest(part));
    }
    let digest = Md5::digest(joined_digests);
    format!("\"{}-{}\"", lower_hex(&digest), N)
}

fn sha256_lower_hex(value: &str) -> String {
    sha256_lower_hex_bytes(value.as_bytes())
}

fn sha256_lower_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    lower_hex(&digest)
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
