# Milestone 4 Smoke Tests

This page documents narrow local smoke recipes for S3Lab milestone 4 storage durability features. It is intended to produce local evidence for the current WAL/event log, content-addressed blobs, committed dirty mutation recovery, snapshots, restore, and reset behavior.

This is not a general S3 compatibility claim. Milestone 4 smoke evidence only covers the local storage workflows exercised below.

## Supported Milestone 4 Behavior

- Local WAL/event log records bucket and object mutations.
- Object bodies are stored as content-addressed blobs for the current object model.
- Committed dirty mutations can be recovered from the local event log and blobs when current state needs recovery.
- Named snapshots can be saved from a data directory.
- Current state can be restored from a named snapshot.
- Current state can be reset while preserving saved snapshots.
- All recipes run offline against localhost or local files. No cloud account or hosted backend is required.

## Current Limitations

- No multipart snapshot behavior beyond the current bucket and object model.
- No hosted backend.
- No production storage guarantee.
- No broad S3 compatibility claim beyond tested and documented behavior.
- No virtual-host style routing, configurable credentials, strict authentication mode, or multipart upload workflow.
- No cross-process write locking guarantee. For these smoke tests, stop the server before running `snapshot save`, `snapshot restore`, or `reset` from a separate CLI process.

## Shared Assumptions

- S3Lab is built or run with Cargo from this repository.
- The AWS CLI is installed and available as `aws` for the endpoint recipe.
- Requests stay offline and target only `http://127.0.0.1:9000`.
- The smoke test uses a temporary local data directory.
- The endpoint recipe uses path-style addressing and the static local S3Lab credentials `s3lab` / `s3lab-secret`.

## Start S3Lab With a Data Directory

Run the server in one terminal:

```powershell
$S3LAB_DATA_DIR = Join-Path $env:TEMP "s3lab-m4-smoke-$([guid]::NewGuid())"
cargo run -- serve --host 127.0.0.1 --port 9000 --data-dir $S3LAB_DATA_DIR
```

Expected startup output includes:

```text
S3 endpoint:  http://127.0.0.1:9000
Data dir:     <temporary data directory>
```

Leave this terminal running while issuing endpoint commands. Before running snapshot or reset commands from another process, stop the server with Ctrl-C.

## Create a Bucket and Object

Run these commands in a second terminal while the server is running:

```powershell
# Set this to the Data dir value printed by the server terminal.
$S3LAB_DATA_DIR = "<temporary data directory>"

$env:AWS_ACCESS_KEY_ID = "s3lab"
$env:AWS_SECRET_ACCESS_KEY = "s3lab-secret"
$env:AWS_DEFAULT_REGION = "us-east-1"
$env:AWS_EC2_METADATA_DISABLED = "true"

$Endpoint = "http://127.0.0.1:9000"
$Bucket = "s3lab-m4-smoke-bucket"
$Key = "hello.txt"
$Body = Join-Path $env:TEMP "s3lab-m4-body.txt"
$Download = Join-Path $env:TEMP "s3lab-m4-download.txt"

Set-Content -Path $Body -Value "hello from milestone 4 baseline" -NoNewline

aws --endpoint-url $Endpoint s3api create-bucket --bucket $Bucket
aws --endpoint-url $Endpoint s3api put-object --bucket $Bucket --key $Key --body $Body
aws --endpoint-url $Endpoint s3api get-object --bucket $Bucket --key $Key $Download
Get-Content -Path $Download
```

The downloaded file should contain:

```text
hello from milestone 4 baseline
```

With the server still running, the data directory should contain local storage evidence for the current object model:

```powershell
Get-ChildItem -Path (Join-Path $S3LAB_DATA_DIR "events")
Get-ChildItem -Path (Join-Path $S3LAB_DATA_DIR "blobs") -Recurse -File | Select-Object -First 5 FullName
```

The `events` directory should include `journal.jsonl`, and the `blobs` directory should include at least one content-addressed object body file. These are implementation evidence for milestone 4 storage durability, not a public storage format guarantee.

Stop the server with Ctrl-C before continuing.

## Save a Snapshot

Run this command after the server has stopped:

```powershell
cargo run -- snapshot save baseline --data-dir $S3LAB_DATA_DIR
```

Expected output:

```text
Snapshot saved: baseline
```

The data directory should now contain a saved snapshot under `snapshots\baseline`.

You can inspect the saved snapshot directory locally:

```powershell
Get-ChildItem -Path (Join-Path $S3LAB_DATA_DIR "snapshots\baseline")
```

The snapshot should include saved state for buckets, blobs, and events. Committed dirty mutation recovery is covered by storage-level automated tests; these smoke commands do not intentionally corrupt or interrupt local writes.

## Mutate Current State

Restart the server with the same data directory:

```powershell
cargo run -- serve --host 127.0.0.1 --port 9000 --data-dir $S3LAB_DATA_DIR
```

Run this in the second terminal:

```powershell
Set-Content -Path $Body -Value "hello after milestone 4 mutation" -NoNewline

aws --endpoint-url $Endpoint s3api put-object --bucket $Bucket --key $Key --body $Body
aws --endpoint-url $Endpoint s3api get-object --bucket $Bucket --key $Key $Download
Get-Content -Path $Download
```

The downloaded file should contain:

```text
hello after milestone 4 mutation
```

Stop the server with Ctrl-C before restoring the snapshot.

## Restore the Snapshot

Run this command after the server has stopped:

```powershell
cargo run -- snapshot restore baseline --data-dir $S3LAB_DATA_DIR
```

Expected output:

```text
Snapshot restored: baseline
```

Restart the server with the same data directory and verify the baseline object was restored:

```powershell
cargo run -- serve --host 127.0.0.1 --port 9000 --data-dir $S3LAB_DATA_DIR
```

In the second terminal:

```powershell
aws --endpoint-url $Endpoint s3api get-object --bucket $Bucket --key $Key $Download
Get-Content -Path $Download
```

The downloaded file should contain:

```text
hello from milestone 4 baseline
```

Stop the server with Ctrl-C before reset.

## Reset Current State While Preserving Snapshots

Run this command after the server has stopped:

```powershell
cargo run -- reset --data-dir $S3LAB_DATA_DIR
```

Expected output starts with:

```text
Storage reset:
```

Restart the server and verify the current bucket state is gone:

```powershell
cargo run -- serve --host 127.0.0.1 --port 9000 --data-dir $S3LAB_DATA_DIR
```

In the second terminal:

```powershell
aws --endpoint-url $Endpoint s3api list-buckets
```

The bucket created earlier should not be listed.

Stop the server, then restore the preserved snapshot:

```powershell
cargo run -- snapshot restore baseline --data-dir $S3LAB_DATA_DIR
```

Restart the server and verify the baseline object is available again:

```powershell
cargo run -- serve --host 127.0.0.1 --port 9000 --data-dir $S3LAB_DATA_DIR
```

In the second terminal:

```powershell
aws --endpoint-url $Endpoint s3api get-object --bucket $Bucket --key $Key $Download
Get-Content -Path $Download
```

The downloaded file should contain:

```text
hello from milestone 4 baseline
```

## What This Exercises

These recipes exercise only this local milestone 4 path:

- start the local endpoint with an explicit data directory
- create one bucket and object through local endpoint calls
- save one named snapshot
- mutate the current object state after the snapshot
- restore the named snapshot
- reset current state while preserving snapshots
- restore the preserved snapshot after reset

It does not prove broad AWS S3 compatibility. It does not cover multipart snapshots beyond the current object model, hosted operation, production storage behavior, virtual-host style addressing, configurable credentials, strict authentication mode, multipart uploads, bucket policies, ACLs, object tags, encryption behavior, range reads, trace persistence/API/UI, or cross-process write locking.
