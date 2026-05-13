# Milestone 6 Smoke Tests

This page documents narrow local smoke recipes for S3Lab milestone 6 embedded inspector UI behavior. It is intended to produce offline evidence that the same Rust binary can serve the local S3 endpoint and a read-only inspector UI for recent requests and local storage state.

This is not a general S3 compatibility claim. Milestone 6 smoke evidence only covers the local inspector workflow exercised below.

## Supported Milestone 6 Behavior

- Start the local S3 endpoint and embedded inspector UI from one `serve` process.
- Print both local startup endpoints: S3 on `127.0.0.1:9000` and inspector UI on `127.0.0.1:9001` by default.
- Override the inspector bind address with `--inspector-host` and `--inspector-port`.
- Inspect recent local S3 requests and request details captured during the current server process.
- Inspect local buckets, objects, active multipart uploads, and saved snapshots.
- Show placeholder or current-status pages for replay sessions, failure rules, and compatibility matrix work.
- All recipes run offline against localhost. No cloud account or hosted backend is required.

## Current Limitations

- The inspector APIs and UI are read-only inspection surfaces.
- The inspector does not create, mutate, restore, delete, replay, or inject failures.
- Request traces are in-memory for the current server process.
- The inspector does not expose object payload bytes, multipart part payload bytes, credential values, authorization signatures, absolute storage paths, or raw filesystem layout.
- Object and multipart views expose metadata such as bucket, key, ETag, content length, content type, timestamps, upload ID, and part counts.
- Replay sessions and failure rules are placeholder pages in this build and are not backed by replay or failure-rule APIs.
- The compatibility matrix page is a current-status summary, not a full compatibility matrix or broad S3 compatibility claim.
- Path-style addressing only, for example `http://127.0.0.1:9000/<bucket>/<key>`.
- No virtual-host style routing.
- No configurable credentials.
- No strict authentication mode.
- No hosted backend.
- No production storage guarantee.
- No full streaming chunk-signature validation.
- No cross-process write locking guarantee. For this smoke test, stop the server before running `snapshot save` from a separate CLI process.

## Shared Assumptions

- S3Lab is built or run with Cargo from this repository.
- The AWS CLI is installed and available as `aws`.
- Requests stay offline and target only `http://127.0.0.1:9000`.
- The inspector UI stays offline and targets only `http://127.0.0.1:9001`.
- The smoke test uses a temporary local data directory and temporary body files.
- The endpoint recipe uses path-style addressing and the static local S3Lab credentials `s3lab` / `s3lab-secret`.

## Start S3Lab

Run the server in one terminal:

```powershell
$S3LAB_DATA_DIR = Join-Path $env:TEMP "s3lab-m6-smoke-$([guid]::NewGuid())"
cargo run -- serve --host 127.0.0.1 --port 9000 --inspector-host 127.0.0.1 --inspector-port 9001 --data-dir $S3LAB_DATA_DIR
```

Expected startup output includes:

```text
S3 endpoint:  http://127.0.0.1:9000
Inspector UI: http://127.0.0.1:9001
Data dir:     <temporary data directory>
```

Leave this terminal running while issuing endpoint commands.

The inspector health endpoint should answer locally:

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:9001/health | Select-Object -ExpandProperty Content
```

Expected output:

```text
ok
```

## Create Local S3 State

Run these commands in a second terminal:

```powershell
$env:AWS_ACCESS_KEY_ID = "s3lab"
$env:AWS_SECRET_ACCESS_KEY = "s3lab-secret"
$env:AWS_DEFAULT_REGION = "us-east-1"
$env:AWS_EC2_METADATA_DISABLED = "true"

$Endpoint = "http://127.0.0.1:9000"
$Bucket = "s3lab-m6-smoke-bucket"
$Key = "inspector/hello.txt"
$MultipartKey = "inspector/large.bin"
$Body = Join-Path $env:TEMP "s3lab-m6-body.txt"
$PartOne = Join-Path $env:TEMP "s3lab-m6-part-1.bin"

Set-Content -Path $Body -Value "hello from milestone 6 inspector" -NoNewline -Encoding ascii
Set-Content -Path $PartOne -Value "multipart inspection body" -NoNewline -Encoding ascii

aws --endpoint-url $Endpoint s3api create-bucket --bucket $Bucket
aws --endpoint-url $Endpoint s3api put-object --bucket $Bucket --key $Key --body $Body --content-type "text/plain"
aws --endpoint-url $Endpoint s3api head-object --bucket $Bucket --key $Key
aws --endpoint-url $Endpoint s3api list-objects-v2 --bucket $Bucket --prefix "inspector/"
```

The object listing should include `inspector/hello.txt`.

Create an active multipart upload so the inspector has multipart state to show:

```powershell
$CreateUpload = aws --endpoint-url $Endpoint s3api create-multipart-upload `
  --bucket $Bucket `
  --key $MultipartKey `
  --content-type "application/octet-stream" | ConvertFrom-Json

$UploadId = $CreateUpload.UploadId

aws --endpoint-url $Endpoint s3api upload-part `
  --bucket $Bucket `
  --key $MultipartKey `
  --upload-id $UploadId `
  --part-number 1 `
  --body $PartOne
```

The upload part response should include an `ETag`.

## Inspect Requests and Storage State

Open the inspector UI:

```powershell
Start-Process http://127.0.0.1:9001/
```

Use the inspector navigation and the Refresh button as needed.

Expected UI evidence:

- Dashboard shows non-zero counts for recent requests, buckets, objects, and multipart uploads.
- Requests shows recent local S3 API calls such as `PUT`, `HEAD`, and `GET` or `POST` requests.
- Selecting a request opens Request detail with trace events for that request.
- Buckets lists `s3lab-m6-smoke-bucket`.
- Objects lists `inspector/hello.txt` for the selected bucket, including size, content type, ETag, and last-modified metadata.
- Multipart uploads lists an active upload for `inspector/large.bin` with one uploaded part.
- Replay sessions shows that replay support is not implemented in this build.
- Failure rules shows that failure injection rules are not implemented in this build.
- Compatibility matrix shows current local evidence status only.

The inspector should not show the object body text, multipart part body text, AWS secret value, authorization signature, absolute data directory path, or raw filesystem layout.

The same state can be checked through the read-only inspector APIs:

```powershell
Invoke-RestMethod http://127.0.0.1:9001/api/requests
Invoke-RestMethod http://127.0.0.1:9001/api/buckets
Invoke-RestMethod "http://127.0.0.1:9001/api/buckets/$Bucket/objects"
Invoke-RestMethod http://127.0.0.1:9001/api/multipart-uploads
Invoke-RestMethod http://127.0.0.1:9001/api/snapshots
```

## Inspect Saved Snapshots

Stop the server with Ctrl-C before saving a snapshot from another process.

Run this command after the server has stopped:

```powershell
cargo run -- snapshot save inspector-baseline --data-dir $S3LAB_DATA_DIR
```

Expected output:

```text
Snapshot saved: inspector-baseline
```

Restart the server with the same data directory:

```powershell
cargo run -- serve --host 127.0.0.1 --port 9000 --inspector-host 127.0.0.1 --inspector-port 9001 --data-dir $S3LAB_DATA_DIR
```

Open or refresh the inspector UI at `http://127.0.0.1:9001/`.

Expected UI evidence:

- Snapshots lists `inspector-baseline`.
- Buckets still lists `s3lab-m6-smoke-bucket`.
- Objects still lists `inspector/hello.txt`.
- Multipart uploads still lists the active upload if it was not completed or aborted before restart.

Recent request traces are process-local. After restart, the Requests view starts from the new process history.

## What This Exercises

These recipes exercise only this local milestone 6 path:

- start one local process serving both the S3 endpoint and inspector UI
- create one bucket and object through local AWS CLI endpoint calls
- create one active multipart upload with one uploaded part
- inspect recent requests and request detail through the embedded UI
- inspect buckets, objects, multipart uploads, and snapshots through read-only inspector surfaces
- confirm replay sessions, failure rules, and compatibility matrix pages are status or placeholder views in this build

It does not prove broad AWS S3 compatibility. It does not cover virtual-host style addressing, configurable credentials, strict authentication mode, session-token presigned URLs, bucket policies, ACLs, object tags, encryption behavior, range reads, hosted operation, production storage behavior, cross-process write locking, replay execution, failure-rule execution, or a full compatibility matrix.
