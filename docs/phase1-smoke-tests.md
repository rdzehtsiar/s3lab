# Phase 1 Smoke Tests

This page documents a narrow AWS CLI smoke recipe for S3Lab Phase 1. It is intended to produce local evidence that the current Phase 1 endpoint can accept a basic AWS CLI bucket and object lifecycle against `localhost`.

This is not a general compatibility claim. Phase 1 smoke evidence only covers the operations exercised below.

## Shared Assumptions

- S3Lab is running locally and listening on a loopback endpoint such as `http://127.0.0.1:9000`.
- The smoke test uses a temporary local data directory.
- The AWS CLI is installed and available as `aws`.
- The AWS CLI uses dummy credentials. No cloud account is required.
- Requests stay offline and target only the local S3Lab endpoint through `--endpoint-url`.
- Phase 1 accepts signed AWS CLI requests but does not validate SigV4 signatures yet.
- Phase 1 supports path-style localhost routing, for example `http://127.0.0.1:9000/s3lab-smoke-bucket/object.txt`.
- Virtual-host style routing, presigned URLs, and multipart uploads are deferred.

## Start S3Lab

Run the server in one terminal:

```powershell
$S3LAB_DATA_DIR = Join-Path $env:TEMP "s3lab-smoke-$([guid]::NewGuid())"
cargo run -- serve --host 127.0.0.1 --port 9000 --data-dir $S3LAB_DATA_DIR
```

Expected startup output includes:

```text
S3 endpoint:  http://127.0.0.1:9000
Data dir:     <temporary data directory>
```

Leave this terminal running while using the AWS CLI commands below.

## AWS CLI Smoke Recipe

Run these commands in a second terminal:

```powershell
$env:AWS_ACCESS_KEY_ID = "s3lab"
$env:AWS_SECRET_ACCESS_KEY = "s3lab-secret"
$env:AWS_DEFAULT_REGION = "us-east-1"
$env:AWS_EC2_METADATA_DISABLED = "true"

$Endpoint = "http://127.0.0.1:9000"
$Bucket = "s3lab-smoke-bucket"
$Key = "hello.txt"
$Body = Join-Path $env:TEMP "s3lab-smoke-body.txt"
$Download = Join-Path $env:TEMP "s3lab-smoke-download.txt"

Set-Content -Path $Body -Value "hello from s3lab phase 1" -NoNewline

aws --endpoint-url $Endpoint s3api create-bucket --bucket $Bucket
aws --endpoint-url $Endpoint s3api put-object --bucket $Bucket --key $Key --body $Body
aws --endpoint-url $Endpoint s3api list-objects-v2 --bucket $Bucket
aws --endpoint-url $Endpoint s3api get-object --bucket $Bucket --key $Key $Download
Get-Content -Path $Download
aws --endpoint-url $Endpoint s3api delete-object --bucket $Bucket --key $Key
aws --endpoint-url $Endpoint s3api delete-bucket --bucket $Bucket
```

The downloaded file should contain:

```text
hello from s3lab phase 1
```

## What This Exercises

This recipe exercises only this local Phase 1 path:

- create one bucket
- put one object
- list objects with ListObjectsV2
- get the object
- delete the object
- delete the bucket

It does not prove broad AWS S3 compatibility. It does not cover virtual-host style addressing, presigned URLs, multipart uploads, signature validation, bucket policies, ACLs, object tags, encryption headers, range reads, or production storage behavior.
