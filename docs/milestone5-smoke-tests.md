# Milestone 5 Smoke Tests

This page documents narrow local smoke recipes for S3Lab milestone 5 multipart upload behavior. It is intended to produce offline AWS CLI evidence for the multipart upload workflow against the local path-style endpoint.

This is not a general S3 compatibility claim. Milestone 5 smoke evidence only covers the local workflow exercised below.

## Supported Milestone 5 Behavior

- Create a multipart upload for an object through the local path-style endpoint.
- Upload numbered parts for the active upload.
- List uploaded parts in stable part-number order.
- Complete the upload with the uploaded part ETags and read the completed object.
- List the completed object through `list-objects-v2`.
- Abort an active multipart upload without creating an object.
- All recipes run offline against localhost. No cloud account or hosted backend is required.

## Current Limitations

- Path-style addressing only, for example `http://127.0.0.1:9000/<bucket>/<key>`.
- No virtual-host style routing.
- No configurable credentials.
- No strict authentication mode.
- No session-token presigned URLs.
- No trace persistence/API/UI.
- No hosted backend.
- No production storage guarantee.
- No full compatibility matrix.
- No broad S3 compatibility claim.
- No broad multipart compatibility claim beyond the local AWS CLI workflow below.
- No full streaming chunk-signature validation.
- No cross-process write locking guarantee.

## Shared Assumptions

- S3Lab is built or run with Cargo from this repository.
- The AWS CLI is installed and available as `aws`.
- Requests stay offline and target only `http://127.0.0.1:9000`.
- The smoke test uses a temporary local data directory and temporary body files.
- The endpoint recipe uses path-style addressing and the static local S3Lab credentials `s3lab` / `s3lab-secret`.

## Start S3Lab

Run the server in one terminal:

```powershell
$S3LAB_DATA_DIR = Join-Path $env:TEMP "s3lab-m5-smoke-$([guid]::NewGuid())"
cargo run -- serve --host 127.0.0.1 --port 9000 --data-dir $S3LAB_DATA_DIR
```

Expected startup output includes:

```text
S3 endpoint:  http://127.0.0.1:9000
Data dir:     <temporary data directory>
```

Leave this terminal running while issuing endpoint commands.

## Configure the AWS CLI Session

Run these commands in a second terminal:

```powershell
$env:AWS_ACCESS_KEY_ID = "s3lab"
$env:AWS_SECRET_ACCESS_KEY = "s3lab-secret"
$env:AWS_DEFAULT_REGION = "us-east-1"
$env:AWS_EC2_METADATA_DISABLED = "true"

$Endpoint = "http://127.0.0.1:9000"
$Bucket = "s3lab-m5-smoke-bucket"
$Key = "multipart/hello.bin"
$AbortKey = "multipart/aborted.bin"
$PartOne = Join-Path $env:TEMP "s3lab-m5-part-1.bin"
$PartTwo = Join-Path $env:TEMP "s3lab-m5-part-2.bin"
$AbortPart = Join-Path $env:TEMP "s3lab-m5-abort-part.bin"
$CompleteRequest = Join-Path $env:TEMP "s3lab-m5-complete.json"
$Download = Join-Path $env:TEMP "s3lab-m5-download.bin"

Set-Content -Path $PartOne -Value "hello " -NoNewline -Encoding ascii
Set-Content -Path $PartTwo -Value "world" -NoNewline -Encoding ascii
Set-Content -Path $AbortPart -Value "abort me" -NoNewline -Encoding ascii
```

All commands below include `--endpoint-url $Endpoint` so the AWS CLI talks to localhost instead of AWS.

## Create a Bucket and Multipart Upload

```powershell
aws --endpoint-url $Endpoint s3api create-bucket --bucket $Bucket

$CreateUpload = aws --endpoint-url $Endpoint s3api create-multipart-upload `
  --bucket $Bucket `
  --key $Key `
  --content-type "application/octet-stream" `
  --metadata case=value | ConvertFrom-Json

$UploadId = $CreateUpload.UploadId
$UploadId
```

Expected output includes a non-empty upload ID similar to:

```text
upload-...
```

The active multipart upload is not yet a readable object and should not appear in object listings:

```powershell
aws --endpoint-url $Endpoint s3api list-objects-v2 --bucket $Bucket --prefix "multipart/"
```

Expected output includes:

```json
{
    "KeyCount": 0
}
```

## Upload Parts

```powershell
$PartOneResult = aws --endpoint-url $Endpoint s3api upload-part `
  --bucket $Bucket `
  --key $Key `
  --upload-id $UploadId `
  --part-number 1 `
  --body $PartOne | ConvertFrom-Json

$PartTwoResult = aws --endpoint-url $Endpoint s3api upload-part `
  --bucket $Bucket `
  --key $Key `
  --upload-id $UploadId `
  --part-number 2 `
  --body $PartTwo | ConvertFrom-Json

$PartOneResult
$PartTwoResult
```

Expected output for each part includes an `ETag` value:

```json
{
    "ETag": "\"...\""
}
```

## List Parts

```powershell
aws --endpoint-url $Endpoint s3api list-parts `
  --bucket $Bucket `
  --key $Key `
  --upload-id $UploadId
```

Expected output includes both parts in part-number order:

```json
{
    "Parts": [
        {
            "PartNumber": 1,
            "ETag": "\"...\"",
            "Size": 6
        },
        {
            "PartNumber": 2,
            "ETag": "\"...\"",
            "Size": 5
        }
    ]
}
```

## Complete the Multipart Upload

Create the completion request from the ETags returned by `upload-part`:

```powershell
$MultipartUpload = @{
  Parts = @(
    @{
      ETag = $PartOneResult.ETag
      PartNumber = 1
    },
    @{
      ETag = $PartTwoResult.ETag
      PartNumber = 2
    }
  )
}

$CompleteJson = $MultipartUpload | ConvertTo-Json -Depth 5
[System.IO.File]::WriteAllText(
  $CompleteRequest,
  $CompleteJson,
  [System.Text.UTF8Encoding]::new($false)
)

$CompleteResult = aws --endpoint-url $Endpoint s3api complete-multipart-upload `
  --bucket $Bucket `
  --key $Key `
  --upload-id $UploadId `
  --multipart-upload "file://$CompleteRequest" | ConvertFrom-Json

$CompleteResult
```

Expected output includes the bucket, key, and a multipart ETag:

```json
{
    "Location": "/s3lab-m5-smoke-bucket/multipart/hello.bin",
    "Bucket": "s3lab-m5-smoke-bucket",
    "Key": "multipart/hello.bin",
    "ETag": "\"...-2\""
}
```

After completion, the active upload is removed. A follow-up `list-parts` for the same upload ID should fail with `NoSuchUpload`:

```powershell
aws --endpoint-url $Endpoint s3api list-parts `
  --bucket $Bucket `
  --key $Key `
  --upload-id $UploadId
```

Expected error includes:

```text
NoSuchUpload
```

## Get and List the Completed Object

```powershell
aws --endpoint-url $Endpoint s3api get-object --bucket $Bucket --key $Key $Download
Get-Content -Path $Download

aws --endpoint-url $Endpoint s3api list-objects-v2 --bucket $Bucket --prefix "multipart/"
```

The downloaded file should contain:

```text
hello world
```

The object listing should include the completed object with size `11`:

```json
{
    "KeyCount": 1,
    "Contents": [
        {
            "Key": "multipart/hello.bin",
            "Size": 11
        }
    ]
}
```

## Abort an Active Multipart Upload

Create a second multipart upload, upload one part, then abort it:

```powershell
$AbortCreate = aws --endpoint-url $Endpoint s3api create-multipart-upload `
  --bucket $Bucket `
  --key $AbortKey | ConvertFrom-Json

$AbortUploadId = $AbortCreate.UploadId

aws --endpoint-url $Endpoint s3api upload-part `
  --bucket $Bucket `
  --key $AbortKey `
  --upload-id $AbortUploadId `
  --part-number 1 `
  --body $AbortPart

aws --endpoint-url $Endpoint s3api abort-multipart-upload `
  --bucket $Bucket `
  --key $AbortKey `
  --upload-id $AbortUploadId
```

The abort command should finish without output. The aborted upload should not create an object:

```powershell
aws --endpoint-url $Endpoint s3api get-object --bucket $Bucket --key $AbortKey $Download
```

Expected error includes:

```text
NoSuchKey
```

The aborted upload should no longer be listable:

```powershell
aws --endpoint-url $Endpoint s3api list-parts `
  --bucket $Bucket `
  --key $AbortKey `
  --upload-id $AbortUploadId
```

Expected error includes:

```text
NoSuchUpload
```

## What This Exercises

These recipes exercise only this local milestone 5 path:

- start the local endpoint with an explicit data directory
- create one bucket through the local AWS CLI endpoint
- create a multipart upload for one object
- upload two parts
- list the uploaded parts
- complete the upload using the returned part ETags
- read and list the completed object
- abort a separate active multipart upload

It does not prove broad AWS S3 compatibility. It does not cover virtual-host style addressing, configurable credentials, strict authentication mode, session-token presigned URLs, bucket policies, ACLs, object tags, encryption behavior, range reads, trace persistence/API/UI, full streaming chunk-signature validation, hosted operation, production storage behavior, cross-process write locking, or a full compatibility matrix.
