# Milestone 3 Smoke Tests

This page documents narrow local smoke recipes for S3Lab milestone 3 presigned URL support. It is intended to produce local evidence for presigned path-style object `GET` and `PUT` requests against `localhost`.

This is not a general compatibility claim. Milestone 3 smoke evidence only covers the presigned URL workflows exercised below.

## Supported Presigned Behavior

- Path-style object `GET` and `PUT` requests, for example `http://127.0.0.1:9000/s3lab-presign-bucket/object.txt`.
- Query-string SigV4 presigned URLs generated with the static local credentials `s3lab` / `s3lab-secret`.
- Expiration validation through the SigV4 presigned URL timestamp and expires parameters.
- Offline local endpoint usage. No cloud account is required.
- Localhost-only requests in the recipes below.

## Current Limitations

- No virtual-host style routing.
- No session-token presigned URLs.
- No configurable credentials.
- No strict authentication mode.
- No multipart upload workflow.
- No broad S3 compatibility claim beyond tested and documented behavior.

## Shared Assumptions

- S3Lab is running locally and listening on `http://127.0.0.1:9000`.
- The smoke test uses a temporary local data directory.
- Python and boto3 are installed by the user for the boto3 recipe.
- Curl is available for issuing the generated presigned requests.
- Requests stay offline and target only the local S3Lab endpoint.
- Clients use path-style addressing and the static local S3Lab credentials `s3lab` / `s3lab-secret`.

## Start S3Lab

Run the server in one terminal:

```powershell
$S3LAB_DATA_DIR = Join-Path $env:TEMP "s3lab-m3-smoke-$([guid]::NewGuid())"
cargo run -- serve --host 127.0.0.1 --port 9000 --data-dir $S3LAB_DATA_DIR
```

Expected startup output includes:

```text
S3 endpoint:  http://127.0.0.1:9000
Data dir:     <temporary data directory>
```

Leave this terminal running while using the smoke commands below.

## Python boto3 Presigned PUT and GET

Run this Python script in a second terminal. It creates a bucket with regular signed SDK calls, generates presigned `PUT` and `GET` URLs, then uses curl against those URLs.

```python
import subprocess

import boto3
from botocore.config import Config

endpoint_url = "http://127.0.0.1:9000"
bucket = "s3lab-presign-boto3-bucket"
key = "hello.txt"
body = "hello from s3lab milestone 3 presigned urls"

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    region_name="us-east-1",
    aws_access_key_id="s3lab",
    aws_secret_access_key="s3lab-secret",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)

s3.create_bucket(Bucket=bucket)

put_url = s3.generate_presigned_url(
    ClientMethod="put_object",
    Params={"Bucket": bucket, "Key": key},
    ExpiresIn=300,
    HttpMethod="PUT",
)

subprocess.run(
    ["curl", "--fail", "--silent", "--show-error", "-X", "PUT", "--data-binary", body, put_url],
    check=True,
)

get_url = s3.generate_presigned_url(
    ClientMethod="get_object",
    Params={"Bucket": bucket, "Key": key},
    ExpiresIn=300,
    HttpMethod="GET",
)

downloaded = subprocess.check_output(
    ["curl", "--fail", "--silent", "--show-error", get_url],
    text=True,
)

print(downloaded)

s3.delete_object(Bucket=bucket, Key=key)
s3.delete_bucket(Bucket=bucket)
```

The output should include:

```text
hello from s3lab milestone 3 presigned urls
```

## Expiration Check

This recipe creates a local object, generates a short-lived presigned `GET` URL, waits until it expires, and confirms that the endpoint rejects it.

```python
import subprocess
import time

import boto3
from botocore.config import Config

endpoint_url = "http://127.0.0.1:9000"
bucket = "s3lab-presign-expiry-bucket"
key = "hello.txt"
body = b"hello before expiration"

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    region_name="us-east-1",
    aws_access_key_id="s3lab",
    aws_secret_access_key="s3lab-secret",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)

s3.create_bucket(Bucket=bucket)
s3.put_object(Bucket=bucket, Key=key, Body=body)

expired_url = s3.generate_presigned_url(
    ClientMethod="get_object",
    Params={"Bucket": bucket, "Key": key},
    ExpiresIn=1,
    HttpMethod="GET",
)

time.sleep(2)

result = subprocess.run(
    ["curl", "--silent", "--show-error", "--output", "-", "--write-out", "\n%{http_code}", expired_url],
    check=False,
    text=True,
    capture_output=True,
)

print(result.stdout)

s3.delete_object(Bucket=bucket, Key=key)
s3.delete_bucket(Bucket=bucket)
```

The final HTTP status should be a client error rather than `200`.

## What This Exercises

These recipes exercise only this local milestone 3 path:

- create one bucket through signed SDK header authentication
- upload one object through a presigned path-style `PUT` URL
- download one object through a presigned path-style `GET` URL
- reject an expired presigned `GET` URL
- delete the object and bucket through signed SDK header authentication

It does not prove broad AWS S3 compatibility. It does not cover virtual-host style addressing, session-token presigned URLs, configurable credentials, strict authentication mode, multipart uploads, bucket policies, ACLs, object tags, encryption headers, range reads, trace persistence/API/UI, full streaming chunk-signature validation, or production storage behavior.
