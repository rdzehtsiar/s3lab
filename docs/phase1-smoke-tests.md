# Phase 1 Smoke Tests

This page documents narrow AWS CLI, Python boto3, AWS SDK for JavaScript v3, and Go SDK smoke recipes for S3Lab Phase 1. It is intended to produce local evidence that the current Phase 1 endpoint can accept a basic bucket and object lifecycle against `localhost`.

This is not a general compatibility claim. Phase 1 smoke evidence only covers the operations exercised below.

## Shared Assumptions

- S3Lab is running locally and listening on a loopback endpoint such as `http://127.0.0.1:9000`.
- The smoke test uses a temporary local data directory.
- The AWS CLI is installed and available as `aws` for the AWS CLI recipe.
- Python and boto3 are installed for the boto3 recipe.
- Node.js and npm are installed for the AWS SDK for JavaScript v3 recipe.
- Go is installed for the Go SDK recipe.
- Clients use dummy credentials. No cloud account is required.
- Requests stay offline and target only the local S3Lab endpoint through `--endpoint-url`, boto3 `endpoint_url`, AWS SDK for JavaScript v3 `endpoint`, or the Go SDK local endpoint configuration.
- Phase 1 accepts signed client requests but does not validate SigV4 signatures yet.
- Phase 1 supports path-style localhost routing, for example `http://127.0.0.1:9000/s3lab-smoke-bucket/object.txt`.
- Virtual-host style routing, presigned URLs, and multipart uploads are deferred.
- Phase 1 preserves valid `x-amz-meta-*` object metadata, normalizes metadata keys to lowercase, returns metadata on `GET` and `HEAD`, and rejects invalid, non-UTF8, or duplicate normalized metadata.

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

Leave this terminal running while using the smoke commands below.

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

## Python boto3 Smoke Recipe

Run this Python script in a second terminal:

```python
import boto3
from botocore.config import Config

endpoint_url = "http://127.0.0.1:9000"
bucket = "s3lab-smoke-boto3-bucket"
key = "hello.txt"
body = b"hello from s3lab phase 1"

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

listed = s3.list_objects_v2(Bucket=bucket)
print([item["Key"] for item in listed.get("Contents", [])])

downloaded = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
print(downloaded.decode("utf-8"))

s3.delete_object(Bucket=bucket, Key=key)
s3.delete_bucket(Bucket=bucket)
```

The output should include:

```text
['hello.txt']
hello from s3lab phase 1
```

## AWS SDK for JavaScript v3 Smoke Recipe

Create a temporary Node.js project in a second terminal:

```powershell
$SmokeDir = Join-Path $env:TEMP "s3lab-js-smoke-$([guid]::NewGuid())"
New-Item -ItemType Directory -Path $SmokeDir | Out-Null
Set-Location $SmokeDir
npm init -y
npm install @aws-sdk/client-s3
```

Create `smoke.mjs` in that directory:

```javascript
import {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

const endpoint = "http://127.0.0.1:9000";
const bucket = "s3lab-smoke-js-bucket";
const key = "hello.txt";
const body = "hello from s3lab phase 1";

const client = new S3Client({
  endpoint,
  forcePathStyle: true,
  region: "us-east-1",
  credentials: {
    accessKeyId: "s3lab",
    secretAccessKey: "s3lab-secret",
  },
});

await client.send(new CreateBucketCommand({ Bucket: bucket }));
await client.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));

const listed = await client.send(new ListObjectsV2Command({ Bucket: bucket }));
console.log((listed.Contents ?? []).map((object) => object.Key));

const downloaded = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
console.log(await downloaded.Body.transformToString());

await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
await client.send(new DeleteBucketCommand({ Bucket: bucket }));
```

Run it:

```powershell
node smoke.mjs
```

The output should include:

```text
[ 'hello.txt' ]
hello from s3lab phase 1
```

## Go SDK Smoke Recipe

Create a temporary Go module in a second terminal:

```powershell
$SmokeDir = Join-Path $env:TEMP "s3lab-go-smoke-$([guid]::NewGuid())"
New-Item -ItemType Directory -Path $SmokeDir | Out-Null
Set-Location $SmokeDir
go mod init s3lab-go-smoke
go get github.com/aws/aws-sdk-go-v2/config github.com/aws/aws-sdk-go-v2/credentials github.com/aws/aws-sdk-go-v2/service/s3
```

Create `main.go` in that directory:

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	ctx := context.Background()
	endpoint := "http://127.0.0.1:9000"
	bucket := "s3lab-smoke-go-bucket"
	key := "hello.txt"
	body := "hello from s3lab phase 1"

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("s3lab", "s3lab-secret", "")),
	)
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		log.Fatal(err)
	}

	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	}); err != nil {
		log.Fatal(err)
	}

	listed, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, object := range listed.Contents {
		fmt.Println(*object.Key)
	}

	downloaded, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer downloaded.Body.Close()

	content, err := io.ReadAll(downloaded.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(content))

	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		log.Fatal(err)
	}

	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		log.Fatal(err)
	}
}
```

Run it:

```powershell
go run .
```

The output should include:

```text
hello.txt
hello from s3lab phase 1
```

## What This Exercises

These recipes exercise only this local Phase 1 path:

- create one bucket
- put one object
- list objects with ListObjectsV2
- get the object
- delete the object
- delete the bucket

It does not prove broad AWS S3 compatibility. It does not cover virtual-host style addressing, presigned URLs, multipart uploads, signature validation, bucket policies, ACLs, object tags, encryption headers, range reads, or production storage behavior.
