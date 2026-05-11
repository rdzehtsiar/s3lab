# S3Lab

[![Tests](https://github.com/rdzehtsiar/s3lab/actions/workflows/tests.yml/badge.svg)](https://github.com/rdzehtsiar/s3lab/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/rdzehtsiar/s3lab/graph/badge.svg)](https://codecov.io/gh/rdzehtsiar/s3lab)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=rdzehtsiar_s3lab&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=rdzehtsiar_s3lab)

S3Lab is an offline S3 compatibility and protocol debugging lab for engineers who build, test, or troubleshoot S3-compatible workflows locally.

The intended experience is simple: run the local endpoint, point an AWS SDK or CLI at localhost, then inspect and reproduce S3-shaped behavior without needing cloud access.

## What We Are Building

S3Lab is not meant to be production object storage and it is not positioned as another general-purpose S3 emulator. The goal is a local lab for understanding and reproducing S3-compatible behavior.

The project is intended to provide:

- a local S3-compatible endpoint for development and testing
- an offline inspector for understanding requests and responses
- compatibility evidence for common SDKs and tools
- reproducible local state through snapshots
- request replay for debugging regressions
- controlled failure scenarios for CI and resilience testing
- a self-contained offline user experience

## Who It Is For

S3Lab is for engineers who need to answer practical questions such as:

- why an SDK request failed against an S3-compatible service
- whether a client behaves consistently across local and CI environments
- how signing, headers, redirects, multipart uploads, or presigned URLs are being interpreted
- how a workflow reacts to timeouts, server errors, partial uploads, or other controlled failures
- how to reproduce an S3-related bug without depending on external infrastructure

## Project State

Phase 1 includes a local S3-shaped HTTP server backed by filesystem persistence. It is intended for local development and compatibility smoke testing, not production object storage.

Current Phase 1 behavior supports:

- create, head, list, and delete buckets
- put, get, head, list, and delete objects
- `ListObjectsV2` with `prefix`, `max-keys`, and `continuation-token`
- path-style localhost routing, such as `http://127.0.0.1:9000/example-bucket/object.txt`
- S3-shaped XML responses and XML error responses
- local filesystem persistence in the configured data directory

This is not a general S3 compatibility claim. Compatibility should be treated as something to prove with focused tests and documented evidence.

## Phase 1 Limitations

Phase 1 is a narrow local endpoint for implemented bucket and object operations. The following capabilities are deferred and are not implemented in Phase 1:

- SigV4 signature validation
- presigned URLs
- virtual-host routing
- multipart upload
- snapshots
- replay
- failure injection
- embedded or local UI

Compatibility evidence is limited to the operations implemented in Phase 1 and the documented smoke recipes. Do not treat Phase 1 behavior as broad AWS S3 compatibility.

## Quick Start

Run the Phase 1 server:

```powershell
cargo run -- serve
```

By default, S3Lab listens at `http://127.0.0.1:9000` and stores local data in `./s3lab-data`.

Point AWS clients at the local endpoint and use dummy credentials. For example, with the AWS CLI:

```powershell
$env:AWS_ACCESS_KEY_ID = "s3lab"
$env:AWS_SECRET_ACCESS_KEY = "s3lab-secret"
$env:AWS_DEFAULT_REGION = "us-east-1"
$env:AWS_EC2_METADATA_DISABLED = "true"

aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket example-bucket
```

Use path-style localhost configuration for SDKs. Phase 1 accepts signed local requests with dummy credentials, so no cloud account is required.

See [Phase 1 Smoke Tests](./docs/phase1-smoke-tests.md) for narrow AWS CLI, boto3, AWS SDK for JavaScript v3, and Go SDK recipes that exercise the implemented bucket and object lifecycle.

## Local Verification

Run the same local, credentials-free checks used by CI before opening a change:

```powershell
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo llvm-cov --workspace --codecov --remap-path-prefix --output-path codecov.json
```

## Project Principles

- offline first
- no cloud service required
- no Docker requirement for the primary experience
- no telemetry by default
- deterministic behavior where possible
- compatibility claims backed by tests
- clear documentation of known limitations

## Scope

The first product target is local S3-compatible development and debugging. Other storage APIs, hosted SaaS features, distributed storage, full identity systems, and production object storage are out of scope for the initial project.

## Roadmap

The current roadmap continues from the Phase 1 local endpoint toward request signing diagnostics, presigned URLs, snapshots, multipart uploads, an embedded local UI, compatibility testing, replay, failure injection, and eventually trusted packaged releases.

Compatibility should be treated as something to prove, not something to claim in advance.

## License

Licensed under the Apache License, Version 2.0.
See [LICENSE](./LICENSE.txt).
