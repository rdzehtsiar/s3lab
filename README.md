# S3Lab

[![Tests](https://github.com/rdzehtsiar/s3lab/actions/workflows/tests.yml/badge.svg)](https://github.com/rdzehtsiar/s3lab/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/rdzehtsiar/s3lab/graph/badge.svg)](https://codecov.io/gh/rdzehtsiar/s3lab)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=rdzehtsiar_s3lab&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=rdzehtsiar_s3lab)

S3Lab is an offline S3 compatibility and protocol debugging lab for engineers who build, test, or troubleshoot S3-compatible workflows locally.

The intended experience is simple: run a local endpoint, point an AWS SDK or CLI at localhost, and inspect S3-shaped behavior without requiring cloud access.

## Vision

S3Lab is meant to help engineers understand and reproduce S3-compatible behavior in a local, deterministic environment.

The product direction is:

- local S3-compatible workflows for development and debugging
- clear visibility into requests, responses, headers, signing behavior, and storage mutations
- evidence-backed compatibility notes for common SDKs and tools
- deterministic local state for repeatable tests and debugging sessions
- offline-first operation with no required cloud account
- clear documentation of what is supported, partial, or intentionally unsupported

S3Lab is not intended to be production object storage, a hosted SaaS service, or a broad replacement for cloud S3.

## Current State

S3Lab is in early milestone development.

The repository contains an early Rust codebase and the first pieces of a local S3-shaped endpoint, but the project should still be treated as experimental and incomplete. APIs, commands, storage layout, responses, tests, and documentation may change quickly.

Do not rely on S3Lab for production workloads. Do not assume general S3 compatibility unless a specific operation and client workflow is covered by tests or documented smoke evidence.

The current local endpoint has narrow contract coverage for path-style bucket and object workflows: create, list, head, get, put, and delete operations for local buckets and objects. Unsigned requests remain accepted for local debugging. SigV4 header-auth requests using `Authorization: AWS4-HMAC-SHA256 ...` are validated against the static local credentials `s3lab` / `s3lab-secret`.

For signed `PUT` object requests, S3Lab checks literal `x-amz-content-sha256` body hashes when present. `UNSIGNED-PAYLOAD` and AWS streaming payload markers are accepted as partial payload validation and traced as partial.

Milestone 3 adds narrow presigned URL support for path-style object `GET` and `PUT` requests against the offline local endpoint. S3Lab validates query-string SigV4 parameters, static local credentials `s3lab` / `s3lab-secret`, and expiration. No cloud account is required. See [Milestone 3 Smoke Tests](./docs/milestone3-smoke-tests.md) for local presigned URL recipes.

Current limitations include no virtual-host style routing, no session-token presigned URLs, no configurable credentials, no strict authentication mode, no multipart uploads, no trace persistence/API/UI, and no full streaming chunk-signature validation. These limitations should not be read as a broad S3 compatibility claim.

## Intended Users

S3Lab is for engineers who need to answer practical questions such as:

- how an SDK or CLI request is shaped before it reaches an S3-compatible service
- why a local S3 workflow behaves differently across machines or CI
- how bucket and object operations are represented over HTTP
- how signing, headers, paths, metadata, and list responses are interpreted
- how to reproduce S3-related bugs without depending on external infrastructure

## Principles

- offline by default
- no telemetry by default
- no cloud account required for the primary local workflow
- no Docker requirement for the primary experience
- deterministic behavior where practical
- compatibility claims backed by tests
- unsupported behavior documented clearly
- cross-platform development and CI friendliness

## Development

S3Lab is currently a Rust project.

Common local checks:

```powershell
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

These checks are expected to stay offline and deterministic.

## Scope

Initial development is focused on a local S3-compatible debugging foundation: bucket and object workflows, S3-shaped HTTP behavior, local persistence, request inspection, and compatibility evidence.

Other storage APIs, hosted services, distributed storage, full IAM behavior, and production object-storage guarantees are outside the initial scope.

## License

Licensed under the Apache License, Version 2.0.
See [LICENSE](./LICENSE.txt).
