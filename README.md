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

S3Lab is at the very beginning of development.

The repository contains an early Rust codebase and the first pieces of a local S3-shaped endpoint, but the project should still be treated as experimental and incomplete. APIs, commands, storage layout, responses, tests, and documentation may change quickly.

Do not rely on S3Lab for production workloads. Do not assume general S3 compatibility unless a specific operation and client workflow is covered by tests or documented smoke evidence.

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
