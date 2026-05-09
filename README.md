# S3Lab

S3Lab is planned as a fully offline S3 compatibility and protocol debugging lab for engineers who build, test, or troubleshoot S3-compatible workflows locally.

The intended experience is simple: download one binary, run it, point an AWS SDK or CLI at localhost, then inspect requests, debug compatibility issues, replay behavior, snapshot local state, and test failure scenarios without needing cloud access.

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

This repository is currently at the public specification stage. The product direction and milestone plan exist, but the application itself has not been implemented yet.

The immediate focus is to establish clear project intent, scope, limitations, and contribution direction before implementation begins. Early work should favor transparency, evidence-based compatibility claims, and a narrow offline-first product scope.

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

The current roadmap starts with project documentation and specification, then moves toward a minimal local S3-compatible server, request signing diagnostics, presigned URLs, snapshots, multipart uploads, an embedded local UI, compatibility testing, replay, failure injection, and eventually trusted packaged releases.

Compatibility should be treated as something to prove, not something to claim in advance.
