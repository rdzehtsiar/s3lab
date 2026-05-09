# AGENTS

This file gives coding agents working in this repository the project context and operating constraints needed to make useful changes.

## Project Context

S3Lab is planned as an offline S3 compatibility and protocol debugging lab. Its purpose is to help engineers inspect, reproduce, replay, snapshot, and test S3-compatible behavior locally.

The repository is currently in the public specification stage. There is no implemented application yet. Do not assume an existing architecture, crate layout, package manager, frontend framework, or test harness unless it is present in the repository.

## Product Boundaries

Keep the project focused on local S3-compatible workflows.

In scope:

- local S3-compatible server behavior
- AWS SDK and CLI compatibility testing
- request and protocol inspection
- SigV4 diagnostics
- deterministic snapshots and replay
- controlled failure injection
- offline local UI
- single-binary distribution goals

Out of initial scope:

- production object storage
- cloud-hosted SaaS
- Azure Blob or GCS API support
- full IAM or identity provider behavior
- Kubernetes operator work
- distributed storage

## Implementation Guidance

When implementation begins, prefer choices that preserve the product promise:

- offline by default
- no telemetry by default
- deterministic state and tests
- reproducible traces and snapshots
- evidence-based compatibility claims
- clear failure explanations for protocol and signing issues
- minimal setup for local users

The plan currently recommends a Rust core with an embedded local web UI and a single static binary. Treat that as direction from the project plan, but still verify the current repository state before adding tooling or structure.

## Documentation Guidance

Human-facing documentation should describe what S3Lab does, who it is for, and the current project state. Avoid exposing unnecessary internal implementation details in user-facing docs.

Agent- and contributor-facing documentation may include implementation direction, constraints, and technical priorities.

## Compatibility Claims

Do not describe S3Lab as generally "S3-compatible" without test evidence. Prefer precise phrasing such as "planned S3-compatible behavior" or name the specific operations and clients that are verified.

When adding compatibility work, include or update tests and documentation that show what is known to work, what is partial, and what is unsupported.

## Working Rules

- Inspect the existing repository before making structural changes.
- Keep changes scoped to the current request.
- Do not introduce frameworks, package managers, generated projects, or large dependencies without a clear need.
- Preserve the offline-first posture.
- Prefer deterministic tests over environment-dependent behavior.
- Document limitations as first-class project information.
