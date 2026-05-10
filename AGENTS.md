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

## Code Quality Requirements

All code changes should be well structured, readable, maintainable, and aligned with clean code and clean architecture practices.

Coding agents must follow these rules:

- Keep module boundaries clear and preserve the intended responsibilities of each crate, package, or module.
- Prefer small, explicit functions with clear names over large procedural blocks.
- Keep protocol parsing, SigV4 logic, storage behavior, replay, failure injection, UI, and CLI concerns separated.
- Avoid hidden side effects, global mutable state, and behavior that makes output nondeterministic.
- Prefer deterministic data structures and stable ordering where output, traces, snapshots, or reports can be observed.
- Write code that is easy to test, with pure logic separated from filesystem, network, clock, random, and terminal concerns when practical.
- Do not introduce abstractions unless they reduce real duplication, clarify ownership, or match the existing architecture.
- Keep errors explainable and actionable instead of panicking on malformed requests, bad configuration, corrupted state, or unsupported S3 behavior.
- Follow Rust best practices for ownership, error handling, typed data, concurrency, and dependency use when working in Rust.
- Keep public APIs conservative and documented enough for future crates or integration tests to use safely.

## Test-First Development Requirements

Use a test-first pattern whenever practical.

Testing expectations:

- Write or update tests before implementing behavior changes when the desired behavior can be specified up front.
- Cover every code change with meaningful tests unless there is a documented reason that testing is impractical.
- Improve test coverage while keeping tests practical, maintainable, and tied to real regression risk.
- Do not add shallow tests only to raise a coverage number; tests should prove behavior, edge cases, error handling, and deterministic output.
- Prefer focused unit tests for protocol parsing, SigV4 canonicalization, signing diagnostics, storage mutations, snapshot logic, replay diffing, failure injection, and configuration behavior.
- Prefer fixture and snapshot-style tests for request traces, replay artifacts, compatibility matrices, JSON output, and UI-independent report formats.
- Include malformed input and negative-path tests where request parsing, signing validation, state recovery, or replay behavior could otherwise panic or silently misreport.
- Keep tests deterministic, offline, and independent of network access, host-specific absolute paths, wall-clock timestamps, and local machine state.
- When changing existing behavior, update or add regression tests that would fail without the fix.
- If a change cannot reasonably be tested in the current task, state the gap clearly in the final response.

## Determinism Requirements

S3Lab should produce deterministic behavior wherever users can inspect or compare output:

- Sort file traversal results and user-visible lists unless the S3 behavior being modeled requires another order.
- Sort diagnostic findings, compatibility results, trace exports, and report output by stable keys.
- Avoid timestamps in default snapshots or golden outputs unless time is the behavior under test.
- Avoid absolute paths in portable output unless explicitly requested.
- Keep JSON key ordering stable where practical.
- Use seeded randomness for controlled failure injection and tests.
- Snapshot-test traces, replay output, compatibility evidence, and report formats where practical.

## Non-Negotiable Product Constraints

Preserve these properties unless the user explicitly changes direction:

- Fully offline by default.
- No telemetry by default.
- No hosted backend required.
- No cloud account required for the primary local workflow.
- Deterministic traces, snapshots, and tests where practical.
- Explainable compatibility, protocol, and signing diagnostics.
- CI-friendly behavior.
- Cross-platform support.
- Evidence-based compatibility claims.
- Clear documentation of unsupported, partial, or intentionally omitted behavior.

Every diagnostic or compatibility finding should make clear:

```text
what happened
where it happened
why it matters
how to fix or reproduce it
what is unsupported or partial, if applicable
```

## Strict Source License Header Rule

All coding agents must include SPDX license headers in source code files they create or edit.

This is a strict must-follow rule:

- When creating a source code file, add an SPDX license header before any code.
- When editing an existing source code file, make sure the file already has an SPDX license header; if it does not, add one as part of the edit.
- Use the file's native comment syntax.
- Use the project license identifier: `SPDX-License-Identifier: Apache-2.0`.
- Do not add duplicate SPDX headers when one already exists.
- Do not add SPDX headers to generated files, vendored third-party files, lockfiles, binary files, or data fixtures unless the project later documents a specific convention for those files.

Examples:

```rust
// SPDX-License-Identifier: Apache-2.0
```

```ts
// SPDX-License-Identifier: Apache-2.0
```

```bash
# SPDX-License-Identifier: Apache-2.0
```

## Documentation Guidance

Human-facing documentation should describe what S3Lab does, who it is for, and the current project state. Avoid exposing unnecessary internal implementation details in user-facing docs.

Agent- and contributor-facing documentation may include implementation direction, constraints, and technical priorities.

Keep docs direct, conservative, and appropriate for an infrastructure debugging tool. Avoid vague claims such as "fully S3-compatible" or "production-ready" unless the repository contains evidence for those claims.

Useful documentation topics as the project matures include:

- architecture
- limitations
- compatibility matrix
- trace format
- snapshot and replay behavior
- failure injection behavior
- configuration reference
- security policy
- contribution guide
- release and binary verification process

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
- Do not revert user changes. If git reports dubious ownership, do not change global git config unless the user asks or the task requires git operations.
- Use imperative mood for commit messages. Prefer subjects such as `Add license metadata`, `Initialize Rust workspace`, or `Document compatibility matrix` instead of past-tense forms such as `Added`, `Initialized`, or `Documented`.
