# Panta — Project State

> This file is the current execution pointer. Update it at every accepted gate.

## Current phase

**MVP implementation — Gate 0**

Gate 0.1 and Gate 0.2 are accepted and merged. Gate 0.3 is authorized and in progress.

## Accepted baseline

- Project name: Panta
- MVP-first: functionality before UI polish
- Team model: 1 Architect + 1 Developer
- OpenList is part of the primary observation path
- IndexCore remains an independent canonical physical-resource kernel
- Panta introduces a separate logical Catalog
- Resource identity model: Asset → Release → Variant → Copy
- Browse/known-path access must work without search
- 115 is the first provider, not the product architecture
- 115 cloud download is an MVP core capability
- Cloud download/share require product login
- Cloud-download result must reach Panta through OpenList visibility + IndexCore scoped refresh, not a full scan
- CodeArts CLI is the initial replaceable Agent Runtime
- Initial backend implementation baseline: Go 1.27.1
- MVP deployment shape: single-process modular monolith
- Product DB: PostgreSQL, kept separate from IndexCore DB
- Product schema uses append-only SQL migrations and fail-closed schema compatibility
- Applied migration history must be an ordered prefix; history gaps/out-of-order states fail closed
- Provider-changing side effects must be represented by a durable Job before execution
- Expired RUNNING job leases become RECOVERY_REQUIRED, never blind automatic retry

## Active gate

### Gate 0 — Skeleton & contracts

Status: **IN PROGRESS**

Goal:
Create the smallest compilable/runnable Panta skeleton with module boundaries and ports before implementing real 115 behavior.

Required outputs:
- module/package skeleton;
- configuration boundary;
- product DB migration mechanism;
- provider contracts;
- IndexCore integration port;
- OpenList integration port;
- Job Engine minimum state model;
- minimal Asset/Release/Variant/Copy schema;
- mock provider;
- unit/contract test harness.

Acceptance:
- core domain compiles/runs without importing the 115 adapter;
- mock provider passes provider contract tests;
- direct-path flow can be represented without Search;
- product DB can be recreated from migrations;
- no code writes to IndexCore DB;
- architecture docs match actual package dependencies.

## Completed bounded tasks

### Gate 0.1 — Bootstrap Go skeleton and freeze core ports

Status: **ACCEPTED**

Tracking: GitHub Issue #1 / PR #2

Merged:
- squash commit `f1cb51fcd8546002bf05d0b5e7f50dff38365d3c`

Accepted scope:
- Go module and runnable binary;
- initial module/package boundaries;
- provider-neutral provider ports;
- IndexCore/OpenList ports;
- in-memory test doubles and build/test harness;
- provider registry identity/capability validation.

### Gate 0.2 — PostgreSQL migrations and minimum Catalog persistence

Status: **ACCEPTED**

Tracking: GitHub Issue #3 / PR #4

Merged:
- squash commit `8d16b9a3692f1f2fb3cab48be6ccbcbacce0646c`

Accepted scope:
- independent Panta PostgreSQL configuration/connection boundary;
- pgx/v5 persistence adapter boundary;
- ordered embedded append-only SQL migrations;
- schema migration checksum/history compatibility;
- fail-closed future/modified/gapped/out-of-order migration detection;
- minimum Asset / Release / Variant / Copy persistence;
- nullable unresolved Copy;
- real PostgreSQL round-trip and constraint tests.

Known follow-up:
- repository CI is not yet present; add before Gate 0 closeout.

## Current bounded task

### Gate 0.3 — Durable Job Engine minimum state and recovery semantics

Status: **AUTHORIZED / IN PROGRESS**

Tracking: GitHub Issue #5

Scope:
- append-only jobs migration;
- provider-neutral jobs domain;
- durable PostgreSQL job repository/state transitions;
- atomic claim and lease ownership;
- explicit RETRY_WAIT;
- RECOVERY_REQUIRED for expired running leases;
- optional product-level idempotency key;
- crash/restart persistence evidence.

Explicitly deferred:
- real worker loop/scheduler;
- provider execution;
- 115/MCP;
- Acquisition Manifest;
- provider-specific retry/backoff/recovery;
- real IndexCore/OpenList integrations;
- auth/usage/search/agent/UI;
- job DAG/event/audit system.

## Planned Gate 0 task sequence

- Gate 0.1 — Skeleton + first ports — **ACCEPTED**
- Gate 0.2 — Product DB migrations + Asset/Release/Variant/Copy minimum persistence — **ACCEPTED**
- Gate 0.3 — Job Engine minimum durable state model — **IN PROGRESS**
- Gate 0.4 — Mock provider + contract test completion + Gate 0 integration acceptance

The sequence may be refined by an architect decision, but later tasks must not be pulled into an earlier PR without updating project state.

## Planned project sequence

- Gate 0 — Skeleton & contracts
- Gate 1 — Observation plane: OpenList → IndexCore → Catalog
- Gate 2 — Resource semantics & direct-path/catalog read flows
- Gate 3 — 115 acquisition + fast IndexCore synchronization
- Gate 4 — Login + share/access + usage accounting
- Gate 5 — Constrained CodeArts agent integration
- Gate 6 — Provider-replacement proof + MVP closeout

## Not authorized yet

Until the relevant gate is accepted, do not implement:
- complex UI;
- payment;
- recommendation system;
- Elasticsearch;
- vector database;
- Kafka / distributed queues;
- microservice split;
- multi-tenant enterprise permissions;
- generalized workflow editor;
- autonomous agent control plane.

## Next architect action

Review the Gate 0.3 PR for Issue #5 against job-state transition safety, atomic claim/lease semantics, restart durability, retry/recovery behavior, PostgreSQL integration evidence and scope control.

Do not authorize Gate 0.4 until Gate 0.3 is accepted (or an explicit architect exception is recorded).
