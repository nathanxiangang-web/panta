# Panta — Project State

> This file is the current execution pointer. Update it at every accepted gate.

## Current phase

**MVP implementation — Gate 2**

Gate 0 and Gate 1 are formally accepted and closed. Gate 2.1 and Gate 2.2 are accepted and merged. Gate 2.3 is authorized and in progress.

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

Status: **ACCEPTED / CLOSED**

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

## Completed bounded tasks

### Gate 0.3 — Durable Job Engine minimum state and recovery semantics

Status: **ACCEPTED**

Tracking: GitHub Issue #5 / PR #6

Merged:
- squash commit `ebc01317ef3c2b7796e612fb4ae938cf6f18dd66`

Accepted scope:
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

### Gate 0.4 — Mock provider conformance, CI, and Gate 0 closeout

Status: **ACCEPTED**

Tracking: GitHub Issue #7 / PR #8

Merged:
- squash commit `e8a9af1b2344a68450de995886e1ebc55347e87c`

Accepted scope:
- reusable provider-neutral mock provider;
- reusable provider contract/conformance suite;
- bounded Gate 0 integration acceptance;
- automated architecture/import guard;
- GitHub CI with PostgreSQL 16;
- Gate 0 closeout evidence and documentation.

Gate 0 closeout evidence:
- GitHub Actions run `36427876203` succeeded;
- unit/contract/race/vet/build checks succeeded;
- PostgreSQL 16 integration/migration checks succeeded;
- no concrete 115 implementation is required by the core runtime/tests.

## Current bounded task

### Gate 1.1 — Storage connection/binding persistence and root mapping

Status: **ACCEPTED**

Tracking: GitHub Issue #9 / PR #10

Merged:
- squash commit `84dd7223c2f12dcd50f7da32f698218c9df7cfa3`

Accepted scope:
- append-only StorageConnection / StorageBinding migration;
- provider-neutral storage mapping domain;
- PostgreSQL persistence;
- OpenList mount-path normalization;
- unique canonical IndexCore-root mapping;
- real PostgreSQL constraint/round-trip tests.

## Current bounded task

### Gate 1.2 — Typed IndexCore HTTP read client and query contract alignment

Status: **ACCEPTED**

Tracking: GitHub Issue #11 / PR #12

Merged:
- squash commit `0cd355a7917e522c24ccee62be949406d955c492`

Scope:
- refine Panta-owned IndexCore query contracts to match Q4/Q5/Q8/Q9 semantics;
- typed server-side HTTP client;
- Q4 parent hierarchy + opaque cursor;
- Q5 all matches + ambiguity;
- Q8 exact per-root after_seq Journal reads;
- Q9 root status;
- typed remote/transport/malformed-response errors;
- httptest contract coverage.

Explicitly deferred:
- Journal cursor persistence;
- Catalog Projector / Copy projection;
- real OpenList client;
- Mutation Hint/scoped refresh;
- 115/MCP;
- auth/search/agent/UI.

## Current bounded task

### Gate 1.3 — Durable Journal cursor and idempotent unresolved Copy projector

Status: **ACCEPTED**

Tracking: GitHub Issue #13 / PR #14

Merged:
- squash commit `5bcffae7ab24ec8a7fd42d48cdd6ee11e4876e87`

Scope:
- per-StorageBinding durable Journal cursor;
- one-shot ProjectOnce application service;
- resource Journal events -> unresolved Copy PRESENT/REMOVED state;
- atomic Copy mutations + cursor advancement;
- cursor concurrency/fencing;
- copies.storage_binding_id FK hardening;
- restart/idempotency/rollback PostgreSQL evidence.

Explicitly deferred:
- continuous polling/daemon;
- OpenList HTTP client/visibility verification;
- Mutation Hint/scoped refresh;
- Asset/Release/Variant classification;
- 115/MCP;
- auth/search/agent/API/UI.

## Current bounded task

### Gate 1.4 — Controlled OpenList → IndexCore → Panta observation E2E and Gate 1 closeout

Status: **ACCEPTED**

Tracking: GitHub Issue #15 / PR #16

Merged:
- squash commit `607debaacc417ac6a71bf7f72c12676b1a3c47ce`

Pinned external integration:
- IndexCore v0.4.0-alpha.1
- commit `6f0eec85c59bd8cbe55011b0d9e512e0cafd6615`

Scope:
- test-only OpenList-compatible controlled fixture;
- real external IndexCore OpenList Collector / Canonical / Journal path;
- real Panta Q4/Q5/Q8 client and PostgreSQL projector;
- direct known-path acceptance without Search;
- repeat-idempotency and additive-change evidence;
- GitHub Actions observation E2E;
- Gate 1 documentation/CI closeout.

Explicitly deferred:
- production Panta OpenList scanner;
- post-download visibility verifier;
- Mutation Hint/scoped refresh;
- continuous Journal polling;
- 115/MCP/acquisition;
- classification/search/auth/share/agent/API/UI.

Gate 1 closeout evidence:
- PR #16 architect review: ACCEPTED;
- GitHub Actions run `36518514268` fully green;
- PostgreSQL 18 controlled OpenList observation E2E passed;
- pinned IndexCore commit `6f0eec85c59bd8cbe55011b0d9e512e0cafd6615`;
- initial observation, identical NOOP repeat, and additive change all passed;
- direct Q5 known-path flow requires no Search;
- IndexCore and Panta databases remain separate.

## Current bounded task

### Gate 2.1 — Logical Catalog hierarchy read model and explicit absence semantics

Status: **ACCEPTED**

Tracking: GitHub Issue #17 / PR #18

Merged:
- squash commit `fe4342d2252689c665008801259fd081eaabe533`

Acceptance evidence:
- GitHub Actions run `36526195429` fully green;
- deterministic Asset → Release → Variant → Copy hierarchy reads;
- explicit missing-entity and ownership-mismatch semantics;
- unresolved Copies remain outside the logical hierarchy;
- Gate 1 controlled observation E2E remains green.

Scope:
- Catalog hierarchy reads for Asset → Release → Variant → Copy;
- deterministic child listing;
- resourceview logical composition service;
- explicit Asset/Release/Variant absence and ownership mismatch semantics;
- derived PRESENT-Copy availability;
- unresolved Copy remains unresolved and outside logical hierarchy;
- no Search dependency.

Explicitly deferred:
- unresolved Copy classification/binding writes;
- IndexCore Q3 physical enrichment;
- Search/FTS;
- public API/UI;
- 115 acquisition / Mutation Hint;
- auth/quota/share;
- Agent.

## Current bounded task

### Gate 2.2 — Physical resource view with optional Catalog attachment

Status: **ACCEPTED**

Tracking: GitHub Issue #19 / PR #20

Merged:
- squash commit `60467b113dab6e54e321cfd4b09173e6395c5017`

Acceptance evidence:
- GitHub Actions run `36542613715` fully green;
- physical-only resources remain usable without Catalog Copy;
- unresolved Copy remains usable without forced classification;
- classified lineage is attached only when complete and consistent;
- Q5 ambiguity and candidate ordering are preserved;
- IndexCore presence and Copy availability remain independent facts.

Scope:
- known root/path resolves through IndexCore Q5;
- preserve zero/one/multiple matches and Q5 ambiguity;
- optional Copy lookup by canonical physical identity;
- explicit PHYSICAL_ONLY / UNRESOLVED_COPY / CLASSIFIED states;
- classified lineage validation through Variant → Release → Asset;
- IndexCore presence remains canonical and separate from Copy availability;
- no Search dependency.

Explicitly deferred:
- unresolved Copy classification/binding writes;
- identity creation from physical resources;
- Agent classification;
- Search/FTS;
- OpenList/provider access;
- public API/UI;
- acquisition / Mutation Hint / scoped refresh;
- reconciliation worker.

## Current bounded task

### Gate 2.3 — Explicit unresolved Copy → Variant binding with concurrency-safe monotonic semantics

Status: **AUTHORIZED / IN PROGRESS**

Tracking: GitHub Issue #21

Scope:
- bind an existing unresolved Copy to an existing Variant;
- same-target replay is idempotent;
- different-target rebind fails closed;
- concurrency-safe one-winner semantics;
- preserve CopyID, physical identity, StorageBinding, availability and created_at;
- projector availability updates preserve VariantID.

Explicitly deferred:
- automatic/Agent classification;
- reclassification/correction workflow;
- bulk classification;
- Search/FTS;
- Q3 enrichment;
- public API/UI;
- acquisition / 115 / Mutation Hint;
- auth/quota/share.

## Planned Gate 0 task sequence

- Gate 0.1 — Skeleton + first ports — **ACCEPTED**
- Gate 0.2 — Product DB migrations + Asset/Release/Variant/Copy minimum persistence — **ACCEPTED**
- Gate 0.3 — Job Engine minimum durable state model — **ACCEPTED**
- Gate 0.4 — Mock provider + contract test completion + Gate 0 integration acceptance — **ACCEPTED**

The sequence may be refined by an architect decision, but later tasks must not be pulled into an earlier PR without updating project state.

## Gate 1 bounded task sequence

- Gate 1.1 — StorageConnection / StorageBinding persistence — **ACCEPTED**
- Gate 1.2 — Typed IndexCore HTTP read client (Q4/Q5/Q8/Q9) — **ACCEPTED**
- Gate 1.3 — Journal cursor persistence + idempotent unresolved Copy projector — **ACCEPTED**
- Gate 1.4 — Controlled OpenList → IndexCore → Panta observation integration + Gate 1 closeout — **ACCEPTED**

## Planned project sequence

- Gate 0 — Skeleton & contracts — **ACCEPTED**
- Gate 1 — Observation plane: OpenList → IndexCore → Catalog — **ACCEPTED / CLOSED**
- Gate 2 — Resource semantics & direct-path/catalog read flows — **IN PROGRESS**
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

Review the Gate 2.3 PR for Issue #21 against monotonic Copy classification, same-target idempotency, different-target fail-closed semantics, real PostgreSQL concurrency fencing, projector compatibility, and full Gate 1/2.1/2.2 regression.

Do not authorize Gate 2.4 until Gate 2.3 is accepted.
