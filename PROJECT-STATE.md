# Panta — Project State

> This file is the current execution pointer. Update it at every accepted gate.

## Current phase

**MVP implementation — Gate 0**

Architecture baseline is accepted. Implementation has started with the first bounded Gate 0 task.

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

### Current bounded task

**Gate 0.1 — Bootstrap Go skeleton and freeze core ports**

Tracking: GitHub Issue #1

Scope:
- Go module and runnable binary;
- initial module/package boundaries;
- provider-neutral provider ports;
- IndexCore/OpenList ports;
- in-memory test doubles and build/test harness.

Explicitly deferred from Gate 0.1:
- PostgreSQL migrations/schema;
- real 115 adapter;
- real IndexCore/OpenList network calls;
- complex Job Engine implementation;
- auth/share/usage;
- search;
- agent integration;
- UI.

## Planned Gate 0 task sequence

- Gate 0.1 — Skeleton + first ports
- Gate 0.2 — Product DB migrations + Asset/Release/Variant/Copy minimum persistence
- Gate 0.3 — Job Engine minimum durable state model
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

Review the developer PR for Issue #1 against its exact scope, dependency direction, tests and evidence.

Do not authorize Gate 0.2 until Gate 0.1 is accepted (or an explicit architect exception is recorded).
