# Panta — Project State

> This file is the current execution pointer. Update it at every accepted gate.

## Current phase

**MVP architecture initialization**

Repository exists; implementation has not started.

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

## Active gate

### Gate 0 — Skeleton & contracts

Status: **PLANNED**

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

## Planned sequence

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

Freeze Gate 0 contracts and issue the first bounded developer task.
