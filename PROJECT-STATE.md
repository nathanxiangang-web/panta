# Panta — Project State

> This file is the current execution pointer. Update it at every accepted gate.

## Current phase

**MVP implementation — Gate 3**

Gate 0, Gate 1, and Gate 2 are formally accepted and closed. Gate 3.1 through Gate 3.10 are accepted and merged. Gate 3.11 is authorized and in progress.

## Accepted baseline

- Project name: Panta
- MVP-first: functionality before UI polish
- Team model: 1 Architect + 1 Developer
- OpenList remains inside the primary observation path as an IndexCore-owned collector dependency
- Panta acquisition does not directly query OpenList for visibility/canonical truth
- IndexCore remains an independent canonical physical-resource kernel
- Panta introduces a separate logical Catalog
- Resource identity model: Asset → Release → Variant → Copy
- Browse/known-path access must work without search
- 115 is the first provider, not the product architecture
- 115 cloud download is an MVP core capability
- Cloud download/share require product login
- Cloud-download result must be handed to the IndexCore-owned OpenList observation pipeline through a trusted scoped Mutation Hint, then canonically confirmed by IndexCore Query/Journal; no Panta direct OpenList verification and no full scan
- CodeArts CLI is the initial replaceable Agent Runtime
- Initial backend implementation baseline: Go 1.27.1
- MVP deployment shape: single-process modular monolith
- Product DB: PostgreSQL, kept separate from IndexCore DB
- Product schema uses append-only SQL migrations and fail-closed schema compatibility
- Applied migration history must be an ordered prefix; history gaps/out-of-order states fail closed
- Provider-changing side effects must be represented by a durable Job before execution
- Expired RUNNING job leases become RECOVERY_REQUIRED, never blind automatic retry
- `docs/AI-ARCHITECTURE-MEMORY.md` is the mandatory compact preflight for AI/Architect sessions; newer accepted Git decisions override old Issue/PR/chat context

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

Status: **ACCEPTED**

Tracking: GitHub Issue #21 / PR #22

Merged:
- squash commit `2aa7469f4f40fe274e715541d8b36961b917b068`

Acceptance evidence:
- GitHub Actions run `36545963720` fully green;
- real PostgreSQL row-lock serialization proves one winner for different-target competition;
- same-target concurrent replay is idempotent;
- CopyID / physical identity / StorageBinding / availability / created_at are preserved;
- ProjectionStore availability updates preserve classification.

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

## Gate 2 closeout

Status: **ACCEPTED / CLOSED**

Gate 2 acceptance is satisfied without a synthetic Gate 2.4:
- one Asset can have multiple Releases;
- one Release can have multiple Variants;
- one Variant can have multiple Copies;
- logical hierarchy reads are deterministic and ownership-safe;
- known-path physical resources remain usable without classification;
- Q5 ambiguity is preserved;
- unresolved Copy classification is explicit, monotonic, idempotent and concurrency-safe;
- Search remains optional and is not required for direct-path or Catalog identity flows.

## Current bounded task

### Gate 3.1 — Durable Acquisition Manifest and validated target intent

Status: **ACCEPTED**

Tracking: GitHub Issue #23 / PR #24

Merged:
- squash commit `6637b695386ecd9976921242ff60c8983b0e6dad`

Acceptance evidence:
- GitHub Actions run `36549876927` fully green;
- migration history compatible at version 5;
- real PostgreSQL Manifest round-trip and FK/constraint evidence passed;
- source intent remains opaque and provider-neutral;
- active StorageBinding and optional logical lineage are validated;
- no provider/OpenList/IndexCore/Job execution side effect was introduced.

Scope:
- append-only Acquisition Manifest persistence;
- provider-neutral source intent;
- validated active StorageBinding target;
- normalized target path;
- optional existing Asset / Release / Variant association;
- nullable future user linkage and Job linkage;
- creation starts at PENDING only;
- no provider execution.

Explicitly deferred:
- real 115 adapter;
- DownloaderProvider execution;
- Source Resolver / magnet parsing;
- durable Job submission / worker loop;
- provider task references;
- OpenList visibility verifier;
- Mutation Hint / scoped refresh;
- canonical READY orchestration;
- auth enforcement / usage / API/UI.

## Current bounded task

### Gate 3.2 — Atomic Manifest activation and durable Acquisition Job linkage

Status: **ACCEPTED**

Tracking: GitHub Issue #25 / PR #26

Merged:
- squash commit `aaa1b0f90dfc6bf4c616ad396321dbf1694a7aab`

Acceptance evidence:
- Round 2 closed the pre-linked PENDING Manifest bypass;
- CreateManifest always produces PENDING + job_id NULL;
- append-only migration 0006 enforces PENDING/ACTIVE Job-link invariants;
- atomic activation creates exactly one QUEUED ACQUISITION Job and ACTIVE linkage;
- replay/concurrency converge to one durable Job;
- forced post-Job-insert failure rolls back both records;
- GitHub Actions run `36555568078` fully green;
- migration history compatible at version 6.

Scope:
- one atomic PostgreSQL activation transaction;
- PENDING Manifest → ACTIVE;
- create/link exactly one generic QUEUED ACQUISITION Job;
- minimal versioned Job payload references manifest_id only;
- deterministic Manifest-scoped Job idempotency;
- replay returns existing durable linkage;
- concurrent activation cannot create orphan/duplicate Jobs;
- forced transaction failure rolls back both Manifest activation and Job creation.

Explicitly deferred:
- Job worker/provider execution;
- 115 adapter / DownloaderProvider calls;
- provider task references;
- Source Resolver / magnet parsing;
- OpenList visibility verification;
- Mutation Hint / scoped refresh;
- canonical confirmation and later Manifest transitions;
- auth/quota/API/UI.

## Current bounded task

### Gate 3.3 — Provider target mapping and side-effect-free acquisition execution input

Status: **ACCEPTED**

Tracking: GitHub Issue #27 / PR #28

Merged:
- squash commit `592596acca7ab7b7916de51bad0bc6689d9b0733`

Acceptance evidence:
- explicit provider_scope is independent from OpenList mount and IndexCore root;
- observation-only bindings remain valid with provider_scope NULL;
- acquisition requires ACTIVE binding + ACTIVE connection + explicit provider scope;
- ACTIVE Manifest resolves exactly to provider-neutral DownloadRequest;
- source_ref/provider_scope remain opaque and unchanged;
- no provider registry lookup/provider call/OpenList/IndexCore side effect;
- GitHub Actions run `36598697218` fully green;
- migration history compatible at version 7.

Scope:
- append-only nullable StorageBinding.provider_scope;
- preserve observation-only bindings without provider scope;
- acquisition targets require explicit provider scope;
- active StorageBinding + active StorageConnection validation;
- StorageConnection.provider_type is the future ProviderID;
- side-effect-free ACTIVE Manifest → provider-neutral DownloadRequest resolution;
- source_ref remains opaque and unchanged;
- no provider registry lookup or provider call.

Explicitly deferred:
- provider registry selection;
- DownloaderProvider execution;
- provider task persistence;
- real 115 adapter;
- Source Resolver / magnet normalization;
- Job worker loop;
- OpenList visibility verifier;
- Mutation Hint / scoped refresh;
- canonical confirmation / READY;
- auth/quota/API/UI.

## Current bounded task

### Gate 3.4 — Durable provider-task linkage and restart-safe downloader execution step

Status: **ACCEPTED**

Tracking: GitHub Issue #29 / PR #30

Merged:
- squash commit `1f99a6e2123e48be28ea387d2fe4fc69cf8e14f7`

Acceptance evidence:
- provider-task linkage is durable and provider-neutral;
- START_RESERVED side-effect fence is persisted before StartDownload;
- retry/restart/concurrent execution cannot create a second provider task;
- REFERENCE_KNOWN replay polls the exact durable reference;
- provider task lifecycle remains provider-owned;
- provider success is not READY;
- real registry.Registry satisfies the acquisition provider lookup contract;
- real registry + testprovider + ExecutionStepService composition is proven;
- GitHub Actions run `36606009751` fully green;
- migration history compatible at version 9.

Scope:
- durable provider-neutral provider-task linkage;
- provider registry lookup at the application boundary;
- one-shot DownloaderProvider StartDownload / DownloadStatus execution step;
- existing durable task is always polled, never recreated;
- provider task state maps to provider-neutral execution outcomes only;
- provider success is not READY;
- uncertain side effect after successful StartDownload but failed persistence fails closed;
- restart/replay must not duplicate external provider tasks.

Explicitly deferred:
- real 115 adapter;
- Source Resolver / magnet normalization;
- worker polling loop;
- Manifest transition to AWAITING_VISIBILITY;
- OpenList visibility verifier;
- Mutation Hint / scoped refresh;
- canonical confirmation / READY;
- auth/quota/API/UI.

## Current bounded task

### Gate 3.5 — Connection-scoped downloader session and credential boundary

Status: **ACCEPTED**

Tracking: GitHub Issue #31 / PR #32

Merged:
- squash commit `54a5df99fa5d81df4661d0b8c56888fbe03cac6e`

Acceptance evidence:
- provider execution resolves by ProviderID + ConnectionID + opaque CredentialRef;
- the same ProviderID can back multiple StorageConnections with distinct downloader sessions;
- connection/credential mismatch fails closed before provider calls;
- contracts.SecretResolver freezes the opaque secret lookup boundary;
- secret contents remain outside Manifest, Job payload, and provider-task persistence;
- provider adapters do not query Panta persistence for credentials;
- all Gate 3.4 side-effect fencing remains intact;
- GitHub Actions run `36608071506` fully green;
- no migration change; schema remains version 9.

Scope:
- distinguish ProviderID from StorageConnection session identity;
- resolve downloader by ProviderID + ConnectionID + opaque CredentialRef;
- allow multiple StorageConnections for the same provider implementation;
- fail closed on connection/credential mismatch;
- keep secret contents outside Manifest/Job/provider-task persistence;
- freeze a narrow secret-resolution port for later concrete provider composition;
- preserve all Gate 3.4 side-effect fencing semantics.

Explicitly deferred:
- real 115 adapter / 115driver dependency;
- real secret backend;
- Source Resolver normalization;
- worker loop;
- AWAITING_VISIBILITY transition;
- OpenList visibility verifier;
- Mutation Hint / scoped refresh;
- canonical confirmation / READY;
- auth/quota/API/UI.

## Current bounded task

### Gate 3.6 — Concrete 115 Downloader adapter on pinned 115driver

Status: **ACCEPTED**

Tracking: GitHub Issue #33 / PR #34

Merged:
- squash commit `a8d9bf806bd12a8299b9974dfdcc4535be7046f6`

Acceptance evidence:
- concrete provider isolated in `internal/providers/115`;
- `github.com/SheltonZhu/115driver` pinned to `v1.3.5`;
- exact URI + Target.Scope(saveDirID) + info_hash mapping;
- Source.Value is not normalized/truncated;
- bounded exact-hash status polling;
- 115 status 0/1/2/-1 maps to provider Pending/Running/Succeeded/Failed only;
- cancel deletes task only with `deleteFiles=false`;
- provider success remains distinct from Panta READY;
- GitHub Actions run `36610303120` fully green;
- no migration change; schema remains version 9.

Scope:
- add isolated concrete `internal/providers/115` DownloaderProvider;
- pin `github.com/SheltonZhu/115driver v1.3.5`;
- cookie secret bytes -> 115driver credential/client construction without network login check;
- StartDownload maps exact URI + Target.Scope saveDirID to one 115 offline task;
- TaskReference is exact 115 info_hash;
- DownloadStatus performs bounded info_hash lookup and maps 115 status 0/1/2/-1;
- CancelDownload removes task only with deleteFiles=false;
- preserve exact source bytes; no source normalization/truncation;
- adapter remains isolated from acquisition/jobs/store/OpenList/IndexCore/session implementation.

Explicitly deferred:
- Source Resolver normalization / magnet rewriting;
- worker loop;
- AWAITING_VISIBILITY transition;
- OpenList visibility verifier;
- Mutation Hint / scoped refresh;
- canonical confirmation / READY;
- live 115 credentials as a CI requirement;
- 115 ShareProvider;
- auth/quota/API/UI.

## Current bounded task

### Gate 3.7 — Atomic provider-stage outcome handoff to Manifest + Job

Status: **ACCEPTED**

Tracking: GitHub Issue #35 / PR #36

Merged:
- squash commit `19121d5b3312cad9ee97155fe5204556a662c89d`

Acceptance evidence:
- provider-stage outcome commits Manifest + linked Job in one PostgreSQL transaction;
- PROVIDER_IN_PROGRESS -> ACTIVE + RETRY_WAIT;
- PROVIDER_SUCCEEDED -> AWAITING_VISIBILITY + RETRY_WAIT on the same ACQUISITION Job;
- provider failure/cancel/recovery mappings are atomic and replay-safe;
- provider success never marks Job SUCCEEDED or Manifest READY;
- exact replay returns Changed=false, including PROVIDER_IN_PROGRESS;
- claim generation and failure retry budget are separate;
- repeated stage polling does not consume failure budget;
- stale claim generations remain fenced;
- v9 -> v10 migration preserves legacy claim generation and resets the new failure counter;
- GitHub Actions run `36657549476` fully green;
- migration history compatible at version 10.

Scope:
- atomic provider-stage Manifest + Job outcome persistence;
- database-time lease fencing;
- idempotent exact replay and fail-closed conflicting replay;
- one ACQUISITION Job spans provider and later visibility/canonical stages;
- claim generation separated from bounded failure retry budget.

Explicitly deferred:
- worker/claim loop;
- polling cadence policy;
- Source Resolver;
- OpenList visibility verifier;
- Mutation Hint / scoped refresh;
- AWAITING_CANONICAL / READY;
- API/UI.

## Current bounded task

### Gate 3.8 — IndexCore trusted Mutation Hint and observation handoff

Status: **ACCEPTED**

Tracking: GitHub Issue #39 / PR #40

Merged:
- squash commit `9de14ab89ce3f7822fae927d68e09711adc24eb7`

Acceptance evidence:
- production `internal/app.HintPort` composes the real IndexCore Hint client into the acquisition-owned port;
- Hint transport remains literal-loopback-only and never follows redirects;
- exact D-029 root/scope/reason mapping;
- no production Panta -> OpenList acquisition verification path;
- 202 Accepted remains non-canonical and never READY;
- accepted Hint atomically commits AWAITING_CANONICAL + RETRY_WAIT on the same Job;
- at-least-once Hint resend is safe before Panta commit and stops after durable handoff;
- GitHub Actions run `36665857635` fully green;
- no migration change; schema remains version 10.

Correction:
- Issue #37 was closed NOT PLANNED because direct Panta -> OpenList acquisition verification violates D-018/D-015.
- D-029 supersedes D-028 for acquisition verification.
- Panta does not directly inspect OpenList in the acquisition path.
- This correction is also frozen in `docs/AI-ARCHITECTURE-MEMORY.md`, `PROJECT-CONTEXT.md`, and `docs/MVP-BLUEPRINT.md`.

Scope:
- implement the Panta-owned typed client for IndexCore's accepted trusted Mutation Hint transport;
- exact Hint mapping: root_id = StorageBinding.indexcore_root_id, scope_key = Manifest.target_path, reason = POSSIBLE_CHANGE;
- preserve IndexCore's loopback-only Bearer-authenticated Hint trust boundary;
- require AWAITING_VISIBILITY Manifest + exact linked fenced ACQUISITION Job;
- accepted Hint atomically hands the same Job to Manifest AWAITING_CANONICAL + Job RETRY_WAIT;
- Hint acceptance remains non-canonical and never READY;
- duplicate Hint after a lost Panta commit is allowed under IndexCore's at-least-once/coalescing semantics;
- no direct Panta OpenList HTTP/API/database access.

Explicitly deferred:
- acquisition result locator capture;
- Q5/Journal canonical confirmation;
- Copy/Projector completion orchestration;
- READY transition;
- acquisition worker loop;
- public API/UI.

## Current bounded task

### Gate 3.9 — Durable acquisition result locator before canonical confirmation

Status: **ACCEPTED**

Tracking: GitHub Issue #42 / PR #43

Merged:
- squash commit `784435d855c3c038131c94d3bebf59e3a9a97f15`

Acceptance evidence:
- separate immutable Manifest.result_name added via migration v11;
- expected_name remains request intent/fallback and is never rewritten by provider execution;
- valid provider ResultName takes precedence over ExpectedName;
- blank provider name permits only a promotable direct-child ExpectedName fallback;
- malformed nonblank provider name fails closed;
- result_name + AWAITING_VISIBILITY + RETRY_WAIT commit atomically;
- same result_name replay is idempotent and different-value replay fails closed;
- 115 FileId/DirId remain provider-private;
- Gate 3.8 root/scope-only Hint semantics remain unchanged;
- GitHub Actions run `36675499153` fully green;
- migration history compatible at version 11.

Scope:
- extend provider-neutral TaskStatus with optional ResultName;
- map pinned 115 OfflineTask.Name only, without fabricating from provider IDs/paths;
- carry result name through the bounded ExecutionStep result;
- add durable immutable Manifest.result_name via migration v11;
- provider success resolves result_name from provider-observed name first, ExpectedName fallback second;
- missing result identity blocks automatic AWAITING_VISIBILITY progression and requires recovery;
- provider success atomically persists result_name + AWAITING_VISIBILITY + RETRY_WAIT;
- no Q5/Journal/OpenList/READY work in this Gate.

Explicitly deferred:
- exact IndexCore canonical confirmation;
- Copy/Projector completion + optional Variant binding;
- READY / Job SUCCEEDED finalization;
- acquisition worker loop;
- public API/UI.

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
- Gate 2 — Resource semantics & direct-path/catalog read flows — **ACCEPTED / CLOSED**
- Gate 3 — 115 acquisition + fast IndexCore synchronization — **IN PROGRESS**
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

Review the Gate 3.11 PR against **Issue #46 / D-034**: exactly one durable Manifest-selected stage per previously claimed ACQUISITION Job; closed ExecutionStep→ProviderOutcome mapping; explicit uncertain-side-effect recovery; trusted IndexCore Hint boundary; canonical READY only through the accepted Q5/Projector/Copy path; fail-closed terminal/inconsistent states; real PostgreSQL multi-claim restart evidence; and Gate 0–3.10 regressions.

Do not authorize Gate 3.12 until Gate 3.11 is accepted and merged.


## Current bounded task

### Gate 3.10 — Canonical confirmation, projected Copy, and READY finalization

Status: **ACCEPTED**

Tracking: GitHub Issue #44 / PR #45

Merged:
- reviewed head `2f8f862eb17ffd1ea5238cba292873e77662c276`;
- squash commit `d211323e7dc30922d9796f57fad611f44b825fe2`.

Acceptance evidence:
- exact IndexCore Q5 candidate + ambiguity fail closed;
- existing Q8 Journal Projector owns Copy creation/availability, one bounded page per invocation;
- exact PRESENT Copy by canonical root/resource/binding;
- optional monotonic Variant binding joins the same PostgreSQL DB-time lease-fenced finalization transaction;
- locked Manifest/Job/Copy cross-check prevents replay-only plans from advancing fresh READY and prevents mismatched Binding/Variant;
- immutable result_copy_id + Manifest READY + same Job SUCCEEDED commit atomically;
- READY exact replay independent of later Binding state, no external calls;
- schema v12, nullable result_copy_id, no default/backfill, historical upgrade-compatible;
- Architect Round 3 ACCEPTED (review `5364884446`);
- exact-head CI `36701499525` all three groups SUCCESS.

Scope:
- build exact canonical candidate path from target_path + result_name;
- use IndexCore Q5 with default PRESENT visibility and preserve ambiguity;
- treat zero matches / not-yet-projected Copy as normal pending RETRY_WAIT;
- advance the existing Q8 Journal Projector by at most one bounded page;
- require the exact PRESENT Copy by root/resource/binding;
- optionally bind the exact Copy to Manifest.variant_id using existing monotonic classification;
- add durable Manifest.result_copy_id via migration v12;
- atomically finalize result_copy_id + Manifest READY + same Job SUCCEEDED;
- no direct Copy creation from acquisition.

Explicitly deferred:
- continuous acquisition worker/scheduler;
- Source Resolver normalization;
- auth/quota/share/access;
- Agent;
- public API/UI.


## Current bounded task

### Gate 3.11 — One-claim acquisition stage dispatcher and controlled end-to-end orchestration

Status: **AUTHORIZED / IN PROGRESS**

Tracking: GitHub Issue #46

Governing decision: D-034.

Scope:
- one already-claimed ACQUISITION Job + current claim generation;
- choose exactly one step from persisted Manifest: ACTIVE provider/outcome, AWAITING_VISIBILITY Hint, AWAITING_CANONICAL canonical confirmation;
- retain existing DB-time-fenced stores, START_RESERVED provider side-effect boundary, IndexCore-owned OpenList observation, D-017 Projector ownership, D-033 READY rules;
- no same-claim chaining to the next stage;
- explicit terminal/recovery and inconsistent-state fail-closed results;
- controlled PostgreSQL multi-claim/restart tests prove the same Job progresses across the stages without duplicate StartDownload or failure-budget consumption;
- no new schema migration planned.

Explicitly deferred:
- automatic ClaimNext loop / worker daemon / process deployment;
- Source Resolver normalization, auth/quota/share/access;
- public API/UI, Agent and Search-dependent behavior.
