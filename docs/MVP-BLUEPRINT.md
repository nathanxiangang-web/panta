# Panta MVP Blueprint v0.1

> First implementation blueprint. Intentionally bounded: enough architecture to support modular staged development without prebuilding the final product.

## 1. MVP objective

Panta v0.1 proves an end-to-end resource lifecycle on top of existing mature infrastructure.

The MVP is successful when a user can:
- browse/resolve an existing resource by known directory/path without using search;
- view that physical resource in a stable logical resource model;
- submit a supported source while logged in;
- let 115 cloud download acquire it;
- notify IndexCore of the affected root/scope after provider completion;
- let the IndexCore-owned OpenList collector perform bounded scoped verification;
- see canonical IndexCore/Journal evidence make the resulting Copy available in Panta;
- create a provider share while logged in;
- use the same product domain with provider-specific code isolated behind contracts.

UI polish is not an MVP acceptance criterion.

## 2. System context

```text
                   ┌────────────────────────────┐
                   │        Panta API           │
                   │ application + jobs + auth  │
                   └─────────────┬──────────────┘
                                 │
              ┌──────────────────┼───────────────────┐
              │                  │                   │
              ↓                  ↓                   ↓
          Catalog           Acquisition          Agent Port
              │                  │                   │
              │                  ↓                   ↓
              │           Provider Contracts    CodeArts CLI
              │                  │
              │             115 Adapter
              │                  │
              │       115driver v1.3.5
              │                  ↓
              │              115Drive
              │                  │
              │                  ↓
              │               OpenList
              │                  ↓
              │        IndexCore OpenList Collector
              │                  ↓
              │          Canonical + Journal
              │                  ↓
              └──────── Catalog Projector
```

Two planes are deliberately separate:

### Observation plane

```text
Storage → OpenList → IndexCore → Journal → Catalog
```

Used to decide what physically exists.

### Control plane

```text
Panta command → Job Engine → Provider Contract → Provider Adapter → Storage
```

Used to request side effects.

A control-plane success does not automatically override observation truth.

## 3. MVP domain model

### 3.1 Asset

Logical product/resource identity.

Minimum fields:
- asset_id
- canonical_name
- category
- status
- created_at / updated_at

### 3.2 Release

A version/revision/channel of an Asset.

Minimum fields:
- release_id
- asset_id
- version_raw
- version_normalized nullable
- version_scheme
- channel
- release_date nullable
- source_ref nullable
- status

Version schemes must not assume SemVer only:
- SEMVER
- DATE
- REVISION
- UPSTREAM_ID
- CUSTOM
- NONE

### 3.3 Variant

A particular form of a Release.

Minimum fields:
- variant_id
- release_id
- variant_key
- attributes JSON
- status

Examples:
- Windows x64;
- GGUF Q4_K_M;
- 2160p / BluRay / Dolby Vision.

### 3.4 Copy

Physical availability of a Variant.

Minimum fields:
- copy_id
- variant_id nullable during unresolved import
- indexcore_root_id
- indexcore_resource_id
- storage_binding_id
- availability
- created_at / updated_at

A physical IndexCore Resource may exist before Panta can identify its Asset/Release/Variant.

That must not prevent browse/path access.

## 4. Resource-entry paths

Search is not mandatory.

Panta must support at least these independent inputs:

```text
A. Known root/path
      ↓
IndexCore hierarchy / resolve
      ↓
Physical Resource Context

B. Catalog identity
      ↓
Asset / Release / Variant
      ↓
Copies

C. Direct source
URL / magnet / supported input
      ↓
Acquisition Job

D. Agent-assisted source discovery
      ↓
Structured candidate
      ↓
Acquisition Job
```

A and C must work even if catalog search is disabled.

## 5. Read model

### 5.1 Physical resource view

Backed by IndexCore:
- resource_id
- root_id
- canonical_path
- parent
- name
- size
- mtime
- hash/content type when available
- presence

Use cases:
- browse;
- known path;
- inspect a file;
- access an unclassified historical resource.

### 5.2 Logical resource view

Backed by Panta Catalog:
- Asset;
- Releases;
- Variants;
- Copies;
- metadata / source evidence.

Use cases:
- resource detail;
- version distinction;
- variant availability;
- update/subscription later.

The two views can link to each other but neither replaces the other.

## 6. OpenList + IndexCore boundary

OpenList is required in the MVP observation path.

Panta does not implement another 115 scanner.

Initial flow:

```text
115Drive
   ↓
OpenList 115 mount
   ↓
IndexCore openlist Collector
   ↓
Canonical Inventory
   ↓
Journal
   ↓
Panta Catalog Projector
```

Panta must not:
- read OpenList DB directly;
- call OpenList directly to decide acquisition visibility/canonical presence;
- create a second production OpenList scanner or known-path verification lane;
- read IndexCore DB directly;
- infer physical deletion merely because a product record is absent.

For acquisition synchronization, Panta sends a trusted Mutation Hint to IndexCore. IndexCore then owns OpenList scoped verification and Canonical/Journal production.

## 7. Storage bindings

Panta needs a mapping between product storage connections and IndexCore roots.

Minimum concept:

```text
StorageConnection
  - id
  - provider_type
  - credential_ref
  - status

StorageBinding
  - id
  - storage_connection_id
  - openlist_mount_path
  - indexcore_root_id
  - status
```

This allows provider replacement without changing logical resource identity.

## 8. Provider contracts

MVP contracts should stay narrow.

### 8.1 StorageProvider

Needed MVP operations:
- stat/verify target;
- resolve provider target when required;
- obtain access URL when supported;
- capability report.

Do not duplicate OpenList browsing in the provider contract unless a real control-plane need proves necessary.

### 8.2 DownloaderProvider

Needed MVP operations:
- create task;
- get task status;
- cancel task if supported;
- capabilities.

Retry/recreate policy belongs to the Panta application/Job Engine. A concrete Downloader adapter must not blindly recreate an external task on its own.

### 8.3 ShareProvider

Needed MVP operations:
- create share;
- inspect share;
- revoke share;
- capabilities.

115 is the first adapter proving these ports. As of Gate 3.6, only the concrete 115 Downloader slice is implemented; Share remains deferred.

## 9. Acquisition flow

```text
logged-in user/source
      ↓
Source Resolver
      ↓
Acquisition Manifest
      ↓
Job Engine
      ↓
DownloaderProvider
      ↓
115 cloud download
      ↓
provider reports completion
      ↓
durable result_name
(provider ResultName > ExpectedName fallback)
      ↓
Manifest AWAITING_VISIBILITY
      ↓
Panta → trusted IndexCore Mutation Hint
  root_id   = StorageBinding.indexcore_root_id
  scope_key = Manifest.target_path
  reason    = POSSIBLE_CHANGE
      ↓
Manifest AWAITING_CANONICAL
      ↓
IndexCore-owned OpenList scoped verification
      ↓
Canonical confirmation / Journal
      ↓
Catalog Projector
      ↓
Copy READY
```

### Required rules

`provider task completed` is not the same as `Copy READY`.

IndexCore Hint `202 Accepted` is not canonical confirmation and is not `READY`.

Panta does not directly call OpenList to verify acquisition visibility. OpenList remains inside the IndexCore-owned observation pipeline.

Before provider success may advance to observation, Panta must have one durable top-level `result_name`. It must never guess the acquired object by listing the target directory. The future exact canonical candidate path is `Join(target_path, result_name)`.

READY requires canonical confirmation from IndexCore Query/Journal plus the corresponding Panta Copy projection/association.

### No blind sleep

A short delay may be part of scheduling/retry policy, but the success criterion is canonical IndexCore evidence, not “wait 10 seconds and assume.”

## 10. Acquisition Manifest

For Panta-created acquisitions, capture enough identity before download so the result does not need to be guessed again.

Minimum:
- manifest_id
- user_id
- source_type
- source_ref
- expected_name nullable
- target_storage_binding_id
- target_path/scope
- asset_id nullable
- release_id nullable
- variant_id nullable
- job_id
- state
- created_at

If Asset/Release/Variant are known before acquisition, bind them deterministically after IndexCore confirms the resulting resource.

## 11. Catalog Projector

A dedicated module consumes IndexCore changes and updates Copy state.

Responsibilities:
- journal offset per root;
- resource-added → create/update unresolved or manifest-bound Copy;
- resource-moved/renamed → update physical locator facts;
- resource-updated → refresh physical metadata;
- resource-removed → update Copy availability;
- idempotent replay.

The Projector does not invent Asset/Release/Variant identity.

## 12. Direct-path flow

Must work without Catalog search:

```text
user selects root/path
      ↓
Panta IndexCore adapter
      ↓
Q4 hierarchy or Q5 resolve
      ↓
Physical Resource Context
      ↓
optional catalog binding
      ↓
access / inspect / classify
```

Acceptance example:
A physical resource that is not yet classified into Asset/Release/Variant can still be found by known path and inspected.

## 13. Resource detail / version distinction

Logical resource detail is assembled around an Asset:

```text
Asset
 ├─ Release 2.0
 │   └─ Variant Q4_K_M
 │       └─ Copy → IndexCore resource
 └─ Release 2.1
     ├─ Variant BF16
     └─ Variant Q4_K_M
         ├─ Copy → 115
         └─ Copy → future provider
```

The MVP API/data model must distinguish:
- Asset missing;
- Asset exists, requested Release missing;
- Release exists, requested Variant missing;
- Variant exists but has no PRESENT Copy;
- requested Variant has one or more PRESENT Copies.

This distinction is required even if the initial UI is minimal.

## 14. Login / quota boundary

MVP requires product login for:
- cloud download;
- share.

Read/browse policy can remain more permissive.

Panta user identity must not equal 115 identity.

A minimal usage ledger should exist for:
- CLOUD_DOWNLOAD;
- SHARE_CREATE;
- AGENT_TASK (when enabled).

MVP may set effective price to zero.

## 15. Access and share

### Access

The access gateway may use OpenList 302 or provider direct URL when supported.

Panta should expose one product-level access operation; provider-specific URLs remain behind the adapter/gateway.

### Share

Share is not “reuse the temporary 302 URL.”

```text
logged-in user
    ↓
Panta Share Service
    ↓
ShareProvider
    ↓
provider share
```

## 16. Agent MVP boundary

Agent integration is deliberately later than deterministic core flows.

Runtime:
- CodeArts CLI initially;
- GLM-5.2 initially;
- replaceable behind an Agent Port.

First authorized agent tasks:
1. Source classification/resolution when rules are insufficient.
2. Release/version extraction from official/source pages.
3. Historical-resource classification into Asset/Release/Variant candidates.
4. Metadata enrichment.
5. Verification of another agent's structured result.

Agent output must be structured and validated.

No agent direct write to:
- IndexCore;
- Product DB;
- provider credentials;
- provider side effects.

## 17. MVP modules

Recommended initial module boundaries:

```text
panta/
├── cmd/ or app/
├── internal/
│   ├── catalog/
│   ├── resourceview/
│   ├── acquisition/
│   ├── jobs/
│   ├── auth/
│   ├── usage/
│   ├── share/
│   ├── providers/
│   │   ├── contracts/
│   │   └── registry/
│   ├── integrations/
│   │   ├── indexcore/
│   │   ├── openlist/   # optional access-layer integration only; not acquisition truth
│   │   └── agent/
│   └── projector/
├── providers/
│   └── 115/
├── migrations/
├── tests/
└── docs/
```

Exact language/package layout may be refined in Gate 0; dependency direction may not.

## 18. Gate plan

### Gate 0 — Skeleton & contracts

Build:
- repository/module skeleton;
- domain entities;
- DB migrations;
- provider ports;
- mock provider;
- IndexCore contracts plus any future OpenList access-layer port kept separate from canonical observation;
- Job Engine minimum state;
- test harness.

Accept when:
- domain does not import 115;
- mock provider passes contract tests;
- DB rebuild works;
- direct-path use case is representable without search.

### Gate 1 — Observation plane

Build:
- storage binding;
- IndexCore read adapter;
- Journal consumer/projector;
- initial OpenList→IndexCore integration configuration;
- unresolved physical Copy projection.

Accept when:
- real or controlled OpenList resources become IndexCore resources;
- journal replay is idempotent;
- Panta can browse/resolve a known path;
- no Catalog search is required.

### Gate 2 — Resource semantics

Build:
- Asset / Release / Variant / Copy persistence;
- binding APIs/services;
- logical resource detail read model;
- minimal catalog lookup (simple PostgreSQL lookup/FTS only if useful).

Accept when:
- one Asset can have multiple Releases;
- one Release can have multiple Variants;
- one Variant can have multiple Copies;
- an unclassified physical resource remains usable through physical view.

### Gate 3 — 115 acquisition & fast sync

Build:
- 115 Downloader adapter;
- Source Resolver for deterministic source types;
- Acquisition Manifest;
- durable provider-task side-effect fence;
- connection-scoped provider session/credential boundary;
- durable provider-neutral acquisition result locator;
- trusted IndexCore Mutation Hint integration;
- acquisition Job stage handoff/retry;
- IndexCore canonical confirmation / Journal completion path.

Accept when:
- supported source creates a real 115 cloud-download task;
- provider completion queues the affected `indexcore_root_id + target_path` through the trusted IndexCore Hint contract;
- Panta performs no direct acquisition-time OpenList verification;
- IndexCore performs the bounded OpenList-backed scoped verification;
- Canonical/Journal evidence causes the expected Copy to become READY;
- no normal full-root scan is required;
- restart/failure leaves explicit recoverable state.

### Gate 4 — Login, access, share, usage

Build:
- minimal auth;
- usage ledger/quota;
- access gateway;
- 115 Share adapter.

Accept when:
- unauthenticated cloud-download/share is rejected;
- authenticated flow works;
- share can be revoked;
- usage is recorded;
- user identity is independent of provider credential identity.

### Gate 5 — Agent integration

Build:
- CodeArts Agent Port;
- strict task contracts;
- structured response validation;
- first Source/Release/Curator task;
- retry/invalid-refusal handling.

Accept when:
- core deterministic flows work with agent disabled;
- agent cannot directly mutate provider/DB/IndexCore;
- malformed/refusal output is classified and retryable;
- source/release result contains evidence.

### Gate 6 — Provider replacement proof & MVP closeout

Build/test:
- second mock or real provider adapter;
- provider capability variation;
- full end-to-end regression;
- documentation/state closeout.

Accept when:
- disabling/removing 115 adapter does not break domain tests;
- second provider works without modifying Asset/Release/Variant/Search/Acquisition domain semantics;
- architecture documentation matches code;
- MVP limitations are explicit.

## 19. Explicitly out of scope before MVP closeout

- polished UI;
- recommendation engine;
- vector database;
- Elasticsearch unless PostgreSQL proves insufficient;
- payment;
- enterprise multi-tenancy;
- distributed queue;
- microservices split;
- autonomous agent scheduler replacing the Job Engine;
- generalized provider/plugin marketplace;
- complex media transcoding.

## 20. MVP architectural acceptance

Panta MVP is architecturally acceptable only if all are true:

1. Known-path resource access works with search disabled.
2. Existing storage is observed through OpenList and canonically confirmed by IndexCore.
3. Product logical identity is Asset/Release/Variant/Copy, not raw path.
4. 115-specific implementation is isolated behind provider contracts.
5. Cloud-download completion is handed to the IndexCore-owned OpenList observation pipeline and canonically confirmed by IndexCore before READY. Panta does not directly verify OpenList during acquisition.
6. Product DB never becomes a second physical-resource truth.
7. Agent can be removed without breaking deterministic resource flows.
8. A second provider can be added without rewriting product domain modules.


## 21. AI architecture reconstruction protocol

Long-running AI-assisted development must not depend on accumulated chat context.

Before any new Gate is designed, authorized, or reviewed, reconstruct current truth from:

```text
docs/AI-ARCHITECTURE-MEMORY.md
        ↓
PROJECT-STATE.md
        ↓
docs/DECISIONS.md
        ↓
PROJECT-CONTEXT.md
        ↓
this blueprint
        ↓
current Issue / PR
        ↓
exact code + tests + CI
```

If an older section of this blueprint, an old Issue/PR, or chat memory conflicts with a newer explicit accepted Decision, the newer Decision wins. The contradiction must then be repaired in the project-memory documents rather than carried forward implicitly.

Do not create a new module or external integration lane until Git has been searched for an already accepted owner of that responsibility.
