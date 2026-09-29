# Panta — Decision Log

> Append-only architecture/product decision memory. Do not rewrite history silently; supersede an old decision with a new entry.

## D-001 — MVP before UI

**Status:** Accepted

Panta MVP prioritizes functional end-to-end flows and modular architecture before UI polish.

## D-002 — OpenList is the observation/access layer

**Status:** Accepted

Existing and future storage providers are preferably exposed through OpenList for observation. Panta does not build another provider scanner for IndexCore.

## D-003 — IndexCore owns physical resource truth

**Status:** Accepted

IndexCore remains an independent canonical physical-resource kernel. Panta consumes its read API/Journal and never writes its database.

## D-004 — Panta owns logical resource semantics

**Status:** Accepted

Panta Catalog models logical identity as:

```text
Asset → Release → Variant → Copy → IndexCore resource
```

This prevents provider paths/files from becoming product identity.

## D-005 — Search is optional

**Status:** Accepted

Known-path browse/resolve, direct acquisition, and physical-resource inspection must work without Catalog search.

## D-006 — Separate observation and control planes

**Status:** Accepted

Observation:

```text
Storage → OpenList → IndexCore → Catalog
```

Control:

```text
Panta → Job Engine → Provider Contract → Provider Adapter → Storage
```

Provider command success does not override canonical observation.

## D-007 — 115 is the first provider, not the architecture

**Status:** Accepted

115 supplies the first storage/cloud-download/share implementations. Provider-specific behavior must remain behind replaceable contracts.

## D-008 — Cloud-download READY requires canonical confirmation

**Status:** Accepted

A completed provider download is not READY until:
1. the result is visible through OpenList;
2. IndexCore scoped refresh confirms it;
3. Journal/Projector creates or updates the Copy.

## D-009 — Agent Runtime is replaceable

**Status:** Accepted

CodeArts CLI + GLM-5.2 is the initial Agent Runtime. Deterministic product flows must work with the agent disabled.

## D-010 — Git is project memory

**Status:** Accepted

Project context, current state, accepted decisions, contracts, implementation and acceptance evidence must be committed to this repository.

## D-011 — Go is the initial Panta backend implementation baseline

**Status:** Accepted

Panta MVP backend implementation starts with **Go 1.27.1**, aligned with the current IndexCore toolchain to reduce build/runtime/tooling divergence during integration.

This is an implementation/toolchain choice, not a domain coupling decision:

- Panta does not import IndexCore internal packages;
- Panta communicates with IndexCore only through Panta-owned ports and supported external integration contracts;
- provider adapters remain replaceable behind Panta provider contracts;
- the MVP remains a single-process modular monolith until real scaling evidence justifies a split.

The Panta product database remains PostgreSQL as established by the MVP blueprint. Database schema and migrations are handled in a later bounded Gate 0 task, not in Gate 0.1.

## D-012 — Panta owns an independent PostgreSQL schema and append-only migration history

**Status:** Accepted

Panta product persistence uses PostgreSQL through `pgx/v5`.

Schema evolution uses ordered, append-only SQL migrations owned by Panta and recorded in a `schema_migrations` history. Runtime ORM auto-migration/schema generation is not used.

Compatibility is fail-closed:

- missing required migrations are incompatible;
- migrations unknown to the running binary are treated as future schema and are incompatible;
- an older Panta binary must not silently run against a newer product schema;
- applied known migrations must form an ordered prefix of the embedded migration history; gaps/out-of-order history are incompatible and must not be auto-repaired.

Panta product DB configuration remains independent from IndexCore DB configuration. Panta never obtains physical truth by reading or writing IndexCore PostgreSQL directly.

Catalog domain packages remain persistence-agnostic; pgx belongs in the PostgreSQL adapter/infrastructure boundary.

## D-013 — Job Engine is the durable control-plane safety boundary

**Status:** Accepted

All provider-changing operations must be represented by a durable Panta Job before external side effects occur.

The Gate 0 Job Engine is intentionally small and provider-neutral. It is a state/claim/recovery kernel, not a workflow engine, scheduler platform, or autonomous agent controller.

Minimum states:

```text
QUEUED
RUNNING
RETRY_WAIT
RECOVERY_REQUIRED
SUCCEEDED
FAILED
CANCELED
```

Terminal states are `SUCCEEDED`, `FAILED`, and `CANCELED`.

Claiming is PostgreSQL-atomic and lease-based. Only the active lease owner may renew, succeed, or fail a running job.

A lease expiry is ambiguous: an external side effect may already have occurred even if the worker disappeared. Therefore expired `RUNNING` work becomes `RECOVERY_REQUIRED` and is never blindly requeued/re-executed.

Retry timing is explicit durable state, not a blind sleep loop. Provider-specific reconciliation/backoff remains outside the generic Job Engine.

An optional product-level idempotency key prevents duplicate tracked command submission without embedding provider-specific identifiers in the generic job model.


## D-014 — Active job leases are fenced by claim generation and database time

**Status:** Accepted

A running Job lease is identified by more than worker name.

Every successful claim increments the Job attempt/generation. Active-lease mutations must prove:
- Job ID;
- lease owner;
- expected claim generation;
- currently unexpired lease.

This prevents a stale execution from an older claim from mutating a newer claim even when both use the same worker ID.

PostgreSQL time is authoritative for lease-expiry authorization and expired-lease recovery eligibility. Caller-supplied timestamps must not be able to revive an expired lease or force early recovery.

The existing `attempt_count` is the MVP fencing generation. A separate lease token is not required unless later provider execution proves a stronger primitive is necessary.


## D-015 — Panta consumes IndexCore through its external Query/Hint contracts only

**Status:** Accepted

Panta treats IndexCore as an independent infrastructure service and never imports IndexCore internals or accesses its PostgreSQL database directly.

For the observation plane:
- Q4 hierarchy reads back known-root browsing;
- Q5 canonical path resolution backs direct known-path access;
- Q8 per-root Journal is the canonical projection feed;
- Q9 root status may support health/generation awareness where needed.

Journal cursor semantics are owned by the IndexCore HTTP contract: `after_seq` is exclusive and the next request reuses the **last event_seq actually observed**, without incrementing it.

IndexCore Query API remains read-only from Panta's perspective.

Mutation Hint is a separate trusted internal transport. A successful Hint acceptance is only a durable signal and never counts as proof that a resource is canonically visible. Canonical success must still be observed through IndexCore Query/Journal.

Panta owns consumer DTOs, typed errors, cursors, and projection state. It does not reuse IndexCore internal Go types.


## D-016 — Panta preserves IndexCore query semantics instead of simplifying them away

**Status:** Accepted

Panta's IndexCore ports are consumer-owned, but they must faithfully preserve the semantics of the accepted external IndexCore contract.

In particular:
- Q4 is hierarchy-by-parent with opaque generation-bound pagination; it is not a path browse API and not a flattened root listing.
- Q5 path resolution returns all matches plus an ambiguity flag. Panta must never silently choose one canonical-path match.
- Resource presence remains an explicit canonical state rather than a boolean that would erase retained/tombstone meaning.
- Q8 Journal ordering is per root only. The HTTP `after_seq` cursor is exclusive; consumers reuse the last event sequence actually observed and never increment it.
- Stable remote error categories remain typed and distinguishable from transport and malformed-response failures.

Gate 0 placeholder contracts may be refined when real external semantics prove them insufficient. Preserving an obsolete simplified contract is not considered backward compatibility when it would hide correctness-critical behavior.


## D-017 — Journal projection commits Copy state and cursor atomically

**Status:** Accepted

Panta's Catalog projection is derived from IndexCore canonical Journal and must be replay-safe.

For each StorageBinding:
- the durable projection cursor is the last Journal event_seq whose Copy mutations are committed;
- IndexCore Q8 is called outside any Panta database transaction using exactly that cursor as after_seq;
- the fetched batch is validated before mutation;
- Copy mutations and cursor advancement commit in one PostgreSQL transaction;
- the transaction compares/locks the expected cursor so concurrent projectors cannot overwrite newer progress.

A crash or failure must leave both Copy projection and cursor at the previous committed state.

Resource-added, resource-updated, resource-renamed, and resource-moved project physical Copy availability as PRESENT. Resource-removed projects REMOVED. Root lifecycle events advance the cursor but do not bulk-change child Copy availability.

Journal payload is not authoritative for physical or logical identity. The projector uses StorageBinding root mapping, event type, and canonical resource_id only.

The projector never creates Asset/Release/Variant identity. Newly observed physical resources remain unresolved Copies with variant_id NULL.

Continuous polling/scheduling is not part of this decision or Gate 1.3.


## D-018 — IndexCore owns OpenList collection in the observation plane

**Status:** Accepted

Panta does not add a second production OpenList tree-walking client for Gate 1.

The observation boundary is:

```text
OpenList-compatible storage view
        ↓
IndexCore OpenList Collector
        ↓
Canonical Inventory + Journal
        ↓
Panta IndexCore Query client + Catalog Projector
```

IndexCore owns collection, normalization, canonical identity, completeness safety, and Journal production. Panta consumes canonical physical truth through IndexCore and projects product-owned Copy state.

Panta's existing OpenList VisibilityPort / AccessPort remains a later control/acquisition-side contract. It may be implemented when Gate 3/4 needs post-download visibility verification or access resolution, but it must not become a second canonical observation scanner.

Gate 1 closeout uses a controlled OpenList-compatible fixture and a real, externally executed IndexCore binary pinned to an accepted version. Panta never imports IndexCore internals to obtain this evidence.

OpenList absence is not physical deletion. The accepted IndexCore OpenList collector is additive-safe unless stronger completeness evidence exists.


## D-019 — Copy classification is an explicit monotonic binding by default

**Status:** Accepted

Panta classification attaches an existing unresolved Copy to an existing Variant.

The default transition is monotonic:

```text
variant_id = NULL
        ↓
explicit binding command
        ↓
variant_id = target Variant
```

Rules:
- a missing Copy is not created by classification;
- a missing Variant is not created by classification;
- repeating the same Copy → Variant binding is idempotent;
- attempting to bind an already-classified Copy to a different Variant fails closed;
- silent reclassification is forbidden;
- CopyID, IndexCore physical identity, StorageBindingID, and physical availability are preserved.

Concurrency must be fenced by the persistence boundary so two different target Variants cannot both win.

The Catalog binding operation owns VariantID. The observation projector owns physical availability. Projector replay must preserve existing classification.

Any future correction/reclassification capability requires a separately authorized operation rather than weakening this default rule.


## D-020 — Acquisition Manifest records intent; Job Engine owns execution safety

**Status:** Accepted

Panta separates acquisition intent from execution state.

The Acquisition Manifest records:
- what source is requested;
- which StorageBinding and target path should receive it;
- optional logical Asset / Release / Variant association;
- optional future product user linkage;
- linkage to a durable Job when execution is authorized;
- coarse acquisition milestone.

The durable Job Engine remains the sole owner of execution claim, lease, retry, recovery, and terminal execution safety.

Therefore:
- the Manifest must not duplicate Job lease/attempt/retry state;
- provider-changing side effects still require a durable Job before execution;
- Manifest existence alone never authorizes a provider side effect;
- provider task references and execution attempts are added only in later Gate 3 work;
- direct acquisition may exist without logical classification;
- classification remains optional product semantics, not a prerequisite for acquisition intent.

Gate 3.1 creates and validates acquisition intent only. It does not execute 115, OpenList, IndexCore Hint, or any other external side effect.


## D-021 — Acquisition becomes executable only after atomic Manifest + Job activation

**Status:** Accepted

A PENDING Acquisition Manifest is durable intent, not executable work.

Before any provider-changing acquisition side effect is allowed, Panta must commit one atomic activation that:
- creates exactly one generic durable `ACQUISITION` Job in `QUEUED`;
- links that Job to the Manifest;
- transitions the Manifest from `PENDING` to `ACTIVE`.

The commit boundary must guarantee:
- no orphan QUEUED acquisition Job;
- no ACTIVE Manifest without its durable linked Job.

The acquisition Job payload is a minimal versioned reference to `manifest_id`; it does not duplicate source_ref, target path, credentials, or provider-specific execution input.

Activation replay is idempotent by Manifest identity. Once a Manifest is ACTIVE and correctly linked, the durable linked Job is authoritative even if a retry proposes another Job ID.

The acquisition application layer may depend on Panta's provider-neutral Jobs domain for this orchestration. It must not duplicate Job lease/retry/recovery semantics.

Provider execution remains forbidden in Gate 3.2. Later workers may act only after this activation commit is durable.


## D-022 — StorageBinding maps provider scope, OpenList mount, and IndexCore root independently

**Status:** Accepted

A Panta StorageBinding connects three coordinate systems:

```text
provider target scope
OpenList mount path
IndexCore root_id
```

These values are related by explicit product configuration and must never be derived from one another.

Rules:
- OpenList mount paths retain their own normalized path semantics.
- IndexCore root IDs remain canonical physical-index identifiers.
- Provider target scope is opaque adapter-owned text and is stored separately.
- Observation-only bindings may omit provider scope.
- Acquisition requires an explicit provider scope.
- Provider adapters must not query Panta persistence to discover this mapping.
- StorageConnection.provider_type is the provider registry identity used for later adapter selection.

This separation prevents provider execution from depending on accidental equivalence between provider paths, OpenList mounts, and IndexCore roots.


## D-023 — Provider side effects require durable task linkage; uncertain starts fail closed

**Status:** Accepted

Panta crosses the external downloader boundary through a durable, provider-neutral provider-task linkage.

Rules:
- one MVP acquisition Manifest maps to at most one provider task;
- if a durable provider task reference exists, replay polls that task and never calls StartDownload again;
- provider task references remain opaque and are not parsed by the acquisition domain;
- provider task status does not replace Panta Job state or Acquisition Manifest milestone state;
- provider-reported success is not Panta READY;
- if StartDownload succeeds but the returned provider task reference cannot be durably recorded, Panta treats the result as an uncertain external side effect;
- uncertain external side effects must fail closed and must not be converted into blind automatic recreation.

This boundary exists so crash/restart behavior cannot accidentally duplicate provider-side downloads.


## D-024 — Provider execution is connection-scoped, not ProviderID-scoped

**Status:** Accepted

Provider implementation identity and authenticated provider session identity are different concerns.

```text
ProviderID     = which adapter implementation
ConnectionID   = which StorageConnection
CredentialRef  = which opaque secret/session reference backs that connection
```

Rules:
- ProviderID alone must never select authenticated provider state.
- The same ProviderID may serve multiple StorageConnections.
- Acquisition/domain code may carry CredentialRef only as opaque metadata; secret contents remain outside product state.
- Provider adapters must not query Panta persistence to discover credentials.
- Secret/session resolution happens at the composition/provider-session boundary.
- A missing or mismatched connection/credential binding fails closed before provider side effects.

This decision is required before the real 115 adapter because a real downloader is authenticated, while Gate 3.4 intentionally proved only the provider-neutral side-effect mechanics.
