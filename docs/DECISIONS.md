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


## D-025 — 115 Downloader maps provider scope to saveDirID and info_hash to task identity

**Status:** Accepted

The MVP concrete 115 Downloader adapter uses the pinned Go library `github.com/SheltonZhu/115driver v1.3.5`.

Mapping:

```text
ProviderID = "115"

DownloadRequest.Source.Value
    -> exact URI passed to 115 offline download

DownloadRequest.Target.Scope
    -> 115 destination directory ID (wp_path_id / saveDirID)

DownloadRequest.Target.Path
    -> Panta expected observation path only
       never converted into provider directory identity

TaskReference.Value
    -> 115 offline task info_hash
```

Rules:
- source URI is never normalized, truncated, or rewritten by the adapter;
- deterministic Source Resolver owns future source canonicalization;
- provider destination is never derived from OpenList mount, IndexCore root, or Target.Path;
- one StartDownload creates exactly one offline URI task;
- provider task status is mapped from 115 task state only and never implies Panta READY;
- cancellation deletes the offline task only, never downloaded files;
- Panta uses the 115driver Go library directly rather than CLI/MCP subprocesses for the runtime adapter.

This keeps 115-specific semantics fully behind the provider contract and preserves the existing control-plane/observation-plane separation.


## D-026 — Provider-stage outcome commits Manifest milestone and Job state atomically

**Status:** Accepted

The durable ACQUISITION Job spans the full acquisition workflow, not only the provider RPC.

Provider-stage outcome mapping:

```text
PROVIDER_IN_PROGRESS
  Manifest ACTIVE
  Job RUNNING -> RETRY_WAIT

PROVIDER_SUCCEEDED
  Manifest ACTIVE -> AWAITING_VISIBILITY
  Job RUNNING -> RETRY_WAIT

PROVIDER_FAILED
  Manifest ACTIVE -> FAILED
  Job RUNNING -> FAILED

PROVIDER_CANCELED
  Manifest ACTIVE -> CANCELED
  Job RUNNING -> CANCELED

UNCERTAIN / RECOVERY
  Manifest ACTIVE -> RECOVERY_REQUIRED
  Job RUNNING -> RECOVERY_REQUIRED
```

Rules:
- Manifest and linked Job are updated in one PostgreSQL transaction.
- Provider success is not Job SUCCEEDED and is never Manifest READY.
- The same ACQUISITION Job continues into visibility/canonical stages; no second visibility Job is created.
- The currently RUNNING Job remains fenced by owner, attempt generation, and database-time lease validity.
- Exact committed outcome replay is idempotent and does not rewrite timestamps.
- Conflicting outcome replay fails closed.
- RECOVERY_REQUIRED remains non-terminal and is not normally claimable.

This prevents provider-stage completion from leaving product milestone and control-plane execution state out of sync.


## D-027 — Claim generation and failure retry budget are separate Job counters

**Status:** Accepted

D-014 remains correct that active Job leases require a monotonically increasing claim generation plus database-time lease validity. Gate 3.7 refines how that generation is stored.

The two concerns are now explicitly separate:

```text
claim_attempts
    monotonically increasing claim generation
    incremented on every successful ClaimNext
    unbounded
    used as the stale-worker fencing token

attempt_count
    failure retry count (Go: jobs.Job.FailureCount)
    incremented only by RetryAt
    bounded by max_attempts
```

Consequences:
- normal provider polling, visibility polling, and later workflow-stage rescheduling do not consume failure retry budget;
- RetryAt is the only generic Job Engine operation that consumes the failure budget;
- a newer claim generation fences every older worker even when the owner string is reused;
- provider success may return the same ACQUISITION Job to RETRY_WAIT without risking exhaustion of the failure budget;
- RECOVERY_REQUIRED remains outside ordinary ClaimNext.

Migration `0010_job_claim_generation.sql` preserves upgrade-time fencing by moving the pre-v10 `attempt_count` value into `claim_attempts` and resetting the new failure counter to zero:

```sql
UPDATE jobs
SET claim_attempts = attempt_count,
    attempt_count = 0;
```

This supersedes only D-014's implementation note that the old `attempt_count` column itself was the claim-generation token. D-014's fencing principle remains unchanged.


## D-028 — Manifest target_path is binding-relative for OpenList observation

**Status:** Accepted

For acquisition visibility checks, OpenList addressing is derived only from the configured OpenList mount and the Manifest's normalized target path:

```text
OpenList request path
    = Join(StorageBinding.openlist_mount_path, Manifest.target_path)
```

Rules:
- `openlist_mount_path` is the OpenList mount coordinate;
- `Manifest.target_path` is the path inside that binding's observed namespace;
- provider_scope is never used to derive the OpenList path;
- IndexCore root identity is never used to derive the OpenList path;
- provider_scope is never derived from the OpenList path;
- the joined path must stay absolute, normalized, and beneath/equal to the configured mount.

Examples:

```text
mount "/"    + target "/downloads/item" -> "/downloads/item"
mount "/115" + target "/downloads/item" -> "/115/downloads/item"
```

This preserves D-022/D-025's separation between provider target coordinates, OpenList observation coordinates, and IndexCore canonical root identity.


## D-029 — Panta does not directly verify OpenList during acquisition

**Status:** Accepted — **supersedes D-028 for acquisition verification**

D-028 incorrectly elevated OpenList from IndexCore's provider/collector dependency into a direct Panta acquisition dependency.

The accepted observation boundary remains D-018:

```text
Storage
  -> OpenList
  -> IndexCore Collector
  -> Canonical + Journal
  -> Panta
```

Therefore, after a provider reports download success, Panta does **not** call OpenList to prove visibility.

Instead:

```text
Manifest AWAITING_VISIBILITY
        ↓
StorageBinding.indexcore_root_id
+ Manifest.target_path
        ↓
IndexCore trusted Mutation Hint
        ↓
IndexCore-owned OpenList scoped verification
        ↓
Canonical + Journal
        ↓
Panta confirmation
```

Exact Mutation Hint mapping:

```text
root_id   = StorageBinding.indexcore_root_id
scope_key = Manifest.target_path
reason    = POSSIBLE_CHANGE
```

The following coordinates remain separate and must not be derived from one another:

```text
provider_scope          provider-side mutation target
openlist_mount_path     IndexCore/OpenList observation configuration
indexcore_root_id       canonical IndexCore root identity
manifest.target_path    directory scope inside the selected binding/root
```

`AWAITING_VISIBILITY` now means the provider stage is complete and Panta is waiting to hand the affected scope to the **IndexCore-owned observation pipeline**. It does not authorize a direct Panta -> OpenList stat.

After IndexCore accepts the trusted Hint, Panta may advance to `AWAITING_CANONICAL`, but Hint acceptance is only durable verification work ingress. It is never Canonical truth and never READY.

Repeated identical Mutation Hints are intentionally safe at-least-once signals: IndexCore coalesces them through its accepted DirtyScopeWork state machine. A duplicate Hint may advance signal metadata but cannot duplicate the provider download or directly create Canonical truth.

D-008's READY principle remains unchanged in meaning: the acquired result must become observable through the OpenList-backed IndexCore pipeline and then be canonically confirmed through IndexCore Query/Journal before Panta marks READY. Panta itself does not need a separate OpenList verification hop.


## D-030 — AI-assisted development reconstructs truth from Git, not chat context

**Status:** Accepted

Panta is developed through long-running AI-assisted architecture and implementation sessions. Accumulated chat context is not reliable enough to act as project memory by itself.

A compact canonical memory file is therefore maintained at:

```text
docs/AI-ARCHITECTURE-MEMORY.md
```

Before designing, authorizing, implementing, or reviewing a Gate, the Architect/AI must reconstruct project truth in this order:

```text
AI-ARCHITECTURE-MEMORY
    ↓
PROJECT-STATE
    ↓
newest relevant DECISIONS
    ↓
PROJECT-CONTEXT / MVP-BLUEPRINT
    ↓
current Issue / PR
    ↓
exact code + tests + exact-head CI
```

Rules:
- chat memory never overrides current Git;
- a newer explicit accepted decision supersedes an older decision/prose/Issue/PR statement;
- contradictions are resolved explicitly and then synchronized across project-memory docs;
- an external integration must be checked against its current accepted contract before a new Panta Gate is written;
- Git must be searched for an existing owner before introducing a new module or responsibility;
- project memory is synchronized after accepted architecture changes and after any discovered context-drift incident;
- team topology remains exactly 1 Architect + 1 Developer unless an explicit later decision changes it.

The purpose is not to duplicate all historical detail. The compact memory keeps only current invariants, dangerous superseded assumptions, active Gate state, and the required reconstruction protocol.

This decision was introduced after a context-drift incident in which direct Panta -> OpenList acquisition verification was proposed despite the already accepted D-018 IndexCore-owned observation boundary. D-029 corrected the architecture; D-030 prevents the same class of drift from becoming process-normal.


## D-031 — Acquisition result identity is one direct child of target_path

**Status:** Accepted

Gate 3.8 established that `Manifest.target_path` is the directory scope handed to the IndexCore-owned scoped observation pipeline.

Canonical acquisition confirmation must not guess which child in that directory belongs to one Manifest.

The provider-neutral identity is therefore:

```text
StorageBinding.indexcore_root_id
+ Manifest.target_path
+ Manifest.expected_name
```

Semantics:

```text
Manifest.target_path
    target directory / IndexCore scoped-refresh directory

Manifest.expected_name
    exact direct-child name expected to appear under target_path
```

Rules:
- `expected_name` may be absent at initial Manifest creation;
- before provider success advances to `AWAITING_VISIBILITY`, a valid expected name must be frozen;
- if intent already supplied expected_name, a provider-reported name must match exactly;
- if intent did not supply expected_name, a provider-reported result name may fill it atomically with provider-success handoff;
- once frozen, expected_name is immutable through the acquisition workflow;
- a successful provider task with neither predeclared nor reported result name cannot advance;
- result identity is descriptive only and does not prove physical/canonical presence;
- provider file IDs, provider directory IDs, OpenList paths, timing windows, Journal ordering and "only new item" heuristics must never substitute for this identity.

A valid expected_name is one direct path segment: valid bounded UTF-8, nonblank, no NUL, no slash/backslash separator, and not `.` or `..`. Exact spelling/case is preserved.

The pinned 115 adapter may map only `OfflineTask.Name` into the provider-neutral result name. `FileId` and `DirId` remain provider-private and are not IndexCore identity.

Gate 3.10 may later resolve the exact canonical child path from this frozen identity; D-031 itself authorizes no Q5 lookup and no READY transition.


## D-031 — Canonical acquisition confirmation requires a durable result locator

**Status:** Accepted

`Manifest.target_path` is a directory scope, not the final acquired resource path. `Manifest.expected_name` is intentionally nullable. Panta must therefore never identify an acquisition result by listing the target directory and choosing a candidate.

Before an acquisition may leave the provider stage for IndexCore observation, Panta must have one durable provider-neutral top-level result name:

```text
valid provider-observed TaskStatus.ResultName
        >
Manifest.ExpectedName fallback
```

The persisted result locator is:

```text
Manifest.result_name
```

and the future exact canonical candidate path is:

```text
Join(Manifest.target_path, Manifest.result_name)
```

Rules:
- result_name is one safe basename/path segment, never a full path;
- provider FileId/DirId/task reference are not IndexCore identity;
- provider-observed result name wins over the expected-name fallback;
- an invalid provider-observed result name fails closed rather than falling back silently;
- if neither source provides a valid name, provider success cannot advance automatically to AWAITING_VISIBILITY and requires explicit recovery;
- once persisted, result_name is immutable;
- same-value replay is idempotent and different-value replay conflicts;
- result_name is locator evidence only, not Canonical truth and never READY.

The 115 adapter may source this field only from the pinned upstream offline task `Name`. It must not fabricate the name from magnet metadata, URL text, provider scope, Target.Path, FileId, or DirId.

Gate 3.9 captures this locator. Exact IndexCore Q5/Journal confirmation remains a later Gate.


## D-032 — result_name is the durable acquisition locator; expected_name remains intent/fallback

**Status:** Accepted

This decision resolves the duplicate D-031 entries and is authoritative wherever they conflict.

The earlier D-031 interpretation that reused and mutated `Manifest.expected_name` as the final acquired-result locator is superseded.

The durable model is:

```text
Manifest.expected_name
    optional request-time expectation / fallback hint

Manifest.result_name
    provider-stage durable top-level acquired-result locator
```

Canonical candidate identity for later confirmation is:

```text
StorageBinding.indexcore_root_id
+ Manifest.target_path
+ Manifest.result_name
```

Rules:

1. `target_path` remains the directory scope refreshed by IndexCore.
2. `expected_name` remains request intent and is not rewritten by provider execution.
3. `result_name` is a separate nullable persistence field introduced by Gate 3.9.
4. A valid provider-observed result name takes precedence over `expected_name`.
5. `expected_name` is used only when the provider reports no result name.
6. If the provider reports an invalid nonblank result name, fail closed; do not silently fall back.
7. If the provider reports no name and expected_name is absent/invalid, provider success cannot advance automatically.
8. Once persisted, `result_name` is immutable; same-value replay is idempotent and different-value replay conflicts.
9. `result_name` is locator evidence only. It is never Canonical truth and never READY.
10. Provider FileId/DirId, OpenList paths, timing, Journal ordering, newest-item and only-item heuristics cannot substitute for `result_name`.

For the pinned 115 adapter:
- successful `OfflineTask.Name` may populate provider-neutral `TaskStatus.ResultName`;
- a blank/absent upstream name yields SUCCEEDED with no ResultName, allowing acquisition-level `expected_name` fallback;
- invalid nonblank/unbounded names fail closed;
- FileId and DirId remain provider-private.

Persistence:
- migration v11 adds nullable `acquisition_manifests.result_name`;
- existing v10 rows are preserved with `result_name = NULL`;
- no database default is allowed;
- the provider-success transition, not a historical migration rewrite, enforces that new automatic progression to observation has a resolved result locator.

Gate 3.8 remains scoped only by `indexcore_root_id + target_path`. The Mutation Hint does not carry result_name. Exact canonical resolution using result_name is deferred to Gate 3.10.

Issue #42 is the authoritative Gate 3.9 contract. Issue #41 is superseded and must not be used for implementation or acceptance.


## D-033 — READY is anchored to one canonical projected Copy

**Status:** Accepted

Gate 3.9 provides a deterministic pre-canonical locator:

```text
StorageBinding.indexcore_root_id
+ Manifest.target_path
+ Manifest.result_name
```

Gate 3.10 turns that coordinate into one stable product result only after both IndexCore canonical truth and Panta Journal projection agree.

READY requires:

```text
exact default-visible Q5 match
        ↓
one PRESENT IndexCore resource
        ↓
existing Q8 Journal Projector
        ↓
exact PRESENT Panta Copy
        ↓
optional monotonic Copy -> Manifest.variant_id binding
        ↓
Manifest.result_copy_id = Copy.ID
Manifest READY
Job SUCCEEDED
```

Rules:
- a path is a coordinate, not identity;
- zero Q5 matches is normal pending work;
- Q5 ambiguity or multiple matches is never guessed and requires recovery;
- the canonical match must echo the exact requested root/path and be PRESENT;
- acquisition must not create or upsert Copy directly;
- D-017 remains authoritative: Q8 Journal + Projector owns Copy creation and physical availability;
- canonical confirmation may advance the existing projector by at most one bounded page per invocation;
- if the exact Copy is not yet projected/PRESENT, the same Acquisition Job returns to RETRY_WAIT without consuming failure budget;
- if Manifest.variant_id is set, the exact Copy must be monotonically bound to that Variant before READY; if no VariantID exists, unresolved Copy is allowed;
- READY atomically stores an immutable `result_copy_id` and marks the same ACQUISITION Job SUCCEEDED;
- `result_copy_id` is the durable historical acquisition result link; later Copy availability changes do not rewrite acquisition history.

Migration v12 adds nullable `acquisition_manifests.result_copy_id` referencing `copies(copy_id)`, with no default and no historical backfill. New READY writes require a result_copy_id while historical pre-Gate-3.10 rows remain upgrade-compatible.

Gate 3.10 does not add a continuous worker daemon, direct OpenList access, direct IndexCore database access, Search-based guessing, or a second Copy projection path.

## D-034 — One claimed ACQUISITION Job executes at most one persisted Manifest stage

**Status:** Accepted

Gate 3.10 completed the bounded provider-neutral, observation, and canonical stage components. Gate 3.11 wires them without replacing their owners.

Stage routing is determined exclusively by the currently persisted Acquisition Manifest:

```text
ACTIVE
  -> bounded ExecutionStepService.Execute
  -> atomic ProviderOutcomeService.Commit

AWAITING_VISIBILITY
  -> bounded RefreshStep.Submit

AWAITING_CANONICAL
  -> bounded CanonicalConfirmation.Confirm
```

Rules:
- one ACQUISITION Job spans all stages; no stage-specific duplicate Job;
- one acquired Job claim generation grants **at most one** stage invocation, regardless of how quickly that stage completes;
- the next stage requires a separately claimed Job; no same-invocation cascade or sleep/poll loop;
- stage selection is based on durable Manifest.State, not guessed provider status, elapsed time, OpenList state, or a transient in-memory cursor;
- active work requires the linked ACQUISITION Job, matching owner and claim generation and the existing database-time fenced commit;
- a terminal Job + corresponding terminal Manifest is a committed result/replay, not a fresh lease-authorized execution; it does no external work;
- provider result-name propagation and D-026 outcome translations remain closed;
- uncertain provider side effects must follow explicit recovery, never blind StartDownload replay;
- normal provider polling, Hint/IndexCore propagation and canonical pending do not consume FailureCount; ClaimAttempts remains the generation fence;
- provider execution, IndexCore Mutation Hint, Journal Projector, and Copy state retain their accepted owners;
- no direct Panta-to-OpenList acquisition visibility validation.

Gate 3.11 builds a bounded one-claim coordinator and controlled multi-claim evidence **without** an automatic scheduler/worker daemon. Later runtime wiring and cadence require a separately authorized Gate. No new database migration is required by this decision.

## D-035 — ACQUISITION-only claiming and atomic linked expired-lease recovery

**Status:** Accepted

Gate 3.11's one-claim dispatcher is accepted. Before enabling an autonomous worker, two Job Engine integration invariants are required:

- An acquisition worker may claim only `Job.Type == ACQUISITION`, using an atomic type-scoped claim that never leases and discards unrelated Job types. Preserve ClaimAttempts, failure-budget and DB-time lease semantics of the existing Job Engine.
- An expired RUNNING ACQUISITION Job is a **linked Manifest + Job recovery transition**, not a standalone Job status update.

Automatic expired acquisition recovery is allowed only for a fully validated `ACTIVE`, `AWAITING_VISIBILITY`, or `AWAITING_CANONICAL` Manifest linked to the correct ACQUISITION Job. Lock Manifest before Job, check expiry by PostgreSQL time, then atomically set both to `RECOVERY_REQUIRED`; leave malformed/missing/conflicting pairs unchanged for explicit diagnosis.

The generic expired-lease sweep must not independently change the state of an ACQUISITION Job, or it would violate the accepted D-034 terminal-pair contract.

No provider task reference, START_RESERVED fence, result_name, result_copy_id, claim generation or failure count is rewritten by this recovery. Nothing automatically repeats a provider mutation.

Gate 3.12 implements these two bounded prerequisites with PostgreSQL evidence. It does not authorize a polling worker daemon, process runtime wiring, OpenList verification, Source Resolver, API or UI.
