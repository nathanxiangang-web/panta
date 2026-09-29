# Acquisition Manifest

Gate 3.1 introduces durable, provider-neutral acquisition intent before any
provider side effect exists. Gate 3.2 adds the atomic boundary that makes that
intent executable.

```text
validated acquisition request
             |
             v
  PENDING Acquisition Manifest
             |
             v
  atomic ACTIVE + Job link
             |
             v
       later Gate: provider
```

## Module boundaries

- `internal/acquisition` owns the Manifest model, closed milestone states,
  target-path normalization, creation service, and persistence port.
- The creation service reads only Panta-owned StorageBinding and Catalog identity
  ports. It does not create Storage or Catalog identity. New acquisition intent
  requires an ACTIVE binding with explicit provider scope and an ACTIVE owning
  StorageConnection.
- `internal/store/postgres` implements Manifest persistence.
- `internal/acquisition` may depend on the Panta-owned, provider-neutral Jobs
  contract for activation and provider contract DTOs for execution input. It
  still cannot depend on Job persistence, provider registry, or adapters.
- Jobs continue to own execution, claim, lease, retry, and recovery behavior.
  The Manifest owns intent, target, optional logical association, and a coarse
  product milestone only.

The acquisition domain cannot import pgx/database/sql, provider execution,
OpenList, IndexCore, Search, Agent, or authentication implementations. Neither
Manifest creation nor activation performs a network call or provider operation.

## Intent validation

Source type and source reference must be non-empty and bounded. Source reference
is stored as opaque input: Gate 3.1 does not parse URLs, magnets, or provider
syntax.

The target identifies a location inside an ACTIVE StorageBinding. It is distinct
from the binding's OpenList mount path. The binding must explicitly configure an
opaque provider scope and its owning connection must be ACTIVE with a valid
provider identity. Target paths:

- are absolute slash paths;
- reject backslashes and `.` / `..` components;
- normalize repeated and trailing slashes;
- are not checked against OpenList existence.

Logical association is optional. If present, it must be shaped and owned as:

```text
Asset -> Release -> Variant
```

A Release requires an Asset and must belong to it. A Variant requires a Release
and must belong to it. Manifest creation never creates Asset, Release, Variant,
or Copy records.

## Persistence

Migration `0005_acquisition_manifests.sql` adds application-supplied Manifest
UUIDs, an ACTIVE StorageBinding target reference, optional Catalog references,
optional future user identity without a user foreign key, and a unique Job
reference used by activation. It introduces no UUID extension or
database-generated UUID default. Migration `0006_enforce_acquisition_activation_link.sql`
requires `PENDING` Manifests to have no Job link and `ACTIVE` Manifests to have
one.

New Manifests are always created in `PENDING` with `job_id=NULL`; the public
creation request cannot pre-link execution state. The frozen state set also reserves
`ACTIVE`, `AWAITING_VISIBILITY`, `AWAITING_CANONICAL`, `READY`, `FAILED`,
`RECOVERY_REQUIRED`, and `CANCELED`.

## Atomic activation

Only a `PENDING` Manifest with no Job link can newly activate. One PostgreSQL
transaction locks the Manifest, creates a generic `QUEUED` Job of type
`ACQUISITION`, links `job_id`, and changes the Manifest to `ACTIVE`. Both records
commit or neither does.

The Job uses application-supplied identity, a positive `max_attempts`, and the
deterministic idempotency key `acquisition:<manifest_id>`. Its versioned payload
contains exactly `schema_version=1` and `manifest_id`; source references, target
paths, credentials, and provider data remain exclusively in their owning model.

An `ACTIVE` replay returns the linked durable Job with `Changed=false`, even if
the retry proposes another Job ID. It does not rewrite timestamps. Missing or
mismatched linkage fails closed. All other Manifest milestones reject activation.
Row locking makes same-ID and different-ID concurrent requests converge on the
single committed link without an orphan Job.

## Side-effect-free execution input

Gate 3.3 resolves only an `ACTIVE` Manifest with a durable Job link. It reads the
explicit StorageBinding and StorageConnection and assembles:

```text
ProviderID       = connection.provider_type
CredentialRef    = connection.credential_ref (opaque reference only)
Source.Scheme    = manifest.source_type
Source.Value     = manifest.source_ref
Target.Scope     = binding.provider_scope
Target.Path      = manifest.target_path
```

The resolver preserves source reference and provider scope exactly. It does not
derive scope from `openlist_mount_path` or `indexcore_root_id`, load credentials,
query the provider registry, invoke a DownloaderProvider, or perform network IO.
Migration `0007_storage_binding_provider_scope.sql` leaves provider scope nullable
for observation-only bindings, has no default, and rejects empty or overlong
non-null values.

## Durable provider-task linkage and the side-effect fence

Gate 3.4 crosses the external side-effect boundary exactly once and records what
it did. Migration `0008_acquisition_provider_tasks.sql` adds one provider-neutral
table, refined by the append-only migration
`0009_provider_task_side_effect_fence.sql`:

```text
acquisition_provider_tasks
  manifest_id        uuid PRIMARY KEY  -> acquisition_manifests
  job_id             uuid NOT NULL UNIQUE -> jobs
  provider_id        text   (bounded opaque registry identity)
  provider_task_ref  text NULL (bounded opaque provider reference)
  state              text   START_RESERVED | REFERENCE_KNOWN
  created_at, updated_at
```

One Manifest maps to at most one provider task and one Job maps to at most one
provider task. Provider identity and reference values are bounded opaque text:
Panta never parses or normalizes them, and the table stores no provider status
because the provider remains authoritative for its own task lifecycle. There are
no database defaults and no provider-specific columns. A conflicting identity
fails closed rather than overwriting a known task reference.

`state` records **side-effect certainty only**, never provider task progress. The
database enforces the invariant so an uncommitted attempt can never look
successful:

```text
START_RESERVED    -> provider_task_ref IS NULL
REFERENCE_KNOWN   -> provider_task_ref IS NOT NULL and non-blank and <= 1024
```

The durable row is therefore written **before** the external call, not after. It
exists in two phases:

```text
no row                                   -> persist START_RESERVED  (exclusive)
START_RESERVED + reference               -> persist REFERENCE_KNOWN
START_RESERVED + no reference            -> fail closed, no start
REFERENCE_KNOWN + same reference         -> idempotent replay
REFERENCE_KNOWN + different reference    -> identity conflict
```

The store performs this decision in one transaction under a per-Manifest
`pg_advisory_xact_lock`, because transactional uniqueness alone cannot serialize
concurrent claimers: two executions could each observe "no row" before either
commits and both would be authorized to start a task. Exactly one claimer can
observe a fresh reservation.

## One bounded execution step

`acquisition.ExecutionStepService` performs one bounded provider step for one
fenced `RUNNING` `ACQUISITION` Job:

```text
fenced Job identity
        |
        v
ACQUISITION Job payload + Manifest linkage validation
        |
        v
Gate 3.3 ExecutionInput (ProviderID, CredentialRef, DownloadRequest)
        |
        v
provider registry lookup + Downloader capability + descriptor identity
        |
        v
durable provider task state
   REFERENCE_KNOWN   -> DownloadStatus for the exact stored reference
   START_RESERVED    -> fail closed: no reference is known
   no row            -> claim START_RESERVED, then StartDownload once,
                        then commit the opaque reference
```

`StartDownload` is reachable only through a newly won `START_RESERVED` claim, so
it can happen at most once per Manifest even under concurrent executions, a lost
reference, or a crash between the external success and the durable commit.

The step fails closed before any provider call when the Job is not an
`ACQUISITION` Job, its payload does not match the Manifest, the Manifest is not
linked to that Job, the Job is not `RUNNING`, the lease owner or attempt does not
match the requesting lease, the lease has expired, the provider is not
registered, the registry entry has no Downloader port, the descriptor identity
differs from the resolved `ProviderID`, or the durable linkage contradicts the
requested identity. Provider-specific errors stay attributed but are never
converted into provider types inside the domain.

Provider task states map only to step outcomes:

```text
PENDING / RUNNING -> PROVIDER_IN_PROGRESS
SUCCEEDED         -> PROVIDER_SUCCEEDED
FAILED            -> PROVIDER_FAILED
CANCELED          -> PROVIDER_CANCELED
```

Provider success is not Manifest `READY`. `READY` still requires OpenList
visibility, canonical confirmation, and Copy mutation, none of which Gate 3.4
performs. An empty or invalid returned task reference and an unknown provider task
state both fail closed.

## Uncertain external side effects

If `StartDownload` succeeds but the returned reference cannot be committed, the
step returns `ErrExecutionSideEffectUncertain` and the `START_RESERVED` row
remains durable. Because a start attempt is known while no reference is, that
outcome must be reconciled by an operator rather than treated as ordinary retry:
every later execution observes the reservation and fails closed instead of
starting a second provider task. This holds within the process, across service and
repository reconstruction, and under concurrent executions.

Reservations are never cleared automatically. A crash before the external call and
a crash after it are indistinguishable from durable state alone, so releasing a
`START_RESERVED` row is an explicit operator decision. The marker carries no lease,
attempt, retry, or provider-status semantics; the Job Engine remains the only
owner of execution state.

Two situations leave a reservation behind:

```text
StartDownload returned a reference, but the reference commit failed
StartDownload returned an error, so no external effect could be confirmed
```

Both are treated identically, conservatively: a returned error does not prove the
provider did not accept the request, so the Manifest stays fenced and every later
execution fails closed. The provider is still invoked at most once.

Recovery is an operator procedure, not an automatic retry:

```text
1. Inspect acquisition_provider_tasks for the Manifest: if a row is
   REFERENCE_KNOWN, nothing is wrong and executions poll it normally.
2. For a START_RESERVED row, determine from the provider whether a task
   already exists for the Manifest's DownloadRequest.
3. If a task exists, supply its opaque reference so the reservation
   transitions to REFERENCE_KNOWN.
4. If no task exists, the operator may delete the START_RESERVED row,
   which re-authorizes exactly one future start attempt.
```

Step 4 is why reservations are not auto-cleared: deleting the row is a claim that
no external effect happened, and only the provider side can establish that.

The claim wait is bounded by `lock_timeout` (5s) on the fence lock. A claim that
cannot hold the fence fails closed with `ErrProviderTaskContention` and grants no
authorization, so a stuck lock can never hang a worker or permit a start.

## Deferred capabilities

Source Resolver/provider syntax normalization, the Job worker loop, Manifest
milestone transition to `AWAITING_VISIBILITY`, the 115 adapter, OpenList
visibility verification, Mutation Hint/scoped refresh, canonical READY
orchestration, auth/quota, and API/UI are separately authorized later work.
