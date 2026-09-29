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

## Durable provider-task linkage

Gate 3.4 crosses the external side-effect boundary exactly once and records what
it did. Migration `0008_acquisition_provider_tasks.sql` adds one provider-neutral
table:

```text
acquisition_provider_tasks
  manifest_id        uuid PRIMARY KEY  -> acquisition_manifests
  job_id             uuid NOT NULL UNIQUE -> jobs
  provider_id        text   (bounded opaque registry identity)
  provider_task_ref  text   (bounded opaque provider reference)
  created_at, updated_at
```

One Manifest maps to at most one provider task and one Job maps to at most one
provider task. Both values are bounded opaque text: Panta never parses or
normalizes them, and the table stores no provider status because the provider
remains authoritative for its own task lifecycle. There are no database defaults
and no provider-specific columns. A conflicting identity fails closed rather than
overwriting a known task reference.

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
durable provider task known?
   no  -> StartDownload once, then persist the opaque reference
   yes -> DownloadStatus for the exact stored reference
```

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

If `StartDownload` succeeds but persisting the returned reference fails, the step
returns `ErrExecutionSideEffectUncertain` and never attempts a second
`StartDownload` on that path. Because no durable reference exists, that outcome
must be reconciled by an operator rather than treated as ordinary retry: a later
execution that still has no durable reference is not authorized to assume the
external side effect did not happen.

## Deferred capabilities

Source Resolver/provider syntax normalization, the Job worker loop, Manifest
milestone transition to `AWAITING_VISIBILITY`, the 115 adapter, OpenList
visibility verification, Mutation Hint/scoped refresh, canonical READY
orchestration, auth/quota, and API/UI are separately authorized later work.
