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

## Deferred capabilities

Provider task references, Source Resolver/provider syntax normalization, Job
worker execution, provider registry selection, DownloaderProvider calls, 115,
OpenList visibility verification, Mutation
Hint/scoped refresh, canonical READY orchestration, auth/quota, and API/UI are
separately authorized later work.
