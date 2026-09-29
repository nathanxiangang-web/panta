# Acquisition Manifest

Gate 3.1 introduces durable, provider-neutral acquisition intent before any
provider side effect exists.

```text
validated acquisition request
             |
             v
  PENDING Acquisition Manifest
             |
             v
       later Gate: Job
             |
             v
       later Gate: provider
```

## Module boundaries

- `internal/acquisition` owns the Manifest model, closed milestone states,
  target-path normalization, creation service, and persistence port.
- The creation service reads only Panta-owned StorageBinding and Catalog identity
  ports. It does not create Storage or Catalog identity.
- `internal/store/postgres` implements Manifest persistence.
- Jobs continue to own execution, claim, lease, retry, and recovery behavior.
  The Manifest owns intent, target, optional logical association, and a coarse
  product milestone only.

The acquisition domain cannot import pgx/database/sql, provider execution,
OpenList, IndexCore, Jobs, Search, Agent, or authentication implementations.
Gate 3.1 performs no network call and no provider operation.

## Intent validation

Source type and source reference must be non-empty and bounded. Source reference
is stored as opaque input: Gate 3.1 does not parse URLs, magnets, or provider
syntax.

The target identifies a location inside an ACTIVE StorageBinding. It is distinct
from the binding's OpenList mount path. Target paths:

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
optional future user identity without a user foreign key, and an optional unique
Job reference. It introduces no UUID extension or database-generated UUID
default.

New Manifests are always created in `PENDING`. The frozen state set also reserves
`ACTIVE`, `AWAITING_VISIBILITY`, `AWAITING_CANONICAL`, `READY`, `FAILED`,
`RECOVERY_REQUIRED`, and `CANCELED`, but Gate 3.1 implements no transitions.

## Deferred capabilities

Provider task references, Source Resolver/provider syntax, durable Job
submission, worker execution, 115, OpenList visibility verification, Mutation
Hint/scoped refresh, canonical READY orchestration, auth/quota, and API/UI are
separately authorized later work.
