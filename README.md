# Panta

Panta is a provider-neutral resource acquisition and catalog platform.

The MVP focuses on proving the resource lifecycle end to end before investing in UI polish:

```text
Storage Provider
      ↓
   OpenList
      ↓
   IndexCore
      ↓
Panta Catalog
      ↑
Acquisition / Agent / Product API
```

## MVP goals

Panta v0.1 must prove five things:

1. Existing resources can be observed through **OpenList → IndexCore → Panta Catalog**.
2. A user who already knows a path can browse/resolve it directly; **search is optional, not mandatory**.
3. Logical resource identity is separated from physical files through **Asset → Release → Variant → Copy**.
4. A logged-in user can submit a supported source to the **115 cloud-download provider**, after which Panta verifies OpenList visibility, triggers IndexCore scoped refresh, and exposes the new Copy without a full scan.
5. Provider-specific logic is replaceable. Panta domain modules must not depend directly on 115 semantics.

## Architecture rules

- OpenList is the storage observation/access layer, not the product database.
- IndexCore is the canonical physical-resource truth, not the product catalog or search engine.
- Panta Catalog owns Asset / Release / Variant / Copy semantics.
- Provider actions go through provider contracts and the Job Engine.
- Agent output is advisory/structured input; agents do not own product state.
- Cloud download and share require product login.
- Panta product account is independent from storage-provider accounts.
- Git is the project memory. Architecture, decisions, current state and acceptance evidence must live in this repository.

## Project documents

- [MVP Blueprint](docs/MVP-BLUEPRINT.md)
- [Project Context](PROJECT-CONTEXT.md)
- [Project State](PROJECT-STATE.md)
- [Development & Acceptance Rules](docs/DEVELOPMENT-RULES.md)
- [Catalog logical read model](docs/CATALOG-READ-MODEL.md)
- [Physical resource view](docs/PHYSICAL-RESOURCE-VIEW.md)
- [Copy classification](docs/COPY-CLASSIFICATION.md)
- [Acquisition Manifest](docs/ACQUISITION-MANIFEST.md)

## Collaboration model

Panta MVP uses a deliberately small team model:

- **Architect:** owns boundaries, contracts, gate acceptance and scope.
- **Developer:** implements one accepted stage at a time, with tests and evidence.

No stage starts because “the code seems ready.” It starts only after the previous gate is accepted.

## Gate 0.1 backend skeleton

The backend requires Go 1.27.1. The Gate 0.1 skeleton can be verified with:

```text
make test
make build
```

The binary reads `PANTA_ENV` (default: `development`) and stays alive until it
receives an interrupt or termination signal. No provider or external service is
required to build or start it.

Package dependencies follow the modular-monolith boundary:

```text
cmd/panta → internal/app → modules and Panta-owned ports
                                  ↑
                    future adapters/integrations
```

- `catalog`, `resourceview`, `acquisition`, and `jobs` own product behavior.
- `providers/contracts` owns provider-neutral ports; `providers/registry` binds
  implementations only at the composition boundary.
- `integrations/indexcore` and `integrations/openlist` are Panta-owned external
  service ports. They expose no upstream database or implementation types.
- In-memory doubles live only in tests. There is no real provider, persistence,
  network integration, search, agent, authentication, HTTP API, or UI in Gate
  0.1.

## Gate 0.2 product database

Panta product state uses a PostgreSQL database that is configured independently
through `PANTA_DATABASE_URL`. It is never shared with or inferred from an
IndexCore database setting.

Schema changes are ordered SQL files in `migrations/`. Applied versions, names,
and checksums are recorded in `schema_migrations`; missing, changed, or unknown
future migrations make schema status incompatible. Apply or inspect them with:

```text
make db-migrate
make db-status
```

`internal/catalog` owns the Asset → Release → Variant → Copy types and the
persistence port. The pgx/v5 implementation is isolated in
`internal/store/postgres`. A Copy may have a null `variant_id` while awaiting
logical classification. Gate 1.3 later hardens `storage_binding_id` with its
now-valid foreign key to `storage_bindings`.

PostgreSQL integration tests require a dedicated disposable database named
`panta_test` or ending in `_test`; the tests recreate its `public` schema:

```text
PANTA_TEST_DATABASE_URL='postgres://user:password@127.0.0.1:5432/panta_test?sslmode=disable' make test-integration
```

## Gate 0.3 durable jobs

`internal/jobs` owns the provider-neutral Job model, frozen states, commands,
errors, and Repository port. `internal/store/postgres` implements that port with
atomic PostgreSQL claims and lease-checked transitions. The dependency remains:

```text
future application/worker → internal/jobs ← internal/store/postgres
```

The generic engine stores opaque JSON payloads and never imports provider,
IndexCore, OpenList, auth, or agent types. Future job types extend through
product-level `job_type` and payload contracts outside the state kernel; they do
not add provider-specific columns to `jobs`.

Claimable work is limited to `QUEUED` and due `RETRY_WAIT` jobs. A running job
with an expired lease moves to `RECOVERY_REQUIRED`; it is not automatically
retried because an external side effect may already have happened. Only the
active, unexpired lease owner can renew, succeed, fail, or schedule a retry.
Gate 0.3 deliberately contains no worker loop, provider execution, backoff
policy, job DAG, or event/audit table.

## Gate 0.4 closeout verification

Gate 0 closes through test-only composition, not by introducing a real
provider. `internal/providers/testprovider` is a deterministic in-memory
implementation of the frozen Storage, Downloader, and Share ports. It is used
by the reusable `internal/providers/contracttest` suite and is not registered by
the production application.

The Gate 0 dependency and test boundaries are:

```text
core domains → Panta-owned ports ← production adapters (future gates)
                              ↖ testprovider + contracttest (tests only)

catalog/jobs ← internal/store/postgres → Panta PostgreSQL
```

- Provider conformance covers descriptor consistency, storage stat/access,
  download start/status/cancel, share create/inspect/access/revoke, opaque
  references, and the provider-neutral unsupported-operation error.
- The Gate 0 acceptance test composes the registry, a known-path IndexCore port
  double, the current append-only migration chain, Catalog persistence, and a
  generic durable Job without coupling those modules or adding an acquisition
  workflow.
- `internal/architecture` automatically guards Catalog, Jobs, and the IndexCore
  port from forbidden database, provider, 115, and cross-module imports.
- `.github/workflows/gate0.yml` runs unit/contract tests, race detection, vet,
  both command builds, PostgreSQL 16 integration tests, and migration
  apply/status checks on pull requests and pushes to `main`.

No 115 adapter, real OpenList/IndexCore client, worker loop, Search, Agent,
authentication, API, or UI is part of Gate 0.4.

## Gate 1.1 storage root mapping

`internal/storage` owns the provider-neutral StorageConnection and
StorageBinding model and its persistence port. A binding records the product
mapping from one connection and normalized OpenList mount path to one canonical
external IndexCore root ID:

```text
StorageConnection ── StorageBinding ── OpenList mount path
                         ├──────────── IndexCore root_id (external identifier)
                         └──────────── provider_scope (optional opaque target)
```

`internal/store/postgres` implements the port. Migration
`0003_storage_bindings.sql` enforces the connection foreign key, globally unique
IndexCore root mapping, unique connection/mount mapping, the small
ACTIVE/DISABLED status sets, and canonical mount-path storage.

Mount paths use absolute slash form. `/` is the root; repeated and trailing
slashes normalize away for non-root mounts. Empty, relative, backslash, `.`, and
`..` component forms are rejected. `credential_ref` is nullable opaque metadata
only; credentials and secrets are not stored in these rows.

Gate 3.3 migration `0007_storage_binding_provider_scope.sql` adds the third,
independent coordinate. A nullable scope keeps observation-only bindings valid;
acquisition requires an explicit scope and never derives it from the OpenList
mount or IndexCore root. Provider scope is bounded opaque text and is preserved
exactly without path normalization.

Gate 3.4 migrations `0008_acquisition_provider_tasks.sql` and
`0009_provider_task_side_effect_fence.sql` add the durable provider-task linkage:
one Manifest and one Job each map to at most one opaque provider task reference,
stored exactly as the provider returned it. The durable row is written *before*
the external call, in two phases (`START_RESERVED`, then `REFERENCE_KNOWN`) under
a per-Manifest advisory lock, so at most one execution can ever call
`StartDownload` and a lost reference fails closed instead of starting a second
provider task. An existing reference is never overwritten or re-created. The
provider stays authoritative for its own task status, the fence carries no
provider lifecycle state, and provider success is not Manifest `READY`.

Gate 3.5 scopes provider execution to a StorageConnection instead of a provider
identity alone: `internal/providers/session` binds one downloader port to an exact
`ProviderID` + `ConnectionID` + opaque `CredentialRef`, and the execution step
resolves that session before any provider call. One `ProviderID` may back several
connections with different credentials, and a missing or mismatched binding fails
closed. `contracts.SecretResolver` freezes the opaque secret lookup port with no
real backend yet; secret contents never enter the Manifest, Job payload, or
provider-task tables.

The module stores no physical inventory and has no IndexCore/OpenList database
or network dependency. Real clients, Journal cursors/projectors, Copy updates,
visibility checks, Mutation Hints, and provider-specific behavior remain outside
Gate 1.1.

## Gate 1.2 IndexCore read adapter

`internal/integrations/indexcore` owns a typed, server-side HTTP adapter for the
accepted IndexCore Q4/Q5/Q8/Q9 read surface. Configure its base URL with
`PANTA_INDEXCORE_BASE_URL`; this is an HTTP endpoint only and never an IndexCore
database setting.

The consumer-owned ports preserve the upstream semantics instead of flattening
them: Q4 browses one parent hierarchy level and round-trips opaque cursors, Q5
returns every canonical-path match plus its ambiguity flag, Q8 sends
`after_seq` exactly as supplied and keeps per-root event order, and Q9 exposes a
narrow read-only root status. Stable remote errors, transport failures,
malformed responses, and unexpected statuses remain separately classifiable
with `errors.Is`.

The adapter uses only the standard HTTP client with a bounded timeout. Contract
tests run against `httptest.Server`; no IndexCore process or database is needed.
Journal cursor persistence, Catalog projection, OpenList integration, retries,
Mutation Hints, scoped refresh, and root mutations remain outside Gate 1.2.

## Gate 1.3 one-shot Journal projection

`internal/projector` owns one bounded `ProjectOnce` application operation. It
loads an ACTIVE StorageBinding, reads that binding's durable cursor, calls Q8
with the exact cursor, validates the ordered page, and maps resource events to
unresolved Copy availability. Root lifecycle events advance only the cursor;
Journal payload remains opaque and does not drive identity or state.

`internal/store/postgres` owns the atomic persistence boundary:

```text
lock and compare expected cursor
  -> upsert ordered Copy mutations without changing CopyID/VariantID/binding
  -> advance cursor to the last event_seq actually seen
  -> commit one PostgreSQL transaction
```

Migration `0004_indexcore_projection.sql` adds the per-binding cursor and the
`copies.storage_binding_id` foreign key. Missing cursor state reads as zero.
Concurrent stale cursors and attempted binding rebinding fail closed. The Q8
network call happens before the transaction begins. Candidate Copy UUIDs are
created by the application-side projector and supplied to persistence; an
upsert uses a candidate only for a genuine insert and preserves the existing
CopyID on conflict.

Gate 1.3 deliberately adds no polling loop, scheduler, OpenList client,
Mutation Hint, classification, provider execution, API, or UI.
