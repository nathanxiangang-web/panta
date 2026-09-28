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
`internal/store/postgres`. `storage_binding_id` is opaque at this gate, and a
Copy may have a null `variant_id` while awaiting logical classification.

PostgreSQL integration tests require a dedicated disposable database named
`panta_test` or ending in `_test`; the tests recreate its `public` schema:

```text
PANTA_TEST_DATABASE_URL='postgres://user:password@127.0.0.1:5432/panta_test?sslmode=disable' make test-integration
```
