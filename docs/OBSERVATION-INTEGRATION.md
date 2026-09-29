# Observation integration runbook

Gate 1 keeps one observation path:

```text
OpenList-compatible source
        -> IndexCore OpenList collector / Canonical Inventory / Journal
        -> Panta IndexCore Q4/Q5/Q8 client
        -> Panta projector and Catalog Copy
```

IndexCore owns OpenList tree collection. Panta owns the product-side
`StorageConnection`, `StorageBinding`, projection cursor, and Catalog `Copy`.
Panta does not read or write the IndexCore database and does not implement a
second OpenList tree walker.

## Mapping

The three values below describe one physical observation boundary and must be
configured together:

```text
StorageBinding.openlist_mount_path = /library
IndexCore root adapter path         = /library
StorageBinding.indexcore_root_id    = IndexCore root UUID
```

The OpenList path is provider-side configuration owned by IndexCore. The root
UUID is the stable physical namespace that Panta persists. A known physical
path is resolved directly with Panta's IndexCore Q5 client; Catalog search is
not part of this flow.

Store only an environment-variable name such as
`PANTA_GATE1_OPENLIST_TOKEN` in the IndexCore adapter configuration. Inject the
token into the IndexCore process at runtime. Do not persist the token value or
print it in logs.

## Controlled Gate 1 acceptance

The opt-in test at `test/e2e/observation` requires two fresh, separate
PostgreSQL 18 databases and an external IndexCore binary built from:

```text
release: v0.4.0-alpha.1
commit:  6f0eec85c59bd8cbe55011b0d9e512e0cafd6615
```

GitHub Actions builds that exact commit, verifies the checkout SHA, starts a
loopback-only test fixture and IndexCore HTTP server, and runs:

```bash
PANTA_GATE1_E2E=1 \
PANTA_GATE1_INDEXCORE_BINARY=/path/to/indexcore \
PANTA_GATE1_INDEXCORE_DATABASE_URL=postgres://.../indexcore_e2e \
PANTA_GATE1_PANTA_DATABASE_URL=postgres://.../panta_e2e \
go test ./test/e2e/observation -run TestControlledObservation -count=1 -v
```

The harness runs real IndexCore migrations, collection, Canonical Inventory,
Journal and HTTP transport, followed by real Panta migrations 0001-0004, Q4,
Q5, Q8 and `ProjectOnce`. It verifies initial projection, identical-scan
idempotency, and one additive resource.

OpenList collection is additive-safe in this gate: absence is not proof of
deletion. Gate 3 may later add post-download visibility verification plus an
IndexCore Mutation Hint and bounded scoped refresh. Those capabilities are not
part of Gate 1.
