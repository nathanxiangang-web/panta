# Copy classification

Gate 2.3 adds one explicit Catalog command that binds an already-observed,
unresolved Copy to an already-existing Variant.

```text
existing Copy with variant_id = NULL + existing target Variant
                            |
                            v
                  explicit Bind command
                            |
                            v
                  same Copy, target Variant
```

## Module boundaries

- `internal/catalog.ClassificationService` owns request validation and exposes
  the explicit application command.
- `internal/catalog.CopyBinder` is the Panta-owned atomic persistence port.
- `internal/store/postgres.CatalogRepository` implements that port with a row
  lock and one transaction.
- `internal/projector` remains independent. It owns physical availability and
  never writes `variant_id` on an existing Copy.

The Catalog service imports no PostgreSQL/pgx, IndexCore, OpenList, provider,
Search, Agent, authentication, usage, or share implementation.

## Monotonic outcomes

- `NULL -> target Variant` succeeds with `Changed=true`.
- Repeating the same target succeeds with `Changed=false`.
- Requesting a different target after classification returns
  `ErrCopyAlreadyClassified` and does not mutate the Copy.
- A missing Copy returns `ErrClassificationCopyNotFound`; classification never
  creates it.
- A missing Variant returns `ErrClassificationVariantNotFound`; classification
  never creates Asset, Release, or Variant identity.

CopyID, IndexCore root/resource identity, StorageBindingID, availability, and
`created_at` are preserved. Only `variant_id` and ordinary `updated_at` metadata
change on the first successful binding.

## Concurrency and ownership

PostgreSQL locks the Copy row before inspecting `variant_id`. Competing commands
therefore serialize inside the persistence boundary:

- different targets produce exactly one winner and one explicit conflict;
- the same target produces one change and one successful idempotent replay.

The target Variant is locked with `FOR KEY SHARE` before the update, so it cannot
disappear between existence validation and the foreign-key write. Expected
absence/conflict outcomes are mapped to Catalog errors instead of leaking pgx
sentinels.

Classification owns `variant_id`; Journal projection owns physical
`availability`. Later PRESENT/REMOVED projection updates retain the same CopyID
and VariantID.

## Deferred capabilities

Automatic or Agent classification, reclassification/correction, bulk binding,
confidence/evidence models, Search/FTS, Q3 enrichment, API/UI, acquisition/115,
Mutation Hint/scoped refresh, auth/quota/share, and recommendation remain outside
Gate 2.3. A correction workflow must be separately authorized rather than
weakening this monotonic command.
