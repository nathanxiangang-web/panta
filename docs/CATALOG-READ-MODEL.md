# Catalog logical read model

Gate 2.1 exposes one Panta-owned logical drill-down:

```text
Asset
  -> Releases
       -> Variants
            -> Copies
```

## Module boundaries

- `internal/catalog` owns logical entities and the `HierarchyReader` port.
- `internal/resourceview` composes Catalog facts and validates requested parent
  ownership. It does not perform SQL, Search, provider calls, or physical
  observation.
- `internal/store/postgres` implements deterministic child reads without
  exposing PostgreSQL types through the port.

The dependency direction is:

```text
resourceview -> catalog port <- postgres adapter
```

## Absence semantics

- A missing selected Asset, Release, or Variant returns a distinct typed error.
- A Release that belongs to another Asset and a Variant that belongs to another
  Release fail closed with ownership-mismatch errors.
- An existing parent with no children returns a valid non-nil empty list.
- A selected Variant with zero Copies is valid and reports
  `HasPresentCopy=false`.
- REMOVED Copies remain visible in the Variant's Copy list but do not establish
  present availability.
- A Copy with `variant_id=NULL` remains valid unresolved physical state and is
  not attached to any logical Variant.

## Determinism and extension rules

PostgreSQL orders Releases by creation time and ID, Variants by key and ID, and
Copies by creation time and ID. Future adapters must preserve deterministic
ordering and the same empty-versus-missing behavior.

Future classification writes, Search/FTS, IndexCore physical enrichment,
provider access, and public API/UI composition require separately authorized
gates. They must not be added behind this read model.
