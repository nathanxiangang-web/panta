# Physical resource view

Gate 2.2 adds a physical-first, known-path read flow. It starts with IndexCore
Q5 and treats the returned physical resources as useful even when Panta has no
Catalog Copy for them.

```text
known root + canonical path
            |
            v
     IndexCore Q5 Resolve
            |
            v
 every physical match + ambiguity
            |
            v
 optional Copy lookup by exact (root_id, resource_id)
            |
            v
 optional Variant -> Release -> Asset lineage
```

## Module boundaries

- `internal/integrations/indexcore.ResolvePort` owns the narrow Q5 contract.
- `internal/catalog.PhysicalIdentityReader` owns exact Copy lookup by canonical
  physical identity.
- `internal/resourceview.PhysicalService` composes those two read ports. It has
  no persistence, provider, OpenList, Search, or write dependency.
- `internal/store/postgres.CatalogRepository` implements the exact Copy lookup
  with the existing unique `(indexcore_root_id, indexcore_resource_id)` key.

The dependency direction is:

```text
IndexCore Q5 port -> resourceview -> Catalog read port <- PostgreSQL adapter
```

No IndexCore implementation or database type crosses the Panta-owned port.
Gate 2.2 adds no migration because the required identity and uniqueness already
exist in the accepted Catalog schema.

## Classification states

- `PHYSICAL_ONLY`: Q5 returned the resource and no Copy exists. This is valid,
  usable physical state rather than a not-found error.
- `UNRESOLVED_COPY`: a Copy exists but its `variant_id` is null.
- `CLASSIFIED`: the Copy has a complete, ownership-consistent
  Copy -> Variant -> Release -> Asset lineage.

Once a Copy claims a Variant, a missing Variant, Release, or Asset—or a reader
returning an entity that does not own the requested immutable ID—is invalid
Catalog state. The service fails closed and does not downgrade it to unresolved
or physical-only.

## Physical truth and ambiguity

The service preserves all Q5 matches in their returned order and forwards
IndexCore's ambiguity decision. It never chooses a winner, including when only
one candidate has a Catalog classification.

IndexCore `ResourcePresence` and Catalog Copy `Availability` are independent
facts and are returned unchanged. Gate 2.2 performs no reconciliation between
them.

## Extension constraints

Future classification writes, resource/entity creation, Agent classification,
Search, Q3 enrichment, provider/OpenList access, API/UI composition,
acquisition, Mutation Hint/scoped refresh, and reconciliation require later
authorized gates. None may be hidden inside this read service.
