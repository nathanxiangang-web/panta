# Panta — Decision Log

> Append-only architecture/product decision memory. Do not rewrite history silently; supersede an old decision with a new entry.

## D-001 — MVP before UI

**Status:** Accepted

Panta MVP prioritizes functional end-to-end flows and modular architecture before UI polish.

## D-002 — OpenList is the observation/access layer

**Status:** Accepted

Existing and future storage providers are preferably exposed through OpenList for observation. Panta does not build another provider scanner for IndexCore.

## D-003 — IndexCore owns physical resource truth

**Status:** Accepted

IndexCore remains an independent canonical physical-resource kernel. Panta consumes its read API/Journal and never writes its database.

## D-004 — Panta owns logical resource semantics

**Status:** Accepted

Panta Catalog models logical identity as:

```text
Asset → Release → Variant → Copy → IndexCore resource
```

This prevents provider paths/files from becoming product identity.

## D-005 — Search is optional

**Status:** Accepted

Known-path browse/resolve, direct acquisition, and physical-resource inspection must work without Catalog search.

## D-006 — Separate observation and control planes

**Status:** Accepted

Observation:

```text
Storage → OpenList → IndexCore → Catalog
```

Control:

```text
Panta → Job Engine → Provider Contract → Provider Adapter → Storage
```

Provider command success does not override canonical observation.

## D-007 — 115 is the first provider, not the architecture

**Status:** Accepted

115 supplies the first storage/cloud-download/share implementations. Provider-specific behavior must remain behind replaceable contracts.

## D-008 — Cloud-download READY requires canonical confirmation

**Status:** Accepted

A completed provider download is not READY until:
1. the result is visible through OpenList;
2. IndexCore scoped refresh confirms it;
3. Journal/Projector creates or updates the Copy.

## D-009 — Agent Runtime is replaceable

**Status:** Accepted

CodeArts CLI + GLM-5.2 is the initial Agent Runtime. Deterministic product flows must work with the agent disabled.

## D-010 — Git is project memory

**Status:** Accepted

Project context, current state, accepted decisions, contracts, implementation and acceptance evidence must be committed to this repository.

## D-011 — Go is the initial Panta backend implementation baseline

**Status:** Accepted

Panta MVP backend implementation starts with **Go 1.27.1**, aligned with the current IndexCore toolchain to reduce build/runtime/tooling divergence during integration.

This is an implementation/toolchain choice, not a domain coupling decision:

- Panta does not import IndexCore internal packages;
- Panta communicates with IndexCore only through Panta-owned ports and supported external integration contracts;
- provider adapters remain replaceable behind Panta provider contracts;
- the MVP remains a single-process modular monolith until real scaling evidence justifies a split.

The Panta product database remains PostgreSQL as established by the MVP blueprint. Database schema and migrations are handled in a later bounded Gate 0 task, not in Gate 0.1.

## D-012 — Panta owns an independent PostgreSQL schema and append-only migration history

**Status:** Accepted

Panta product persistence uses PostgreSQL through `pgx/v5`.

Schema evolution uses ordered, append-only SQL migrations owned by Panta and recorded in a `schema_migrations` history. Runtime ORM auto-migration/schema generation is not used.

Compatibility is fail-closed:

- missing required migrations are incompatible;
- migrations unknown to the running binary are treated as future schema and are incompatible;
- an older Panta binary must not silently run against a newer product schema.

Panta product DB configuration remains independent from IndexCore DB configuration. Panta never obtains physical truth by reading or writing IndexCore PostgreSQL directly.

Catalog domain packages remain persistence-agnostic; pgx belongs in the PostgreSQL adapter/infrastructure boundary.
