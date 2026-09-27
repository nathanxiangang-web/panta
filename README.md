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
