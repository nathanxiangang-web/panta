# Panta — Project Context

> Durable project memory. Update this file when project intent, architecture invariants, or product boundaries change.

## 1. Product intent

Panta manages the lifecycle from “I want / I have a resource” to “the resource is actually usable.”

It is not tied to 115Drive. 115 is the first Storage/Downloader/Share provider used to prove the model.

Future storage providers may include China Mobile Cloud Drive, WebDAV, NAS, or other services. Replacing a provider must not require rebuilding the product domain.

## 2. MVP product behaviors

The MVP must support these entry paths independently:

1. **Browse / known path** — user knows where a resource is and navigates or resolves the path directly.
2. **Catalog lookup** — user knows the logical resource or version and wants the product representation.
3. **Direct acquisition** — user submits URL / magnet / supported source for acquisition.
4. **Agent-assisted discovery** — later MVP stage; agent helps resolve a source or identify a resource.

Search is useful but is not a prerequisite for browse, path resolution, acquisition, or use.

## 3. Resource model

Panta distinguishes four identities:

```text
Asset
  ↓
Release
  ↓
Variant
  ↓
Copy
  ↓
IndexCore resource_id
```

### Asset
The logical thing the user recognizes, such as a software project, model, film, course, or resource set.

### Release
A published version/revision/channel of an Asset.

### Variant
A particular form of a Release, such as platform/architecture, quantization, quality, codec, resolution, package type, etc.

### Copy
A physical stored instance of a Variant. A Copy binds to one or more IndexCore physical resources.

This separation allows:
- multiple releases of one Asset;
- multiple variants of one Release;
- multiple storage copies of one Variant;
- provider migration without changing Asset/Release/Variant identity.

## 4. Observation plane

Panta observes storage through:

```text
115 / future storage
        ↓
     OpenList
        ↓
IndexCore OpenList Collector
        ↓
Canonical Inventory + Journal
        ↓
Panta Catalog Projector
```

Rules:
- OpenList is the upstream storage observation/access layer used by IndexCore.
- IndexCore is the canonical physical-resource truth.
- Panta never redefines physical presence independently of IndexCore.
- Panta does not directly query OpenList to decide acquisition visibility or canonical presence.
- Panta must not read OpenList DB or read/write IndexCore PostgreSQL directly.
- Product-facing physical-resource services consume IndexCore read APIs / Journal.
- After provider mutations, Panta may send a trusted IndexCore Mutation Hint; IndexCore owns the subsequent OpenList scoped verification.
- Trusted Mutation Hint acceptance is work ingress only, not canonical truth.

## 5. Control plane

Provider mutations are separate from observation:

```text
Product command
     ↓
Job Engine
     ↓
Provider Contract
     ↓
115 Adapter / future Adapter
     ↓
Provider API / MCP implementation
```

Provider-specific implementation details must remain below the provider contract.

## 6. 115 MVP role

115 is the first provider used to prove Panta's provider-neutral model.

Current implemented provider slice:
- Cloud Downloader Provider: `internal/providers/115`;
- runtime library pinned to `github.com/SheltonZhu/115driver v1.3.5`;
- exact URI -> offline task -> `info_hash` mapping;
- connection-scoped authenticated session selected by ProviderID + ConnectionID + CredentialRef.

Not yet implemented:
- 115 ShareProvider;
- a separate Panta StorageProvider scanner.

Existing-storage observation remains OpenList -> IndexCore, not a 115 scanner inside Panta. Future provider implementation may change without changing the Panta domain contract.

## 7. Cloud-download synchronization

Cloud-download completion alone does not make a resource READY.

Required chain:

```text
Acquisition Job
  ↓
115 cloud download completes
  ↓
provider stage commits AWAITING_VISIBILITY
  ↓
Panta sends trusted IndexCore Mutation Hint
  root_id   = StorageBinding.indexcore_root_id
  scope_key = Manifest.target_path
  reason    = POSSIBLE_CHANGE
  ↓
IndexCore-owned incremental runtime
  ↓
IndexCore OpenList scoped verification
  ↓
IndexCore Canonical + Journal
  ↓
Panta Query/Journal confirmation + Catalog Projector
  ↓
READY
```

Panta does **not** add a separate direct OpenList visibility-verification hop. OpenList remains inside the IndexCore-owned observation pipeline.

No full 40 TB rescan is required for a normal acquisition.

## 8. Access and share

Personal download/play access may use a provider direct URL or OpenList access gateway when supported.

Share is a separate product capability and should use a provider-supported share mechanism.

Cloud download and share require product login.

## 9. Product account boundary

Panta account and storage-provider account are different identities:

```text
Panta User
   ↓
Storage Connections
   ├─ 115
   ├─ future provider
   └─ ...
```

Credentials are provider connection secrets and must not become agent prompt content.

## 10. Agent boundary

CodeArts CLI is the initial Agent Runtime.

Agents may assist with:
- source classification;
- release/version extraction;
- resource identification;
- metadata enrichment;
- recovery diagnosis.

Agents must not own:
- authorization;
- quota;
- canonical resource state;
- job state transitions;
- provider credentials;
- destructive side effects.

Agent Runtime is replaceable. Product domain code must not depend on CodeArts-specific behavior.

## 11. Commercial boundary

MVP may be free, but quota/usage accounting should exist from the beginning.

Likely gated actions:
- cloud download;
- share;
- agent-assisted acquisition;
- subscription/update automation.

Pricing is explicitly out of scope for the first implementation gates.

## 12. Architecture invariants

1. Provider is replaceable.
2. Search is optional.
3. OpenList and IndexCore remain independent upstream components.
4. Product DB and IndexCore DB are separate.
5. Panta Catalog owns logical resource semantics.
6. All side effects go through jobs/provider ports.
7. A provider callback cannot unilaterally declare a Copy READY.
8. Agent output never becomes canonical truth without deterministic validation/evidence.
9. Provider-specific names/types must not leak into core domain interfaces.
10. Git is the project memory; accepted decisions and state changes must be committed.
11. Panta acquisition does not directly observe OpenList; IndexCore owns OpenList collection/scoped verification.
12. If an old document, issue, PR, or chat memory conflicts with a newer accepted decision, the newer accepted Git decision wins.
13. Before a new Gate is planned or reviewed, read `docs/AI-ARCHITECTURE-MEMORY.md`, `PROJECT-STATE.md`, and the newest relevant decisions.


## 13. AI / Architect reconstruction rule

To prevent context drift in long AI-assisted development sessions, project truth must be reconstructed from Git before every new Gate or architecture review.

Read order:

```text
docs/AI-ARCHITECTURE-MEMORY.md
        ↓
PROJECT-STATE.md
        ↓
docs/DECISIONS.md
        ↓
PROJECT-CONTEXT.md
        ↓
docs/MVP-BLUEPRINT.md
        ↓
current Issue / PR
        ↓
exact code + tests + CI
```

Chat history is never authoritative when it conflicts with current Git.

The compact memory file must be updated whenever:
- an architecture boundary changes;
- a previous decision is superseded;
- a Gate changes the runtime/state model;
- an external integration contract changes materially;
- an AI review uncovers context drift or duplicated responsibility.
