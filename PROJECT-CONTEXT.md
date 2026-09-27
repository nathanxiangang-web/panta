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
- OpenList is the storage observation/access layer.
- IndexCore is the canonical physical-resource truth.
- Panta never redefines physical presence independently of IndexCore.
- Panta must not read/write IndexCore PostgreSQL directly.
- Product-facing services consume IndexCore read APIs / Journal.
- Trusted mutation hints remain an internal integration path.

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

115 currently provides the first:
- Storage Provider;
- Cloud Downloader Provider;
- Share Provider.

Implementation may initially use 115 MCP and may later change to official API or another library without changing the Panta domain contract.

## 7. Cloud-download synchronization

Cloud-download completion alone does not make a resource READY.

Required chain:

```text
Acquisition Job
  ↓
115 cloud download completes
  ↓
verify storage result
  ↓
verify/refresh OpenList visibility
  ↓
IndexSync mutation hint
  ↓
IndexCore scoped refresh
  ↓
IndexCore canonical confirmation / Journal
  ↓
Catalog Projector creates or updates Copy
  ↓
READY
```

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
