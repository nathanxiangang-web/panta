# Panta — AI Architecture Memory

> Canonical short-form project memory for Architect/AI sessions.
>
> Last architecture-memory synchronization: **2026-09-30**
> Active Gate: **3.9 / Issue #41 — Deterministic acquired-result identity capture**
> Governing corrections: **D-029** (IndexCore-owned observation), **D-030** (Git-first AI reconstruction), and **D-031** (deterministic direct-child acquisition identity).
>
> **Read this file before planning, reviewing, or authorizing any new Gate.**
>
> If chat context, an old issue, an old PR description, README text, or an older decision conflicts with this file plus the latest accepted Decision Log / Project State, **Git wins and the newer accepted decision wins**.

## 1. Source-of-truth order

When reconstructing project truth, read in this order:

1. `docs/AI-ARCHITECTURE-MEMORY.md` — compact current invariants and anti-drift rules.
2. `PROJECT-STATE.md` — current accepted gates and exactly one authorized next task.
3. `docs/DECISIONS.md` — append-only architecture decisions; later decisions may explicitly supersede earlier ones.
4. `PROJECT-CONTEXT.md` — durable product/architecture context.
5. `docs/MVP-BLUEPRINT.md` — end-to-end product blueprint.
6. Current GitHub Issue / PR for the active Gate.
7. Code + tests + CI on the exact PR head.

Do **not** infer current architecture from chat memory alone.

## 2. Project identity

Panta is a provider-neutral resource acquisition + logical catalog platform.

115 is the first provider, not the product architecture.

Logical identity:

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

Search is optional. Known-root/path flows and acquisition must work without Search.

## 3. Hard architecture boundary

### Observation plane

```text
Storage / 115
    ↓
OpenList
    ↓
IndexCore OpenList Collector
    ↓
IndexCore Canonical + Journal
    ↓
Panta IndexCore Query / Journal consumer
    ↓
Panta Catalog Projector / Copy
```

**IndexCore owns physical truth.**

Panta does not:
- scan OpenList;
- query OpenList to decide canonical visibility during acquisition;
- read OpenList DB;
- read/write IndexCore DB;
- invent physical presence independently of IndexCore.

OpenList is an upstream dependency of IndexCore's observation pipeline, not a Panta acquisition truth source.

### Control plane

```text
Panta command
    ↓
Job Engine
    ↓
Provider Contract
    ↓
Provider Adapter
    ↓
Storage / 115
```

Provider success never overrides observation truth.

## 4. Acquisition synchronization — current canonical flow

```text
Acquisition Manifest
    ↓
durable ACQUISITION Job
    ↓
connection-scoped Downloader session
    ↓
115 adapter
    ↓
provider task
    ↓
provider reports SUCCEEDED
    ↓
Manifest AWAITING_VISIBILITY
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
Panta canonical confirmation / projector
    ↓
Copy becomes PRESENT / READY workflow completes
```

Critical meanings:

- `PROVIDER_SUCCEEDED` != Job `SUCCEEDED`.
- `PROVIDER_SUCCEEDED` != Manifest `READY`.
- IndexCore Hint `202 Accepted` != canonical confirmation.
- Panta does **not** add a direct OpenList visibility verifier.
- Same ACQUISITION Job spans provider, observation-handoff, and canonical stages.

## 5. Coordinate separation

Never derive one coordinate from another:

```text
provider_scope
    provider-side mutation target, e.g. 115 saveDirID

openlist_mount_path
    IndexCore/OpenList observation configuration

indexcore_root_id
    canonical IndexCore root identity

manifest.target_path
    normalized directory scope inside the selected binding/root
```

Current Mutation Hint mapping:

```text
root_id   = indexcore_root_id
scope_key = manifest.target_path
reason    = POSSIBLE_CHANGE
```

Do not use `openlist_mount_path` or `provider_scope` to build the Hint.

## 5.1 Deterministic acquisition child identity

Before a newly successful provider acquisition can proceed beyond the provider stage, Panta must know one exact child name under the target directory:

```text
indexcore_root_id
+ manifest.target_path
+ manifest.expected_name
```

Semantics:
- `target_path` = directory scope refreshed by IndexCore;
- `expected_name` = exact direct child expected under that scope;
- expected_name may be absent when the Manifest is first created;
- provider success must freeze it before `AWAITING_VISIBILITY`;
- if intent already supplied the name, provider-reported identity must match exactly;
- no provider FileId/DirId, OpenList path, Journal ordering, timing window, newest-item or only-item heuristic may replace it;
- the 115 adapter may use only successful `OfflineTask.Name` for this provider-neutral identity.

Gate 3.10 may later use the frozen identity for exact canonical confirmation. Gate 3.9 does not authorize Q5/READY.

## 6. Job Engine invariants

The ACQUISITION Job is the durable execution safety boundary.

Current counter split:

```text
claim_attempts
    monotonic claim generation
    increments on every successful claim
    stale-worker fencing token
    unbounded

attempt_count / FailureCount
    genuine failure retry budget
    increments only through RetryAt
    bounded by max_attempts
```

Active lease validity requires:
- Job is RUNNING;
- owner matches;
- claim generation matches;
- database-time lease is unexpired.

Expired RUNNING side-effecting work never blind-retries; it goes to explicit recovery semantics.

## 7. Provider-side-effect invariant

Before external `StartDownload`:

```text
persist START_RESERVED
    ↓
exactly one caller may cross provider side-effect boundary
```

Then:

```text
StartDownload succeeds
    ↓
persist REFERENCE_KNOWN + opaque provider_task_ref
```

If reference persistence is uncertain:

```text
START_RESERVED remains
    ↓
future execution must NOT call StartDownload again
    ↓
operator recovery required
```

This fence is for provider side effects only.

IndexCore Mutation Hint is intentionally at-least-once/coalescing and does **not** use the provider-task fence.

## 8. 115 adapter invariants

Pinned runtime library:

```text
github.com/SheltonZhu/115driver v1.3.5
```

Frozen mapping:

```text
ProviderID             = "115"
Source.Value           = exact URI
Target.Scope           = 115 saveDirID / provider scope
Target.Path            = Panta observation scope only
TaskReference.Value    = 115 info_hash
```

No source normalization/truncation inside the adapter.

Cancel uses:

```text
DeleteOfflineTasks([info_hash], false)
```

Never delete downloaded files as part of task cancellation.

## 9. Provider session / secret boundary

Authenticated provider execution is selected by:

```text
ProviderID + ConnectionID + CredentialRef
```

ProviderID alone must never select authenticated state.

Secrets:
- never enter Manifest;
- never enter Job payload;
- never enter provider-task persistence;
- never enter Agent prompts;
- never be logged;
- are resolved only at the composition/provider-session boundary.

## 10. Current Gate status

Accepted:
- Gate 0 — skeleton/contracts — CLOSED
- Gate 1 — observation plane — CLOSED
- Gate 2 — resource semantics — CLOSED
- Gate 3.1 — Manifest
- Gate 3.2 — activation + Job
- Gate 3.3 — execution input/provider scope
- Gate 3.4 — durable provider-task fence
- Gate 3.5 — connection-scoped session/credential boundary
- Gate 3.6 — concrete 115 Downloader
- Gate 3.7 — atomic provider-stage outcome handoff
- Gate 3.8 — IndexCore trusted Mutation Hint and observation handoff

Authorized now:
- **Gate 3.9 — Issue #41 — Deterministic acquired-result identity capture**

Not authorized yet:
- direct Panta OpenList acquisition verifier;
- Q5/Journal canonical confirmation and READY beyond current Gate;
- API/UI;
- auth/quota/share;
- Agent implementation;
- generalized workflows/microservices/distributed queue.

## 11. Superseded / dangerous historical statements

These statements may still appear in old issues/commits and must NOT be revived:

### Superseded
```text
"Panta verifies OpenList visibility directly after provider success."
```

Superseded by D-029.

### Wrong
```text
"Panta should call /api/fs/get during acquisition."
```

Wrong for production acquisition architecture.

### Wrong
```text
"OpenList is Panta's canonical physical truth."
```

IndexCore is canonical truth.

### Wrong
```text
"provider task SUCCEEDED means READY."
```

Canonical confirmation is still required.

### Wrong
```text
"attempt_count is still the claim-generation token."
```

Since migration 0010, `claim_attempts` is generation and `attempt_count` is failure budget.

## 12. Anti-hallucination preflight

Before proposing or implementing a Gate:

1. Read this file.
2. Read `PROJECT-STATE.md`.
3. Read the newest relevant decisions in `docs/DECISIONS.md`.
4. Inspect the actual current code/contracts in Git.
5. If integrating an external repo, inspect that repo's current accepted interface before writing the Issue.
6. Search for an existing accepted module before proposing a duplicate implementation.
7. Check whether the proposed dependency direction violates an accepted boundary.
8. Write the bounded Issue only after those checks.
9. Do not authorize the next Gate until the current Gate is accepted and merged.

Before accepting a PR:

1. inspect exact head SHA;
2. inspect diff;
3. inspect relevant implementation, tests, migrations, and architecture guards;
4. inspect exact-head CI;
5. verify behavior against the current Issue and the newest decisions;
6. check replay/restart/concurrency/failure semantics where durability is claimed;
7. update `PROJECT-STATE.md` and architecture memory only after acceptance.

## 13. Conflict rule

If two documents disagree:

```text
newer explicit accepted decision
    > older decision
    > blueprint prose
    > old issue/PR description
    > chat memory
```

Never silently choose between contradictory documents.

Instead:
1. identify the conflict;
2. resolve it with an explicit Architect decision;
3. update this memory file + Project State + affected blueprint/context docs in the same architecture synchronization pass.

## 14. Team / scope rule

Team model is exactly:

```text
1 Architect
1 Developer
```

Do not invent additional workers.

The Developer implements only the currently authorized bounded Gate.

The Architect owns:
- boundaries;
- contracts;
- acceptance;
- decisions;
- project memory synchronization.
