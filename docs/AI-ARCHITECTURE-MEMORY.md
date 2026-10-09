# Panta — AI Architecture Memory

> Canonical short-form project memory for Architect/AI sessions.
>
> Last architecture-memory synchronization: **2026-10-09**
> Active Gate: **3.18 / Issue #60 — Human-authorized real 115 + OpenList + IndexCore staging E2E**
> Governing corrections: **D-029** (IndexCore-owned observation), **D-030** (Git-first AI reconstruction), **D-032** (durable result_name locator), **D-033** (READY anchored to one canonical projected Copy), **D-034** (one-claim stage routing), **D-035** (type-scoped claiming and atomic recovery), **D-036** (one explicit runner tick), **D-037** (opt-in worker lifecycle), **D-038** (fail-closed runtime graph and secret/trust boundaries), **D-039** (protected operator bootstrap/preflight), **D-040** (deterministic source intake with exact value preservation), and **D-041** (human-authorized live staging acceptance without architectural bypass).
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
durable Manifest.result_name
    provider ResultName > ExpectedName fallback
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

## 5.1 Deterministic acquisition result locator

Before provider success may advance automatically beyond the provider stage, Panta must durably resolve one top-level locator:

```text
valid provider-observed TaskStatus.ResultName
        >
Manifest.ExpectedName fallback
        ↓
Manifest.result_name
```

Canonical candidate identity for the later gate is:

```text
StorageBinding.indexcore_root_id
+ Manifest.target_path
+ Manifest.result_name
```

Semantics:
- `target_path` remains the directory scope refreshed by IndexCore;
- `expected_name` remains optional request-time intent/fallback and is not rewritten by provider execution;
- `result_name` is the immutable durable acquired-result locator;
- a valid provider-observed name wins over ExpectedName;
- blank/absent provider name may fall back to a valid ExpectedName;
- an invalid nonblank provider name fails closed instead of silently falling back;
- no provider FileId/DirId, OpenList path, Journal ordering, timing window, newest-item or only-item heuristic may replace result_name;
- result_name is locator evidence only, never Canonical truth or READY.

Gate 3.10 uses the frozen locator for exact canonical confirmation before READY.

## 5.2 Canonical READY anchor

Gate 3.10 finalizes one acquisition only after the pre-canonical locator has been resolved through IndexCore and the existing Journal Projector:

```text
indexcore_root_id + target_path + result_name
        ↓ Q5
one PRESENT IndexCore resource
        ↓ Q8 Journal / existing Projector
one PRESENT Panta Copy
        ↓ optional monotonic Variant bind
Manifest.result_copy_id = Copy.ID
Manifest READY
Job SUCCEEDED
```

Rules:
- zero Q5 matches is pending, not failure;
- Q5 ambiguity/multiple matches is recovery, never guesswork;
- acquisition never creates/upserts Copy directly;
- one invocation may advance the existing projector by at most one bounded page;
- unresolved Copy is sufficient when the Manifest has no Variant intent;
- if VariantID is present, the exact Copy must be bound to that Variant before READY;
- result_copy_id is the stable historical acquisition result link.

## 5.3 One-claim stage routing

Gate 3.11 connects accepted bounded services into one state-driven application coordinator:

```text
already claimed RUNNING ACQUISITION Job (owner, ClaimAttempts)
     ↓ fetch durable linked Manifest
ACTIVE             → one ExecutionStep + one atomic ProviderOutcome
AWAITING_VISIBILITY→ one trusted IndexCore Mutation Hint handoff
AWAITING_CANONICAL → one bounded canonical confirmation
terminal pair      → no-op/replay, no provider/IndexCore work
```

Never chain into a second stage in the same claim; a new claim is required.
Do not consume FailureCount for ordinary stage waiting, do not bypass provider START_RESERVED, do not convert uncertain provider effects into blind retry.
Gate 3.11 does not authorize an automatic scheduler, a continuous worker daemon, new stage Jobs, OpenList acquisition observation, Source Resolver parsing, or public API/UI.

## 5.4 Acquisition lease recovery safety

D-035 / Gate 3.12 closes two runtime prerequisites before an automatic worker may operate:

1. Claiming must be type-scoped: acquisition workers only lease ACQUISITION Jobs and must not claim/discard other future Job types.
2. Expired RUNNING ACQUISITION leases require an **atomic paired transition** of the exact linked Manifest and Job to RECOVERY_REQUIRED, not a Job-only update.

Lock order: Manifest then Job. PostgreSQL time decides lease expiry; revalidate linkage and states under locks. Never erase START_RESERVED / provider task identity, retry StartDownload, reset failure budget, or fabricate terminal Manifest state.

The existing generic expired-Job sweep must not independently place an ACQUISITION Job into RECOVERY_REQUIRED. No continuous scheduling/runtime is authorized by this Gate.

## 5.5 One-shot runner boundary

D-036 / Gate 3.13 connects D-035 and D-034 in one explicit bounded tick:

```text
bounded paired expired acquisition recovery
    → if no recovery debt: type-scoped ClaimNextByType(ACQUISITION)
    → if eligible Job: one StageDispatcher.Dispatch
    → return IDLE / recovery / one-stage outcome
```

Never claim an unrelated Job; never perform two Manifest stages in one tick; never convert a stage error into a generic Job-only Fail/Succeed/RetryAt mutation; never automatically repeat a provider side effect.

Gate 3.13 does not start an autonomous background worker, timer, process runtime, or live credential integration. Later runtime wiring requires a new Gate.

## 5.6 Opt-in worker lifecycle

D-037 / Gate 3.14 makes D-036 RunOnce repeatable under one controlled, opt-in serial worker:

```text
disabled-by-default worker, validated unique owner and bounded policy
    → one RunOnce at a time
    → interruptible bounded wait
    → next distinct tick or graceful shutdown
```

IDLE/RECOVERY_ONLY/STAGE_COMPLETE are normal; errors must not bypass stage stores or mutate Job-only terminal state; ErrLeaseRecoveryDebt must stop new work. Bound tick duration below lease with a documented margin. Cancellation propagates to in-flight work while START_RESERVED remains the durable duplicate-start barrier.

Do not claim enabled production service until real PostgreSQL, IndexCore and 115 authenticated runtime composition is complete in a separately accepted Gate. A process enabled without dependencies must fail closed; default process remains side-effect-free.

## 5.7 Runtime composition boundary

D-038 / Gate 3.15 may compose one accepted runtime graph of Panta PostgreSQL stores, IndexCore Q5/Q8 read client plus distinct trusted Hint port, exact provider-session registry, D-017 Journal Projector, D-036 RunOnce and D-037 serial Worker.

Require validated schema v12, complete mandatory clients, injected secrets and exact ProviderID/ConnectionID/CredentialRef match *before* enabling worker; no fallback to mocked integrations. The main Panta DB is never IndexCore's DB. Hint acceptance never implies READY; only D-033 canonical Copy confirmation may finalize.

Default disabled process must remain side-effect-free. Controlled PostgreSQL+HTTP-fake E2E is in scope; real 115 account secrets and live deployment are not yet authorized. No direct Panta OpenList verification, Job Engine duplication, or new schema is allowed.

## 5.8 Protected operator bootstrap

D-039 / Gate 3.16 is accepted: stock CLI remains disabled and side-effect-free; an explicit read-only preflight validates externally protected credentials, trusted Hint configuration, schema v12 and exact active ProviderID/ConnectionID/CredentialRef coverage **without** claiming Jobs, submitting hints, migrating, contacting 115, or mutating any data.

Explicit operator worker start reuses exactly one accepted D-038 runtime graph with a restricted external SecretResolver, D-037 serial Worker and SIGINT/SIGTERM cancellation. No raw cookie/token in CLI process args, logs, Git or Panta tables; no silent credential fallback.

Gate 3.16 does not authorize AI to run a real 115 account download or deploy to production. Only local controlled PostgreSQL/HTTP fakes were accepted as evidence.

## 5.9 Deterministic source intake and submission

D-040 / Gate 3.17 owns the missing deterministic intake boundary before any human-authorized live-account acceptance.

Supported MVP schemes are initially `magnet`, `http`, `https`, and `ed2k`. Resolution is validation/classification only: it performs no DNS, HTTP, provider, OpenList, IndexCore, Search or Agent work.

The resolver may normalize the **scheme/type** but must preserve the accepted source value exactly:

```text
input source string
  -> validate/classify scheme
  -> Manifest.source_ref = exact accepted input
  -> ExecutionInput.Download.Source.Value = same exact value
```

Do not trim, truncate, rebuild magnet query parameters, percent-reencode, strip suffixes or invent a provider-specific canonical source. HTTP(S) userinfo is rejected. ExpectedName remains explicit caller intent; never guess result_name from the source.

Submission composes the already accepted Manifest creation and activation services. Stable IDs/idempotency must make replay deterministic; an activation failure leaves explicit PENDING intent rather than hidden cleanup. Submission itself never starts the worker or crosses a provider/network side-effect boundary.

Gate 3.17 is accepted.

## 5.10 Human-authorized real staging acceptance

D-041 / Gate 3.18 authorizes one bounded **operator-run staging validation**, not a new product architecture.

Phase A staging harness is accepted and merged:
- PR #61 reviewed exact HEAD `7424e5322fbb061fd14472dc0becde03b9fa6272`;
- Architect Review `5465144212`;
- squash `7a507a2411a26f8e36b0abf70271b3a8417942d7`;
- exact-head CI `37874559508` all green.
The merged harness is read-only evidence collection only. **Gate 3.18 remains open until the human operator executes Phase B with the real staging 115/OpenList/IndexCore environment and returns redacted evidence.**

Pinned external IndexCore baseline:

```text
v0.4.0-alpha.1
6f0eec85c59bd8cbe55011b0d9e512e0cafd6615
```

The staging path is:

```text
Panta protected submission
  -> real 115 provider mutation
  -> provider result_name
  -> trusted loopback IndexCore Mutation Hint
  -> IndexCore hybrid incremental runtime
  -> IndexCore OpenList collector
  -> Canonical + Journal
  -> Panta Q5/Q8 Projector
  -> exact PRESENT Copy
  -> READY + result_copy_id + Job SUCCEEDED
```

Panta still never queries OpenList for acquisition truth. Provider success and Hint 202 remain non-terminal. Because IndexCore's accepted Hint listener is literal-loopback-only, Panta and IndexCore share one host/network namespace for this acceptance rather than weakening trust for separate containers.

Real secrets stay outside Git/chat/reporting. CI/AI must not use the real account. A human explicitly starts the worker. If START_RESERVED becomes uncertain, stop and preserve evidence; never force another StartDownload.

Acceptance uses bounded polling of durable state and IndexCore Query/Journal, not blind sleep. A failure is diagnostic evidence, not permission to bypass D-029/D-033 or add a second observer.

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
- Gate 3.9 — durable acquisition result locator
- Gate 3.10 — canonical Q5 + projected Copy + atomic READY finalization
- Gate 3.11 — one-claim acquisition stage dispatcher and controlled E2E
- Gate 3.12 — acquisition-only claims and atomic expired-lease recovery
- Gate 3.13 — bounded one-shot acquisition RunOnce
- Gate 3.14 — opt-in serial acquisition worker lifecycle
- Gate 3.15 — fail-closed runtime composition and controlled PostgreSQL+HTTP E2E
- Gate 3.16 — protected operator bootstrap, read-only preflight and explicit worker startup
- Gate 3.17 — deterministic Source Resolver and acquisition submission

Authorized now:
- **Gate 3.18 — Issue #60 — Human-authorized real 115 + OpenList + IndexCore staging E2E**

Not authorized yet:
- direct Panta OpenList acquisition verifier;
- direct acquisition Copy creation/upsert;
- production deployment, public API/UI, or Gate 4+ implementation before Gate 3.18 closeout;
- weakening IndexCore loopback Hint trust to accommodate deployment topology;
- provider-specific source rewriting/truncation without a new decision;
- auth/quota/share/access/usage;
- Agent implementation;
- generalized workflows/microservices/distributed queue.

## 10A. Acquisition result locator

Before provider success may advance to observation, Panta must durably know one top-level result basename.

```text
provider-observed TaskStatus.ResultName
        >
Manifest.ExpectedName fallback
        ↓
Manifest.result_name
```

Rules:
- target_path remains a directory scope;
- result_name is one safe path segment, not a path;
- never infer the acquired result by listing the target directory or choosing a new/sole object;
- provider FileId/DirId/info_hash are not IndexCore physical identity;
- missing result identity requires explicit recovery;
- future exact canonical candidate path is `Join(target_path, result_name)`;
- result_name itself is not Canonical truth and never READY.

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
