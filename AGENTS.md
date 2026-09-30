# AGENTS.md — Panta AI/Developer Entry Point

This repository uses AI-assisted development.

Before planning, changing, or reviewing code, **read these files first**:

1. `docs/AI-ARCHITECTURE-MEMORY.md`
2. `PROJECT-STATE.md`
3. newest relevant entries in `docs/DECISIONS.md`
4. current authorized GitHub Issue
5. actual current contracts/code/tests

Chat history is not a source of truth.

## Current non-negotiable boundaries

### Observation

```text
Storage / 115
  -> OpenList
  -> IndexCore Collector
  -> IndexCore Canonical + Journal
  -> Panta
```

IndexCore owns physical truth.

**Do not add direct Panta -> OpenList acquisition visibility verification.**

After provider completion, Panta sends a trusted IndexCore Mutation Hint:

```text
root_id   = StorageBinding.indexcore_root_id
scope_key = Manifest.target_path
reason    = POSSIBLE_CHANGE
```

Then Panta waits for IndexCore canonical Query/Journal evidence.

### Control

```text
Panta
  -> Job Engine
  -> Provider Contract
  -> Provider Adapter
  -> Storage
```

Provider success is never READY by itself.

## Coordinate separation

Never derive these from each other:

- `provider_scope` — provider mutation target;
- `openlist_mount_path` — IndexCore/OpenList observation configuration;
- `indexcore_root_id` — IndexCore canonical root;
- `manifest.target_path` — scope inside the selected root/binding.


## Acquisition result identity

Before provider success may advance automatically beyond the provider stage:

```text
valid provider-observed TaskStatus.ResultName
        >
Manifest.ExpectedName fallback
        ↓
Manifest.result_name
```

Canonical candidate identity is later formed from:

```text
StorageBinding.indexcore_root_id
+ Manifest.target_path
+ Manifest.result_name
```

- `target_path` is the directory scope refreshed by IndexCore.
- `expected_name` is request-time intent/fallback and must not be rewritten by provider execution.
- `result_name` is the immutable durable provider-stage locator.
- A valid provider result wins; blank/absent provider result may use ExpectedName fallback.
- Invalid nonblank provider result fails closed; do not silently fall back.
- Never use provider FileId/DirId, OpenList paths, Journal order, timing, newest-item, or only-item heuristics as a substitute.
- This locator still does not prove canonical presence; IndexCore confirmation remains required.

## Canonical completion

READY is not authorized by provider success or Hint acceptance.

For Gate 3.10:
```text
root_id + target_path + result_name
  -> exact Q5 PRESENT resource
  -> existing Q8 Journal Projector
  -> exact PRESENT Copy
  -> optional monotonic Variant binding
  -> result_copy_id + READY + Job SUCCEEDED
```

- zero Q5 matches is pending;
- Q5 ambiguity/multiple matches fails closed;
- acquisition must never create/upsert Copy directly;
- reuse the existing projector in bounded one-page steps;
- `result_copy_id` is the durable final acquisition result reference.

## One-claim acquisition dispatcher

Current authorized Gate 3.11: Issue #46 / D-034.

Dispatch exactly ONE stage per claimed ACQUISITION Job using persisted Manifest.State:

```text
ACTIVE              -> provider Execute + atomic ProviderOutcome
AWAITING_VISIBILITY -> trusted IndexCore Mutation Hint step
AWAITING_CANONICAL  -> exact canonical confirmation step
terminal pair       -> committed no-op / replay
```

- Use Job owner and ClaimAttempts fencing, with PostgreSQL database-time check at every durable stage write.
- After an accepted handoff return; never execute the next stage in the same claim.
- Never convert provider uncertainty into blind StartDownload retry.
- Normal pending status does not consume failure budget.
- Do not implement a continuous worker daemon / automatic ClaimNext scheduler yet.

## Type-scoped claim and linked recovery

Active Gate 3.12: Issue #48 / D-035.

- A future acquisition worker must only claim ACQUISITION Jobs using a database-atomic type filter. Never claim arbitrary Jobs and then discard mismatched types.
- Expired RUNNING ACQUISITION Jobs require a PostgreSQL-time-fenced atomic transition of both linked Manifest and Job to RECOVERY_REQUIRED.
- Lock Manifest before Job, recheck exact idempotency/payload/reverse link under lock.
- Generic Job-only expired-lease sweep must not mutate ACQUISITION Jobs independently.
- Do not drop START_RESERVED / task references or automatically repeat 115 StartDownload.
- Maintain ClaimAttempts generation and FailureCount semantics, no worker daemon or process runtime in Gate 3.12.

## One-shot runner

Current Gate 3.13 / Issue #50 / D-036 allows a single explicit application RunOnce tick:

```text
bounded expired-acquisition linked-pair recovery
  → if healthy: ClaimNextByType(ACQUISITION) at most once
  → StageDispatcher.Dispatch at most once
  → return a typed IDLE / RECOVERY / STAGE result
```

Recovery debt stops new claims in the tick. Stage errors must not be translated into generic Job-only Fail/Succeed/RetryAt writes; that would desynchronize the Manifest. Pass the actual claimed owner/ClaimAttempts. No autonomous worker daemon, timer, process startup wiring or live provider-credential integration yet.

## Opt-in acquisition worker lifecycle

Active Gate 3.14 / Issue #52 / D-037: serial opt-in lifecycle around the accepted RunOnce.

- Worker disabled by default; an enabled process with no real injected dependencies must fail closed at startup.
- Use exactly one worker loop and one in-flight RunOnce per process; a slow tick cannot overlap another.
- Owner stable for the process and unique across replicas; bounded interval, deadline, retry and lease safety margin; never busy spin.
- Treat IDLE/recovery-only/completed tick as normal, pace transient errors, stop on corrupt linked-pair recovery debt.
- Propagate cancellation, no detached task goroutines, no new claim after stop.
- Do not mutate Jobs on runner errors, recreate provider tasks or claim canonical READY from a worker-level result.
- Real 115 credential/IndexCore/PostgreSQL startup graph and deployment remain later authorized work.

## Runtime composition and secrets

Active Gate 3.15 / Issue #54 / D-038.

- Construct a single complete real-protocol graph from accepted Panta PostgreSQL stores, IndexCore Q5/Q8, separate trusted Mutation Hint, provider-session registry, Journal Projector, RunOnce and worker.
- Worker-enabled startup must fail closed if any dependency, schema-v12 guard, trusted Hint configuration or exact provider/connection credential binding is absent.
- Resolve 115 credentials through an externally injected SecretResolver; never put cookies, tokens or user magnets/URLs in Git, Panta DB, logs or diagnostic errors.
- Never query IndexCore DB or inspect OpenList directly from Panta for acquisition visibility; Hint 202 is NOT canonical READY.
- No new acquisition Copy writer; Q8 Projector owns Copy and D-033 canonical transaction owns READY.
- Controlled PostgreSQL + HTTP fake E2E is authorized, live 115 account acceptance / deployment remains a later Gate.

## Job safety

- `claim_attempts` = monotonic claim-generation fence.
- `attempt_count` / `FailureCount` = bounded failure retry budget.
- stale claim generations fail closed.
- provider StartDownload requires the durable START_RESERVED fence.
- START_RESERVED with unknown provider reference must never auto-recreate.

## Provider safety

Current 115 downloader:
- `github.com/SheltonZhu/115driver v1.3.5`;
- Source.Value passed unchanged;
- Target.Scope is the 115 saveDirID;
- task reference is `info_hash`;
- cancel never deletes downloaded files.

Secrets never enter Manifest, Job payload, provider-task persistence, logs, or Agent prompts.

## Team / scope

Exactly:
- 1 Architect
- 1 Developer

The Developer implements only the currently authorized bounded Gate.

Do not start the next Gate until the current Gate is accepted and merged.

## Conflict rule

If old prose/Issue/PR/chat conflicts with a newer accepted Git decision:

```text
newer accepted decision wins
```

Stop, identify the conflict, and synchronize project memory before continuing.
