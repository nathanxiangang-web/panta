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

Before provider success may advance a new acquisition beyond the provider stage:

```text
StorageBinding.indexcore_root_id
+ Manifest.target_path
+ Manifest.expected_name
```

- `target_path` is the directory scope refreshed by IndexCore.
- `expected_name` is one exact direct child under that directory.
- It may be absent at Manifest creation, but must be frozen before `AWAITING_VISIBILITY`.
- For 115, only successful `OfflineTask.Name` may supply it.
- Never use provider FileId/DirId, OpenList paths, Journal order, timing, newest-item, or only-item heuristics as a substitute.
- This identity still does not prove canonical presence; IndexCore confirmation remains required.

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
