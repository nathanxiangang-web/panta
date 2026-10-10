# Issue #62: human-operated single-attempt diagnostic plan

**PLAN ONLY. No source submission, Worker start or real 115 mutation is
authorized by this document.** Governing instruction: Architect comment
6095304635 and review 5478202260. Use the accepted
[staging runbook](GATE3.18-STAGING-RUNBOOK.md) for topology and acceptance, and
[diagnostic evidence](GATE3.18-START-DIAGNOSTICS.md) for the parser contract.
Gate 3.18 Phase B remains STOP; Issues #62/#60 remain OPEN.

## Impact, authorization and preservation

The environment owner must affirmatively approve ONE exact harmless HTTPS
object (preferably <=1 MiB), its `source_ref_sha256` as defined below, an empty
dedicated 115 destination,
fresh distinct Manifest/Job UUIDs, selected Binding/Root, and a 20-minute window.
The original source belongs only in the protected submission file. Approval
must not disclose it in GitHub/chat; `source_ref_sha256` and a private file path
identify the approved source string.
Neither this plan nor the older general staging authorization replaces that
approval. Do not prepare a real request file on behalf of an unapproved run.

Submission adds one durable pair. One explicit Worker process may make one
StartDownload for that Manifest, then only poll the known reference and advance
accepted stages on later claims. Multiple ClaimAttempts during a successful
chain are expected; they are NOT additional download submissions. No process
restart, fresh-ID retry, SQL requeue, fence removal or ad hoc POST is allowed.

Five historical RECOVERY_REQUIRED/START_RESERVED pairs must stay unchanged.
They are not automatically claimable and must not be deleted to obtain a clean
queue. Historical recovery state is distinct from corrupt linkage/expired
RUNNING recovery debt. On uncertainty or error: terminate this Worker, join it,
inspect remotely read-only, and retain every record and any downloaded file.
Stopping cannot undo a task that 115 may already have accepted. Cleanup/cancel
is outside this authorization. No production deployment or schema migration.

### Digest definitions (do not interchange)

- `source_ref_sha256` = SHA-256 of the exact original `source_ref` string,
  decoded from the protected request JSON and encoded as UTF-8, **without an
  appended terminating newline**. Preserve every actual source character;
  do not trim, normalize, URL-decode, re-encode or reconstruct the string.
  JSON escape sequences must be decoded first; do not hash their literal
  spelling, the JSON envelope, the request file, or newline-added shell output.
- The existing `panta-staging` field named `source_sha256` uses this same
  definition: SHA256(UTF8(exact persisted Manifest.source_ref)). Compare its
  value to the approved `source_ref_sha256`; no production field is renamed.
- `object_content_sha256`, if separately checked by the operator, is SHA-256
  of the downloaded object's file bytes. It is optional, distinct evidence,
  NOT the source-ref identity/digest used by approval or staging comparison.
- Binary SHA-256 values identify executable bytes, not either source digest.

## 1. Preconditions (read-only; all must pass before approval/start)

- Build provenance: reviewed HEAD `a140486f9ff7626a6a322764ba4b880c2e969583`,
  merged as `9e2e2e5cdaf140e97c030b0678e1418660d1b48c`.
  Record actual deployed revision and binary SHA-256. Do not use the older
  staging binary without parser_stage/response_shape. Confirm source tree
  matches the reviewed code; no unreviewed patch or SDK upgrade.
- IndexCore `v0.4.0-alpha.1`, SHA
  `6f0eec85c59bd8cbe55011b0d9e512e0cafd6615`, one serve writer with P10/P11,
  ACTIVE OpenList root, healthy Query readiness and successful baseline.
  Panta schema v12, separate Panta/IndexCore DB identities, no migrations.
- Same host/network namespace as IndexCore; Hint listener literal 127.0.0.1
  and matching protected >=32-byte token. No boundary relaxation.
- Exact ACTIVE connection/session/credential-ref and provider saveDirID agree.
  Independently verify provider_scope, OpenList collector path, Root and
  Manifest target_path; do not derive native IDs from paths.
- Read-only 115 inventory before submission: approved destination exists and
  is EMPTY; scan task pages and record coverage/limits, counts and absence of
  the approved test object/destination. No task refs, names or URLs in reports.
  If the earlier destination still contains its baseline marker, it is NOT
  empty: stop and obtain a separately approved empty destination/configuration.
  Do not delete the marker or improvise a Binding during this run.
- No other Worker/service/container/cron may claim this Panta DB. Freeze other
  submitters for the window. No other QUEUED, RETRY_WAIT (even future-dated), or
  RUNNING ACQUISITION Job; no unexpected active Manifest or corrupt links.
  The Worker does NOT support selecting a Job by ID. Queue isolation is a
  hard prerequisite, not a claim that the command filters by ManifestID.
- Inspect historical pairs read-only, including reverse linkage, payload
  schema_version=1/manifest_id and acquisition:<ManifestID> idempotency key.
  Any missing/mismatched link or expired RUNNING task means STOP; do not run
  recovery as a preparation shortcut. Existing five uncertain pairs stay put.
- Credentials/config/reports remain outside Git in owner-only 0700 directories
  and 0600 regular files, no symlinks. Never use set -x, env dumps, psql URI
  arguments, HTTP debug, packet dumps, raw body fixtures or plaintext secrets.

Read-only SQL can print safe aggregate state counts, not payloads/source refs:

```sql
BEGIN READ ONLY;
SELECT state, count(*) FROM jobs WHERE job_type='ACQUISITION' GROUP BY state;
SELECT count(*) AS unexpected_live_work FROM jobs
 WHERE job_type='ACQUISITION' AND state IN ('QUEUED','RETRY_WAIT','RUNNING');
SELECT count(*) AS expired_running FROM jobs
 WHERE job_type='ACQUISITION' AND state='RUNNING'
 AND lease_expires_at <= CURRENT_TIMESTAMP;
SELECT state, count(*), count(result_copy_id)
 FROM acquisition_manifests GROUP BY state;
SELECT state, count(*), count(provider_task_ref)
 FROM acquisition_provider_tasks GROUP BY state;
COMMIT;
```

Before submission unexpected_live_work and expired_running must both be zero.
Counts are not sufficient to prove linkage integrity; the operator must also
verify the safe ID/state pairs. No repair UPDATE/DELETE is part of this plan.

## 2. Operator shell setup and read-only preflight

These are Bash commands for the staging host, NOT PowerShell. Replace bracketed
paths/IDs privately after selecting the exact reviewed binaries/configuration.
Do not paste secrets or the source URL into these commands. Environment files
must export the accepted variables, and must themselves be owner-protected.

```bash
set -euo pipefail
umask 077
set +x
source /home/nathan/staging-gate318/private/panta-db.env
source /home/nathan/staging-gate318/private/panta-runtime.env
export PANTA_ACQUISITION_WORKER_ENABLED=true
export PANTA_ACQUISITION_WORKER_INTERVAL=5s
export PANTA_ACQUISITION_WORKER_TICK_TIMEOUT=30s
export PANTA_ACQUISITION_WORKER_LEASE_DURATION=2m
export PANTA_ACQUISITION_WORKER_RETRY_DELAY=10s
export PANTA_STAGING_DEADLINE_SECONDS=1200
PANTA_BIN='/absolute/reviewed/bin/panta'
STAGING_BIN='/absolute/reviewed/bin/panta-staging'
export PANTA_STAGING_ROOT_ID='<approved-active-root-uuid>'
export PANTA_STAGING_MANIFEST_ID='<fresh-manifest-uuid>'
export PANTA_STAGING_JOB_ID='<different-fresh-job-uuid>'
RUN_DIR='/home/nathan/staging-gate318/evidence/<fresh-run-label>'
mkdir -m 700 -- "$RUN_DIR"  # must be new; an existing directory is a STOP
export PANTA_STAGING_BASELINE_FILE="$RUN_DIR/baseline.json"
sha256sum "$PANTA_BIN" "$STAGING_BIN" > "$RUN_DIR/binary-digests.txt"
"$STAGING_BIN" baseline > "$RUN_DIR/before-preflight.json"
"$PANTA_BIN" preflight-acquisition > "$RUN_DIR/preflight.log" 2>&1
"$STAGING_BIN" baseline > "$PANTA_STAGING_BASELINE_FILE"
```

Compare Job/provider counts and total claims before/after preflight; investigate
autonomous Q9/Q8 drift separately. Verify /readyz, ACTIVE root, protected
credentials, source/destination readiness and zero other runnable work before
continuing. This does not authenticate 115 or prove the submit endpoint works.

## 3. Approval checkpoint, protected request and enqueue

Required affirmative record: "Authorize ONE real 115 diagnostic attempt for
source_ref_sha256 <digest>, private request file <path>, Manifest <UUID>, Job
<UUID>, Binding <UUID>, Root <UUID>, destination <approved fingerprint>,
20-minute window. Preserve all historical reservations; STOP on first error."

Only after that approval, the human creates one protected regular JSON file per
[submission contract](ACQUISITION-SUBMISSION.md). Set manifest_id/job_id to the
approved fresh UUIDs, max_attempts=1, source_ref to the EXACT approved original
bytes, selected target_storage_binding_id/target_path and explicit expected_name.
No source normalization. Ensure both IDs are absent from the DB and no matching
remote task exists; verify `source_ref_sha256` privately before enqueueing,
without printing the source. Compare it to the approval record, not to
`object_content_sha256`.
Do not create this file by echoing its contents into recorded terminal history.

```bash
export PANTA_ACQUISITION_SUBMISSION_FILE='/absolute/protected/approved-request.json'
python3 - "$PANTA_ACQUISITION_SUBMISSION_FILE" <<'PY'
import hashlib
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as request_file:
        source_ref = json.load(request_file)["source_ref"]
    if not isinstance(source_ref, str):
        raise ValueError("invalid source type")
    digest = hashlib.sha256(source_ref.encode("utf-8")).hexdigest()
except (OSError, ValueError, KeyError, TypeError):
    raise SystemExit("STOP: source_ref digest verification failed")
print("source_ref_sha256=" + digest)
PY
```

The human must compare that digest to the expressly approved value and STOP
on any mismatch BEFORE pasting the following enqueue commands. The Python
example prints only the digest; its output newline is not part of the hashed
input. This is future operator verification, not permission to read the real
protected request during documentation work.

```bash
"$PANTA_BIN" submit-acquisition > "$RUN_DIR/submission.log" 2>&1
"$STAGING_BIN" inspect > "$RUN_DIR/inspect.json"
```

Require CREATED (not unexpected REPLAY), READY_TO_START, exact approved IDs,
ACTIVE/QUEUED, claim_attempts=0, and inspect.source_sha256 equal to the approved
`source_ref_sha256`, with no provider-task row.
Stop on PENDING_ACTIVATION, conflicting replay or any error; do not automatically
repeat submission. Recheck queue inventory: exactly ONE live ACQUISITION Job,
the approved Job, zero RUNNING/RETRY_WAIT/expired Jobs and no other submitters.
This is still enqueue-only; starting is the distinct action below.

## 4. Exactly one bounded Worker start and observation

Human pastes this block ONLY after the preceding approval and checks. It uses
the stock product commands, not an alternate downloader or scheduler. The
transient shell only supervises exit and read-only evidence; it creates no Jobs.
It never restarts the Worker. IndexCore remains the sole OpenList observer.

```bash
(
  set -euo pipefail
  worker_supervisor_pid=''
  watch_pid=''
  stop_children() {
    for pid in "$worker_supervisor_pid" "$watch_pid"; do
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
      fi
    done
    for pid in "$worker_supervisor_pid" "$watch_pid"; do
      if [ -n "$pid" ]; then
        if wait "$pid" 2>/dev/null; then rc=0; else rc=$?; fi
        printf '%s %s\n' "$pid" "$rc" >> "$RUN_DIR/supervisor-exits.txt"
      fi
    done
  }
  trap stop_children EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  # timeout forwards TERM, then bounds a hung shutdown by 15 additional seconds.
  timeout --signal=TERM --kill-after=15s 1200s \
    "$PANTA_BIN" start-acquisition > "$RUN_DIR/worker.log" 2>&1 &
  worker_supervisor_pid=$!
  printf '%s\n' "$worker_supervisor_pid" > "$RUN_DIR/worker-supervisor.pid"
  "$STAGING_BIN" watch > "$RUN_DIR/timeline.jsonl" \
    2> "$RUN_DIR/watch.log" &
  watch_pid=$!
  while kill -0 "$worker_supervisor_pid" 2>/dev/null && \
        kill -0 "$watch_pid" 2>/dev/null; do
    if grep -Eq 'first_error_category=[A-Z_]+|event=TRANSIENT_ERROR|panta stopped:' \
         "$RUN_DIR/worker.log"; then
      printf '%s\n' 'STOP: first worker diagnostic/error' > "$RUN_DIR/stop-reason.txt"
      break
    fi
    sleep 1  # supervisor cadence, NOT evidence of provider success
  done
  # PASS, STOP, timeout, error and unexpected exit ALL terminate/join this Worker.
)
```

The timeout supervisor shares the operator shell's network namespace; prove
that namespace equals IndexCore BEFORE start. Never mistake the recorded
supervisor PID for the actual Worker PID. No detached/persistent supervisor or
service restart policy is allowed. Error log monitoring stops within one poll;
watch checks durable uncertainty every five seconds. START_RESERVED continues
to prevent a second StartDownload even during this brief shutdown interval.
Failure to observe/stop, force-kill, interrupted shutdown or Worker timeout is
STOP and must be reported, not automatically retried. As a hard preflight
requirement, the operator must have a verified way to identify the actual
Panta Worker process and confirm its exit after timeout/SIGTERM. Bash syntax
validation does not test live signal delivery, process groups or child cleanup.
Record supervisor/watch exit codes; a supervisor exit code alone is not proof
of graceful Worker shutdown. Confirm no remaining child Worker before handoff.

If provider succeeds, the SAME process may continue later claims through Hint,
Q5/Q8 and the existing Projector within the same bound. Never launch a second
start-acquisition to "finish" this run. If twenty minutes is insufficient,
report TIMEOUT and retain the job/task rather than expanding the approval.

## 5. Read-only aftermath, parser matrix and handoff

After joining the Worker, collect safe durable ID/state/counter/reference-
presence fields and recheck all five historical pairs against the baseline.
Do not use inspect as a general post-run report: inspect is a READY_TO_START
gate and intentionally exits STOP for already-claimed Jobs. Use the final
watch record plus read-only SQL and the existing human provider inquiry path.
Check the approved destination/task list read-only, with pagination/coverage
limits. No match is NOT proof the remote task was never created.

Only publish these fields: deployed revision/binary digest, safe run IDs, timestamp,
operation, parser_stage, response_shape, http_status, closed cause_type,
elapsed_ms/category; Manifest/Job/task states, claim/failure counters and
reference presence; Q5/Q8/Copy/result_copy_id evidence. Never publish worker
logs wholesale, raw errors/body, source/result name, task reference, query
parameters, process environment, Cookie, Token, DSN or protected config files.
The approval/private-verification/evidence identity is consistently
`source_ref_sha256`; label staging's existing `source_sha256` as that value in
the handoff. If included, label `object_content_sha256` separately and never
use it as the staging source comparison.

| Observed parser/category | What is proven | Action |
| --- | --- | --- |
| USER_INFO_GET | failure while lazy user lookup is the last recognized operation | STOP; POST not proven reached |
| OFFLINE_POST_TRANSPORT | request transport/deadline failure | STOP; remote side effect remains possible |
| OFFLINE_POST_OUTER_JSON | complete bounded outer response failed pinned DownloadResp parsing; shape may be HTML/TEXT or schema-invalid OBJECT | STOP; do not guess crypto failure |
| OFFLINE_POST_BASE64_CRYPTO | pinned crypto returned Base64 decode error | STOP; do not claim deeper decryption without evidence |
| OFFLINE_POST_DECRYPTED_JSON | outer envelope parsed, later typed JSON failure | STOP; decrypt-output JSON boundary identified |
| OFFLINE_POST_REFERENCE | task reference absent/invalid/ambiguous | STOP; do not choose a likely task |
| AUTH/SOURCE/QUOTA/REQUEST rejection category | recognized provider error category | STOP; operation may remain UNKNOWN |
| UNKNOWN | truncated/incomplete/unrecognized boundary | STOP; no fabricated localization |

PR #64 does not log a separate operation field. Infer OFFLINE_POST only from
its closed POST stage prefix, USER_INFO_GET from that stage; otherwise report
operation UNKNOWN. This inference must be labelled, not represented as captured
wire data. HTTP 200 alone NEVER proves acceptance or a remote task reference.

PASS requires ONE real Provider task with known reference, exact Q5 PRESENT,
new matching Q8 event after baseline, projected PRESENT Copy, Manifest READY
with result_copy_id, and the SAME Job SUCCEEDED (plus the accepted watch
generation/cursor checks). Anything less is STOP/TIMEOUT or partial evidence.
On failure, return the exact safe boundary and coverage limits to the Architect
before proposing a narrowly scoped fixture-backed repair. No SDK guessing,
schema changes, alternate downloader, user-facing 302 or Gate 3.19 work.
