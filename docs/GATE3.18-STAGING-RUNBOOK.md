# Gate 3.18 — human-operated staging acceptance runbook

For Issue #62's diagnostic-only, separately authorized fresh attempt, use the
[single-attempt diagnostic plan](GATE3.18-SINGLE-ATTEMPT-DIAGNOSTIC.md) as the
additional approval/queue-isolation/stop checklist. That plan is not permission
to start a download or retry any historical reservation.

Issue #60 / D-041. This runbook prepares one staging-only real-account test;
merging the harness does **not** execute it or authorize automation to start a
real 115 download. The human operator controls the source, destination,
credentials, start command and stop decision. No production deployment, new
schema, alternate verifier or second scheduler is part of this Gate.

## Impact, stop and rollback

`submit-acquisition` writes one Panta Manifest and ACQUISITION Job. It does not
contact 115. `start-acquisition` is the **only** step that may create a real 115
offline task and trigger IndexCore observation through the accepted Hint. Use a
small, non-sensitive, operator-controlled HTTPS object with a stable basename,
and an isolated 115 staging directory. Do not use copyrighted/private media.

To stop, send SIGTERM to the Panta worker and wait for exit. Do not automatically
delete the 115 task, downloaded file, Manifest, Job, Copy or IndexCore evidence.
If `START_RESERVED` exists and the provider reference is uncertain, **stop**:
never resubmit with a new ID, clear the reservation or call StartDownload again.
Other STOP conditions are listed below. A failed run remains diagnostic data.

## Module boundaries and topology

```text
protected request -> Panta submit-acquisition -> Manifest + Job
human start       -> Panta worker -> 115 DownloaderProvider
                                      -> trusted Hint (127.0.0.1 only)
IndexCore P10/P11 -> OpenList collector -> Canonical + Q8 Journal
Panta Q5/Q8       -> Projector Copy -> READY + result_copy_id + Job SUCCEEDED
```

Panta never queries OpenList for acquisition truth or reads IndexCore's DB.
The evidence tool `cmd/panta-staging` is **read-only**: it checks Panta schema
v12, reads explicit Panta columns in a read-only transaction, and calls only
IndexCore Q5/Q8/Q9. It has no command to submit or start a worker. Its JSONL
report contains IDs, states, counters, SHA-256 of the source/result name,
generation, cursor and canonical resource ID; it excludes raw source, cookie,
provider task reference, Hint token, OpenList credential and DB URL.

Record these four independent coordinates before any mutation; never derive
one by string concatenation or path guessing:

| Coordinate | Staging value (operator fills privately) | Owner |
| --- | --- | --- |
| `provider_scope` | dedicated 115 saveDirID | Panta StorageBinding |
| `openlist_mount_path` and collector path | real OpenList staging mount/scope | IndexCore collector config |
| `indexcore_root_id` | one ACTIVE root ID | IndexCore |
| `Manifest.target_path` | root-relative destination directory | Panta acquisition |

Panta and IndexCore must share a **host/network namespace**. Two isolated Docker
containers with different loopbacks are not acceptable. Do not change the
trusted Hint listener to `0.0.0.0` or a bridge IP to make this test work.

## 1. Prepare the fixed external baseline (no acquisition yet)

Use the IndexCore prerelease `v0.4.0-alpha.1`, whose annotated tag peels to
`6f0eec85c59bd8cbe55011b0d9e512e0cafd6615`. Verify the running binary
and source/image provenance against that commit; do not mix an upgrade with this
acceptance. Record the exact Panta commit and OpenList version in the report.

IndexCore's accepted `serve` mode must have one writer, a compatible schema,
`INDEXCORE_INCREMENTAL_RUNTIME_ENABLED=true`, an ACTIVE OpenList-backed root,
and a successful baseline observation. Configure the separate trusted Hint
listener with literal `127.0.0.1:<port>` and a token of at least 32 bytes.
The Query API stays on a private/loopback address; `/readyz` must succeed. The
token is an env-only secret to IndexCore; Panta receives the matching token
through its protected bootstrap mount. Do not paste either value into a shell
command, Git, chat or the evidence report.

IndexCore operator checks (run in its configured staging environment):

```text
indexcore version
indexcore doctor
indexcore root list
indexcore root adapter get --root-id <staging-root-id>
GET <private-query-base>/readyz  -> HTTP 200
```

The root adapter must be `openlist` with `token_env` or
`username_env`/`password_env` references, not embedded plaintext. Confirm
the real OpenList mount can see the dedicated 115 staging destination. The
P10/P11 runtime and writer lock belong to the same `indexcore serve` process;
do not launch a competing manual `incremental run` while it owns the lock.

Keep Panta and IndexCore database names distinct. Verify the two database
settings locally without printing either DSN. Check the IndexCore Hint socket
is bound only to loopback. **Before starting the worker**, verify its planned
process/container network namespace is the same as the running IndexCore
`serve` process (`/proc/<pid>/ns/net` or the service/container configuration).
If that placement cannot be proved, do not start the worker.

## 2. Protected inputs and non-mutating preflight

On the staging host, create owner-only (`0700`) directories and owner-readable
(`0600`) files outside Git. The Panta bootstrap file names the exact
ProviderID/ConnectionID/CredentialRef, the protected 115 cookie file, and the
separate Hint token file. IndexCore's OpenList credential, Hint token and DB
passwords remain in its protected runtime environment. Do not copy the user's
desktop credential file into this repository or into a command line.

Required Panta environment variables are `PANTA_DATABASE_URL`,
`PANTA_INDEXCORE_BASE_URL`, `PANTA_ACQUISITION_BOOTSTRAP_FILE` and
`PANTA_ACQUISITION_WORKER_ENABLED=true`. Supply values from protected service
configuration, not inline shell arguments. The preflight only validates local
secrets and exact active session coverage; it cannot authenticate 115 or prove
an external service will complete a download.

Build the staging evidence tool from this PR's reviewed Panta commit. Set:

```text
PANTA_STAGING_ROOT_ID=<the ACTIVE IndexCore root ID>
PANTA_STAGING_MANIFEST_ID=<stable UUID for this one run>
PANTA_STAGING_JOB_ID=<different stable UUID for this one run>
PANTA_STAGING_BASELINE_FILE=<absolute protected baseline JSON path>
PANTA_STAGING_DEADLINE_SECONDS=<60..7200 for watch only>
```

The IDs and root are non-secret. Never put the source URL in these variables.
Set `umask 077` before redirecting reports. The evidence command has three
read-only modes:

```text
panta-staging baseline  > <protected-baseline.json>
panta-staging inspect   > <protected-inspect.json>
panta-staging watch     > <protected-timeline.jsonl>
```

Before submission, run `panta-staging baseline`; it checks Panta schema v12,
IndexCore Q9 ACTIVE root/generation/admission sequence, drains bounded Q8
pages to the last `event_seq`, and records Panta Job count, total claim
attempts and provider-task count. Then run
`panta preflight-acquisition` explicitly. Capture a second baseline and
compare Job/provider-task counts, total claim attempts, Q9 admission sequence
and Q8 cursor. Autonomous IndexCore activity
may legitimately advance Q9/Q8; investigate drift rather than attributing it
to preflight. Preflight must not claim a Job, call 115, send a Hint or write
either database. If preflight fails, do not submit.

## 3. Human submission — still no provider mutation

Prepare exactly one protected JSON request file per
`docs/ACQUISITION-SUBMISSION.md`, using the stable IDs above, dedicated
StorageBinding, explicit target_path, and the small controlled HTTPS URL.
Do not paste that URL into terminal arguments or evidence. Keep the original
source bytes in the file; do not transform or shorten them. The source must
have a stable basename so provider ResultName and the final candidate can be
checked. Set `PANTA_ACQUISITION_SUBMISSION_FILE` to the file path.

The human now invokes:

```text
panta submit-acquisition
panta-staging inspect > <protected-inspect.json>
```

`inspect` exits successfully only for `READY_TO_START`: Manifest `ACTIVE`,
same Job `QUEUED`, `claim_attempts=0`, no provider-task row. Confirm the
source SHA-256 in its report against a locally calculated digest without
printing the URL. If a submission returns `PENDING_ACTIVATION`, do not start
the worker; diagnose and retry the **same** ManifestID/JobID and request file.

## 4. Human explicit start and bounded observation

Only after `READY_TO_START`, the human invokes the existing
`panta start-acquisition` in the foreground or a controlled service session.
Record its PID without recording its environment. Reconfirm its actual network
namespace is identical to the IndexCore `serve` PID. A mismatch is a STOP
condition: SIGTERM Panta immediately and correct placement; the pre-start
placement check above is mandatory because this post-start check alone cannot
prevent a worker from claiming a Job.

In another protected operator session, invoke `panta-staging watch` with a
bounded deadline. It emits one redacted JSON object per poll (five-second
interval) to a protected JSONL file. It does **not** send Hint or claim work.
The timeline records:

- Manifest state, hashed result_name, result_copy_id;
- Job state, claim_attempts and failure_count;
- provider-task START_RESERVED / REFERENCE_KNOWN and reference presence, not
  the raw reference;
- IndexCore Q9 generation, Q5 exact candidate match/ambiguity/resource ID;
- Q8 last event_seq and exact resource event after the baseline cursor;
- Panta projector cursor and Copy availability.

The watch exits `PASS` only for one exact Q5 PRESENT resource, new Q8 evidence
for that resource, matching PRESENT Panta Copy, advanced projection cursor,
`Manifest READY + result_copy_id + same Job SUCCEEDED`, a known provider task
reference and a post-baseline Q9 generation. Provider `SUCCEEDED` or Hint
`202 Accepted` alone never passes. On STOP/TIMEOUT, send SIGTERM to Panta,
wait for process exit, and preserve all durable records. Do not use a blind
fixed sleep as success evidence.

## 5. Human acceptance record

Attach only the redacted baseline, inspect and timeline JSON/JSONL plus a
short operator note. Include Panta commit, pinned IndexCore tag+commit,
OpenList version, separate DB identities (names only), namespace equality,
four-coordinate table, source SHA-256/scheme, one provider task creation
evidence, signal/shutdown outcome, ManifestID, JobID, result_copy_id,
canonical resource_id, and final PASS/STOP reason. Do not attach raw process
environments, service files, source URL, 115 cookie, OpenList credential,
Hint token, provider task reference, or password-bearing DSN.

STOP rather than workaround if session/coordinate coverage mismatches, root
is not ACTIVE, there is a second IndexCore writer, Hint is not literal
loopback, a provider reference is uncertain after START_RESERVED, Q5 is
ambiguous/mismatched, paired recovery debt appears, or any sensitive value is
disclosed. A real failure is evidence for architect review, not permission to
bypass IndexCore ownership, clear fences or rewrite the source.
