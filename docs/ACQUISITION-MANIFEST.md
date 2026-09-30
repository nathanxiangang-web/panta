# Acquisition Manifest

Gate 3.1 introduces durable, provider-neutral acquisition intent before any
provider side effect exists. Gate 3.2 adds the atomic boundary that makes that
intent executable.

```text
validated acquisition request
             |
             v
  PENDING Acquisition Manifest
             |
             v
  atomic ACTIVE + Job link
             |
             v
       later Gate: provider
```

## Module boundaries

- `internal/acquisition` owns the Manifest model, closed milestone states,
  target-path normalization, creation service, and persistence port.
- The creation service reads only Panta-owned StorageBinding and Catalog identity
  ports. It does not create Storage or Catalog identity. New acquisition intent
  requires an ACTIVE binding with explicit provider scope and an ACTIVE owning
  StorageConnection.
- `internal/store/postgres` implements Manifest persistence.
- `internal/acquisition` may depend on the Panta-owned, provider-neutral Jobs
  contract for activation and provider contract DTOs for execution input. It
  still cannot depend on Job persistence, provider registry, or adapters.
- Jobs continue to own execution, claim, lease, retry, and recovery behavior.
  The Manifest owns intent, target, optional logical association, and a coarse
  product milestone only.

The execution step's provider port is `DownloaderSessionResolver`, which resolves
the complete accepted execution identity to a `contracts.DownloaderBinding`:

```text
ProviderID + ConnectionID + CredentialRef  ->  contracts.DownloaderBinding
```

That result type lives in `internal/providers/contracts`, so the composition
package and the acquisition domain both depend only on the provider-neutral
contract:

```text
contracts
    ^
    |
acquisition     providers/session
```

Provider identity and provider session identity are different facts, so
`ProviderID` alone never selects authenticated provider state. A real adapter
executes under the credential session belonging to one configured
StorageConnection, and the same `ProviderID` may back several connections with
different credentials. `CredentialRef` is opaque and never carries secret
material, so it is safe to persist.

`internal/providers/session.Registry` satisfies
`acquisition.DownloaderSessionResolver` directly, with no composition adapter.
`internal/providers/session/registry.go` asserts that wiring at compile time,
because a structurally similar but distinct named result type would satisfy Go's
assignability rules in isolation and still fail to wire the real composition.

The acquisition domain cannot import pgx/database/sql, provider execution,
OpenList, IndexCore, Search, Agent, or authentication implementations. Neither
Manifest creation nor activation performs a network call or provider operation.

## Intent validation

Source type and source reference must be non-empty and bounded. Source reference
is stored as opaque input: Gate 3.1 does not parse URLs, magnets, or provider
syntax.

The target identifies a location inside an ACTIVE StorageBinding. It is distinct
from the binding's OpenList mount path. The binding must explicitly configure an
opaque provider scope and its owning connection must be ACTIVE with a valid
provider identity. Target paths:

- are absolute slash paths;
- reject backslashes and `.` / `..` components;
- normalize repeated and trailing slashes;
- are not checked against OpenList existence.

Logical association is optional. If present, it must be shaped and owned as:

```text
Asset -> Release -> Variant
```

A Release requires an Asset and must belong to it. A Variant requires a Release
and must belong to it. Manifest creation never creates Asset, Release, Variant,
or Copy records.

## Persistence

Migration `0005_acquisition_manifests.sql` adds application-supplied Manifest
UUIDs, an ACTIVE StorageBinding target reference, optional Catalog references,
optional future user identity without a user foreign key, and a unique Job
reference used by activation. It introduces no UUID extension or
database-generated UUID default. Migration `0006_enforce_acquisition_activation_link.sql`
requires `PENDING` Manifests to have no Job link and `ACTIVE` Manifests to have
one.

New Manifests are always created in `PENDING` with `job_id=NULL`; the public
creation request cannot pre-link execution state. The frozen state set also reserves
`ACTIVE`, `AWAITING_VISIBILITY`, `AWAITING_CANONICAL`, `READY`, `FAILED`,
`RECOVERY_REQUIRED`, and `CANCELED`.

## Atomic activation

Only a `PENDING` Manifest with no Job link can newly activate. One PostgreSQL
transaction locks the Manifest, creates a generic `QUEUED` Job of type
`ACQUISITION`, links `job_id`, and changes the Manifest to `ACTIVE`. Both records
commit or neither does.

The Job uses application-supplied identity, a positive `max_attempts`, and the
deterministic idempotency key `acquisition:<manifest_id>`. Its versioned payload
contains exactly `schema_version=1` and `manifest_id`; source references, target
paths, credentials, and provider data remain exclusively in their owning model.

An `ACTIVE` replay returns the linked durable Job with `Changed=false`, even if
the retry proposes another Job ID. It does not rewrite timestamps. Missing or
mismatched linkage fails closed. All other Manifest milestones reject activation.
Row locking makes same-ID and different-ID concurrent requests converge on the
single committed link without an orphan Job.

## Side-effect-free execution input

Gate 3.3 resolves only an `ACTIVE` Manifest with a durable Job link. It reads the
explicit StorageBinding and StorageConnection and assembles:

```text
ProviderID       = connection.provider_type
CredentialRef    = connection.credential_ref (opaque reference only)
Source.Scheme    = manifest.source_type
Source.Value     = manifest.source_ref
Target.Scope     = binding.provider_scope
Target.Path      = manifest.target_path
```

The resolver preserves source reference and provider scope exactly. It does not
derive scope from `openlist_mount_path` or `indexcore_root_id`, load credentials,
query the provider registry, invoke a DownloaderProvider, or perform network IO.
Migration `0007_storage_binding_provider_scope.sql` leaves provider scope nullable
for observation-only bindings, has no default, and rejects empty or overlong
non-null values.

## Durable provider-task linkage and the side-effect fence

Gate 3.4 crosses the external side-effect boundary exactly once and records what
it did. Migration `0008_acquisition_provider_tasks.sql` adds one provider-neutral
table, refined by the append-only migration
`0009_provider_task_side_effect_fence.sql`:

```text
acquisition_provider_tasks
  manifest_id        uuid PRIMARY KEY  -> acquisition_manifests
  job_id             uuid NOT NULL UNIQUE -> jobs
  provider_id        text   (bounded opaque registry identity)
  provider_task_ref  text NULL (bounded opaque provider reference)
  state              text   START_RESERVED | REFERENCE_KNOWN
  created_at, updated_at
```

One Manifest maps to at most one provider task and one Job maps to at most one
provider task. Provider identity and reference values are bounded opaque text:
Panta never parses or normalizes them, and the table stores no provider status
because the provider remains authoritative for its own task lifecycle. There are
no database defaults and no provider-specific columns. A conflicting identity
fails closed rather than overwriting a known task reference.

`state` records **side-effect certainty only**, never provider task progress. The
database enforces the invariant so an uncommitted attempt can never look
successful:

```text
START_RESERVED    -> provider_task_ref IS NULL
REFERENCE_KNOWN   -> provider_task_ref IS NOT NULL and non-blank and <= 1024
```

The durable row is therefore written **before** the external call, not after. It
exists in two phases:

```text
no row                                   -> persist START_RESERVED  (exclusive)
START_RESERVED + reference               -> persist REFERENCE_KNOWN
START_RESERVED + no reference            -> fail closed, no start
REFERENCE_KNOWN + same reference         -> idempotent replay
REFERENCE_KNOWN + different reference    -> identity conflict
```

The store performs this decision in one transaction under a per-Manifest
`pg_advisory_xact_lock`, because transactional uniqueness alone cannot serialize
concurrent claimers: two executions could each observe "no row" before either
commits and both would be authorized to start a task. Exactly one claimer can
observe a fresh reservation.

## One bounded execution step

`acquisition.ExecutionStepService` performs one bounded provider step for one
fenced `RUNNING` `ACQUISITION` Job:

```text
fenced Job identity
        |
        v
ACQUISITION Job payload + Manifest linkage validation
        |
        v
Gate 3.3 ExecutionInput (ProviderID, ConnectionID, CredentialRef, DownloadRequest)
        |
        v
connection-scoped downloader session resolution + descriptor identity
        |
        v
durable provider task state
   REFERENCE_KNOWN   -> DownloadStatus for the exact stored reference
   START_RESERVED    -> fail closed: no reference is known
   no row            -> claim START_RESERVED, then StartDownload once,
                        then commit the opaque reference
```

`StartDownload` is reachable only through a newly won `START_RESERVED` claim, so
it can happen at most once per Manifest even under concurrent executions, a lost
reference, or a crash between the external success and the durable commit.

The step fails closed before any provider call when the Job is not an
`ACQUISITION` Job, its payload does not match the Manifest, the Manifest is not
linked to that Job, the Job is not `RUNNING`, the lease owner or attempt does not
match the requesting lease, the lease has expired, the provider is not
registered, the registry entry has no Downloader port, the descriptor identity
differs from the resolved `ProviderID`, or the durable linkage contradicts the
requested identity. Provider-specific errors stay attributed but are never
converted into provider types inside the domain.

Provider task states map only to step outcomes:

```text
PENDING / RUNNING -> PROVIDER_IN_PROGRESS
SUCCEEDED         -> PROVIDER_SUCCEEDED
FAILED            -> PROVIDER_FAILED
CANCELED          -> PROVIDER_CANCELED
```

Provider success is not Manifest `READY`. After provider success, Panta hands
the affected root/scope to the **IndexCore-owned observation pipeline** through a
trusted Mutation Hint. IndexCore performs the OpenList-backed scoped verification.
`READY` still requires canonical IndexCore Query/Journal evidence plus the
corresponding Copy projection/association. Panta does not directly verify OpenList
during acquisition. An empty or invalid returned task reference and an unknown provider task
state both fail closed.

## Uncertain external side effects

If `StartDownload` succeeds but the returned reference cannot be committed, the
step returns `ErrExecutionSideEffectUncertain` and the `START_RESERVED` row
remains durable. Because a start attempt is known while no reference is, that
outcome must be reconciled by an operator rather than treated as ordinary retry:
every later execution observes the reservation and fails closed instead of
starting a second provider task. This holds within the process, across service and
repository reconstruction, and under concurrent executions.

Reservations are never cleared automatically. A crash before the external call and
a crash after it are indistinguishable from durable state alone, so releasing a
`START_RESERVED` row is an explicit operator decision. The marker carries no lease,
attempt, retry, or provider-status semantics; the Job Engine remains the only
owner of execution state.

Two situations leave a reservation behind:

```text
StartDownload returned a reference, but the reference commit failed
StartDownload returned an error, so no external effect could be confirmed
```

Both are treated identically, conservatively: a returned error does not prove the
provider did not accept the request, so the Manifest stays fenced and every later
execution fails closed. The provider is still invoked at most once.

Recovery is an operator procedure, not an automatic retry:

```text
1. Inspect acquisition_provider_tasks for the Manifest: if a row is
   REFERENCE_KNOWN, nothing is wrong and executions poll it normally.
2. For a START_RESERVED row, determine from the provider whether a task
   already exists for the Manifest's DownloadRequest.
3. If a task exists, supply its opaque reference so the reservation
   transitions to REFERENCE_KNOWN.
4. If no task exists, the operator may delete the START_RESERVED row,
   which re-authorizes exactly one future start attempt.
```

Step 4 is why reservations are not auto-cleared: deleting the row is a claim that
no external effect happened, and only the provider side can establish that.

The claim wait is bounded by `lock_timeout` (5s) on the fence lock. A claim that
cannot hold the fence fails closed with `ErrProviderTaskContention` and grants no
authorization, so a stuck lock can never hang a worker or permit a start.

## Credential boundary

`contracts.SecretResolver` is the frozen opaque secret lookup port:

```go
ResolveSecret(context.Context, contracts.CredentialRef) ([]byte, error)
```

Gate 3.5 defines the boundary only; there is no real secret backend yet.
`internal/providers/session.StaticSecretResolver` is a controlled composition
double for tests: it copies material on registration and on resolution so callers
cannot mutate stored state, and its errors name only the opaque reference.

Secret contents never enter product state. Concretely:

```text
acquisition_manifests        -> no credential column
jobs.payload                 -> carries only schema_version and manifest_id
acquisition_provider_tasks   -> opaque provider identity and task reference only
CredentialRef                -> an opaque reference, safe to persist and log
```

The session registry owns no secret material at all: it stores a downloader port
per provider/connection identity, and any session state lives inside that port. No
component in this path logs resolved secret values, and the session package is
covered by the architecture guard so it cannot reach persistence or integrations.

## Concrete 115 downloader adapter

`internal/providers/115` is the first concrete provider. It implements
`contracts.DownloaderProvider` on the pinned
`github.com/SheltonZhu/115driver v1.3.5` library, and freezes the D-025 mapping:

```text
contracts.ProviderID  = "115"

Source.Value          -> the exact URI passed to 115 offline download
Target.Scope          -> the 115 destination directory ID (wp_path_id)
Target.Path           -> Panta's expected observation path only
TaskReference.Value   -> the 115 offline task info_hash
```

`Target.Path` is never converted into a provider directory ID, and the
destination is never derived from an OpenList mount or an IndexCore root. The
source value is passed byte-for-byte: the adapter performs no magnet truncation or
normalization, which belongs to a future Source Resolver.

Status mapping is frozen and explicit:

```text
115 status 0   -> PENDING
115 status 1   -> RUNNING
115 status 2   -> SUCCEEDED
115 status -1  -> FAILED
anything else  -> explicit adapter error
```

There is no 115 code for a canceled offline task: `CancelDownload` removes the
task without deleting provider files, so a removed task surfaces as an explicit
not-found error rather than as a canceled state. Provider `SUCCEEDED` is never
Panta `READY`.

Safety properties:

```text
one StartDownload        -> exactly one 115 offline URI task
zero/multiple/blank hash -> fail closed, never a task reference
pagination               -> bounded and fail-closed on malformed or
                            non-progressing page metadata
cancel                   -> DeleteOfflineTasks([info_hash], false);
                            downloaded provider files are never deleted
retry                    -> none in the adapter; the Job Engine owns policy
cookie material          -> never logged, never returned, never embedded in
                            an error, and never retained after client import
```

The adapter reaches 115 only through an adapter-private `Backend` port, so unit
tests need neither real credentials nor network access. Two construction paths
exist: `p115.New` wraps an injected backend, and `p115.NewAdapterFromCookie`
builds the real library client from resolved secret material. Only the latter
touches the upstream library, and a note in the code records that the first real
provider call lazily resolves the authenticated 115 user.

The package is covered by the architecture guard: it may import only
`internal/providers/contracts`, the pinned driver package, and the standard
library.

## Atomic provider-stage outcome handoff

Gate 3.7 commits the outcome of one fenced `RUNNING` `ACQUISITION` Job so the
Manifest milestone and the Job state can never diverge. D-026 freezes the mapping:

```text
PROVIDER_IN_PROGRESS   Manifest ACTIVE   (unchanged)   Job RUNNING -> RETRY_WAIT
PROVIDER_SUCCEEDED     Manifest ACTIVE -> AWAITING_VISIBILITY
                                                      Job RUNNING -> RETRY_WAIT
PROVIDER_FAILED        Manifest ACTIVE -> FAILED       Job RUNNING -> FAILED
PROVIDER_CANCELED      Manifest ACTIVE -> CANCELED     Job RUNNING -> CANCELED
PROVIDER_RECOVERY      Manifest ACTIVE -> RECOVERY_REQUIRED
                                                      Job RUNNING -> RECOVERY_REQUIRED
```

`PROVIDER_SUCCEEDED` is explicitly **not** Job `SUCCEEDED` and not Manifest
`READY`. The ACQUISITION Job represents the whole acquisition workflow rather than
one provider RPC, so provider success queues the *same* Job for the later
visibility stage. No second visibility Job is created.

`internal/acquisition` owns `ProviderOutcomeService` and the `ProviderOutcomeStore`
port. `internal/store/postgres` implements the commit in one transaction:
lock Manifest, verify exact identity plus `ACTIVE`, lock Job, verify the frozen
Gate 3.2 linkage plus the `RUNNING` lease fence (owner, claim generation, and
unexpired lease authorized by database time), then mutate both rows and commit. A
failure between the two mutations rolls both back.

Job mutation semantics:

```text
RETRY_WAIT         lease cleared, next_attempt_at = RetryAt, finished_at NULL,
                   claim generation and failure budget preserved. This
                   intentionally does not consult max_attempts: the Job is being
                   queued for the next acquisition stage, not retried for the same
                   work.
FAILED             lease cleared, next_attempt_at NULL, last_error required,
                   finished_at set, claim generation and failure budget preserved
CANCELED           lease cleared, next_attempt_at NULL, finished_at set, and no
                   provider files or tasks are touched by this persistence step
RECOVERY_REQUIRED  lease cleared, next_attempt_at NULL, last_error required,
                   finished_at stays NULL because recovery is non-terminal, and
                   the Job is not claimable by ordinary ClaimNext
```

When the outcome keeps the Manifest at `ACTIVE`, its `updated_at` is deliberately
preserved: nothing about the Manifest changed, so rewriting the timestamp would be
a spurious durable write.

Replay is idempotent, and replay detection keys on the **durable Manifest + Job
pairing** rather than on the Manifest state alone. `PROVIDER_IN_PROGRESS`
legitimately leaves the Manifest `ACTIVE`, so keying on "Manifest is not ACTIVE"
would misclassify its own committed outcome as a fresh handoff and fail its replay.
If the Manifest already holds the exact milestone this outcome produces and the
linked Job already holds its exact D-026 pairing, the call returns the durable
records with `Changed=false` and rewrites nothing. A different proposed outcome
after a committed one fails closed with a typed error, and two callers with the
same fenced claim generation produce at most one committed change.

## Claim generation versus failure budget

One ACQUISITION Job now carries the whole acquisition workflow, so the same Job is
claimed repeatedly: for provider polling, then visibility, then canonical stages.
A single counter could not serve both the stale-worker fence and the failure retry
budget, because stage transitions would consume the budget and could strand a Job in
`RETRY_WAIT` forever — or block the visibility stage immediately after a successful
download.

Migration `0010_job_claim_generation.sql` therefore splits the two:

```text
claim_attempts  monotonically increasing claim generation
                every successful claim increments it
                never bounded
                the fencing token: ExpectedClaim must equal it

attempt_count   failure/retry budget (jobs.Job.FailureCount)
                only RetryAt increments it
                bounded by max_attempts
                governs terminal FAILED
```

Consequences:

```text
ClaimNext       claims QUEUED, or RETRY_WAIT once next_attempt_at is due, and
                never refuses a claim because the failure budget is spent
RetryAt         the only operation that consumes the budget; it still terminates
                the Job at max_attempts
handoff         never changes the claim generation or the failure budget
stale worker    a superseded generation cannot mutate state, even when the owner
                string still matches
```

The existing `attempt_count` column name is retained for compatibility; the Go field
is `jobs.Job.FailureCount` so the distinction is explicit at every call site.

### Upgrading an existing database

Before version 10, `attempt_count` **was** the claim generation, because every claim
incremented it. The migration therefore moves the legacy value rather than dropping
it:

```sql
UPDATE jobs
SET claim_attempts = attempt_count,
    attempt_count = 0;
```

Adding the column with a default of 0 and leaving `attempt_count` in place would have
been wrong in two ways:

```text
legacy generation N rewound to 0   -> an already superseded worker passes the fence
N historical claims read as N failures -> the next real failure terminates the Job
                                          early once N reaches max_attempts
```

Two upgrade tests pin this against a real schema applied at version 9 and then
upgraded: `TestPostgresJobClaimGenerationUpgradeMigratesLegacyAttemptCount` (a
`RETRY_WAIT` legacy row keeps generation `N`, reports `FailureCount = 0`, advances to
`N+1` on the next claim, and generation `N` is then fenced) and
`TestPostgresJobClaimGenerationUpgradeKeepsLiveLeaseFence` (a `RUNNING` legacy row
keeps working at its migrated generation and its first genuine failure counts as
`1`, not `N+1`).

One consequence is deliberate: a Job that had already recorded failures under the
pre-10 counter starts with a fresh failure budget, because that counter never
measured failures. Only generation is preserved, since only generation has a
fencing meaning that must stay monotonic.

## Deferred capabilities

Source Resolver/provider syntax normalization, the Job worker loop, trusted
IndexCore Mutation Hint / observation handoff, the AWAITING_CANONICAL transition,
canonical READY confirmation, auth/quota, and API/UI are separately authorized
later work. Direct Panta -> OpenList acquisition verification is not a planned
stage; D-029 keeps OpenList observation owned by IndexCore. A real secret backend, the 115 ShareProvider, and
115-specific retry policy are also deferred.


## Acquisition observation ownership correction

D-029 is authoritative for post-provider observation:

```text
provider succeeds
    ↓
Manifest AWAITING_VISIBILITY
    ↓
Panta -> IndexCore trusted Mutation Hint
    ↓
IndexCore-owned OpenList scoped verification
    ↓
IndexCore Canonical + Journal
    ↓
Panta confirmation / Copy projection
```

The Manifest's `target_path` is used as the IndexCore mutation-hint
`scope_key` together with the binding's `indexcore_root_id`.

Do not derive this Hint from:
- `provider_scope`;
- `openlist_mount_path`;
- provider task reference.

A direct Panta OpenList visibility client is not part of the acquisition flow.



## Durable result locator before canonical confirmation

D-032 is authoritative for the provider-neutral result locator before exact IndexCore confirmation.

`target_path` remains the destination directory scope. It is not the acquired
resource's complete canonical path. `expected_name` remains optional acquisition
intent.

Before provider success may progress automatically to `AWAITING_VISIBILITY`,
Panta resolves one top-level `result_name`:

```text
valid provider-observed TaskStatus.ResultName
        >
Manifest.ExpectedName fallback
        ↓
Manifest.result_name
```

The future canonical candidate is then deterministic:

```text
Join(Manifest.target_path, Manifest.result_name)
```

Panta must never replace this locator with directory guessing, a sole/newest file,
provider FileId/DirId, provider task reference, or an arbitrary Journal event.

If no valid result name exists, provider success cannot progress automatically;
the acquisition requires explicit recovery. A persisted `result_name` is
immutable and replay-safe, but it is only a locator: Q5/Journal/Copy confirmation
is still required before READY.
