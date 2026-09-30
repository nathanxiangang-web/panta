# Observation integration runbook

Gate 1 keeps one observation path:

```text
Storage -> OpenList -> IndexCore Collector -> Canonical + Journal -> Panta
```

IndexCore owns OpenList tree collection and the OpenList-facing observation
pipeline. Panta owns the product-side `StorageConnection`, `StorageBinding`,
projection cursor, and Catalog `Copy`. Panta does not read or write the IndexCore
database, does not implement a second OpenList tree walker, and — per D-029 — does
not call OpenList directly during acquisition.

## Mapping

The three values below describe one physical observation boundary and must be
configured together:

```text
StorageBinding.openlist_mount_path = /library
IndexCore root adapter path         = /library
StorageBinding.indexcore_root_id    = IndexCore root UUID
```

The OpenList path is provider-side configuration owned by IndexCore. The root
UUID is the stable physical namespace that Panta persists. A known physical
path is resolved directly with Panta's IndexCore Q5 client; Catalog search is
not part of this flow.

Store only an environment-variable name such as
`PANTA_GATE1_OPENLIST_TOKEN` in the IndexCore adapter configuration. Inject the
token into the IndexCore process at runtime. Do not persist the token value or
print it in logs.

## Acquisition observation handoff (Gate 3.8)

After a provider reports download success, Panta does not prove visibility itself.
It hands the affected directory scope to the IndexCore-owned observation pipeline
with one trusted Mutation Hint, and then advances its own state only once IndexCore
has durably accepted that work.

```text
Manifest AWAITING_VISIBILITY + fenced RUNNING ACQUISITION Job
        ↓
root_id   = StorageBinding.indexcore_root_id
scope_key = Manifest.target_path
reason    = POSSIBLE_CHANGE
        ↓
POST /internal/v1/mutation-hints   (IndexCore, loopback-only, Bearer)
        ↓
202 Accepted
        ↓
atomic Panta handoff
Manifest AWAITING_CANONICAL + Job RETRY_WAIT
```

`AWAITING_VISIBILITY` therefore means: the provider stage is complete and Panta is
waiting to hand the affected scope to the IndexCore-owned observation pipeline. It
does not authorize a Panta-to-OpenList stat.

### Four coordinates stay separate

```text
provider_scope          provider-side mutation target
openlist_mount_path     IndexCore/OpenList observation configuration
indexcore_root_id       canonical IndexCore root identity
manifest.target_path    directory scope inside the selected binding/root
```

None of these may be derived from another. In particular the Hint never encodes an
OpenList path, and the OpenList mount and `provider_scope` never influence it.

### Trusted Hint contract

Panta's `indexcore.HintClient` targets IndexCore's accepted P9 transport:

```text
POST /internal/v1/mutation-hints
Content-Type: application/json
Authorization: Bearer <token>
{"root_id":"root-115-a","scope_key":"/downloads/movies","reason":"POSSIBLE_CHANGE"}
```

The body is closed: IndexCore decodes it with `DisallowUnknownFields`, so Panta sends
exactly those three fields. Panta validates `scope_key` against IndexCore's own rule
before sending — root-absolute, no trailing slash, no backslash, and no empty, `.`
or `..` component — so a malformed scope fails locally instead of as a remote `400`.

The accepted receipt is `202` with
`{"status":"accepted","root_id":…,"scope_key":…,"work_state":…,"signal_seq":…}`. Panta
requires the receipt root and scope to match the submitted Hint exactly. `signal_seq`
is observation-work metadata; it is never a canonical generation and never a resource
identity.

Typed failures are distinct so a caller cannot confuse a retryable condition with an
authorization or protocol error: `400` invalid request, `401` unauthorized, `413`
request too large, `415` unsupported media type, `429` backpressure (with the remote
`Retry-After` preserved and never slept on internally), `503` ingest unavailable,
plus unexpected status, transport failure, and malformed/trailing/oversized response.
The bearer token is never logged, returned, or embedded in an error.

### Hint acceptance is not canonical truth

`202 Accepted` means only that IndexCore durably ingested "this scope needs
re-verification". It is not proof the file exists, not Canonical truth, not a
resource identity, and never a reason to mark a Manifest `READY`. The Manifest is not
READY and the Job is not `SUCCEEDED` by this handoff.

### At-least-once window

There is an unavoidable window between IndexCore accepting the Hint and Panta
committing its own handoff. No provider-style side-effect fence is created for it,
deliberately: an IndexCore Mutation Hint is at-least-once and coalescing, and is not
authoritative. Resending the same root/scope may advance IndexCore signal metadata,
but it cannot duplicate a provider download, create Canonical truth, or gain
destructive authority.

```text
while Panta is still AWAITING_VISIBILITY   -> a retry may resend the same Hint
once Panta is AWAITING_CANONICAL+RETRY_WAIT -> exact replay returns Changed=false
                                               and sends no second Hint
```

### Boundary

Panta's refresh step depends only on a narrow Hint port plus its existing Manifest,
Storage, and Job contracts. `internal/integrations/indexcore` owns the concrete HTTP
client and may not import acquisition, jobs, store, providers, search, agent, auth,
or IndexCore's internal Go packages. There is no Panta-to-OpenList production HTTP
client in this flow.

## Controlled Gate 1 acceptance

The opt-in test at `test/e2e/observation` requires two fresh, separate
PostgreSQL 18 databases and an external IndexCore binary built from:

```text
release: v0.4.0-alpha.1
commit:  6f0eec85c59bd8cbe55011b0d9e512e0cafd6615
```

GitHub Actions builds that exact commit, verifies the checkout SHA, starts a
loopback-only test fixture and IndexCore HTTP server, and runs:

```bash
PANTA_GATE1_E2E=1 \
PANTA_GATE1_INDEXCORE_BINARY=/path/to/indexcore \
PANTA_GATE1_INDEXCORE_DATABASE_URL=postgres://.../indexcore_e2e \
PANTA_GATE1_PANTA_DATABASE_URL=postgres://.../panta_e2e \
go test ./test/e2e/observation -run TestControlledObservation -count=1 -v
```

The harness runs real IndexCore migrations, collection, Canonical Inventory,
Journal and HTTP transport, followed by real Panta migrations 0001-0004, Q4,
Q5, Q8 and `ProjectOnce`. It verifies initial projection, identical-scan
idempotency, and one additive resource.

OpenList collection is additive-safe in this gate: absence is not proof of
deletion. The Gate 3.8 observation handoff above is the accepted extension that
replaces post-download visibility verification. Later gates add canonical
confirmation through IndexCore Query/Journal, `ProjectOnce`, and only then `READY`.

## Deployment precondition

Automatic scoped verification requires IndexCore to run with its accepted Hint
transport and incremental runtime enabled. Panta does not own or reimplement that
worker. If the Hint endpoint accepts work while the IndexCore runtime is disabled,
Panta still must not bypass IndexCore by calling OpenList: canonical confirmation
simply remains pending until the IndexCore execution path runs.

The Hint listener is literal-loopback-only on IndexCore's side. For MVP deployment
Panta's backend process must be colocated within the same trusted loopback network
namespace. Exposing the Hint transport publicly is out of scope and would need its
own security decision.
