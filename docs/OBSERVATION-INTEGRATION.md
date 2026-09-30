# Observation integration runbook

Gate 1 keeps one observation path:

```text
OpenList-compatible source
        -> IndexCore OpenList collector / Canonical Inventory / Journal
        -> Panta IndexCore Q4/Q5/Q8 client
        -> Panta projector and Catalog Copy
```

IndexCore owns OpenList tree collection. Panta owns the product-side
`StorageConnection`, `StorageBinding`, projection cursor, and Catalog `Copy`.
Panta does not read or write the IndexCore database and does not implement a
second OpenList tree walker.

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
deletion.

For acquisition, D-029 keeps this ownership boundary unchanged: Panta does not
add a direct OpenList visibility-verification lane. After provider completion,
Panta sends a trusted Mutation Hint to IndexCore using the binding's
`indexcore_root_id` and the Manifest's `target_path`; IndexCore then performs
the OpenList-backed scoped verification and produces Canonical/Journal evidence.

Those Gate 3 control-plane handoff capabilities are not part of Gate 1.

## Trusted Mutation Hint transport contract

This section records the exact wire contract the Gate 3.8 handoff implements. It
does not change ownership: IndexCore still performs the OpenList-backed scoped
verification and produces Canonical/Journal evidence.

### Hint request

```text
POST /internal/v1/mutation-hints
Content-Type: application/json
Authorization: Bearer <token>
{"root_id":"root-115-a","scope_key":"/downloads/movies","reason":"POSSIBLE_CHANGE"}
```

The body is closed. IndexCore decodes it with `DisallowUnknownFields`, so Panta sends
exactly those three fields. `scope_key` is validated against IndexCore's own rule
before sending — root-absolute, no trailing slash, no backslash, and no empty, `.`
or `..` component — so a malformed scope fails locally instead of as a remote `400`.

### Accepted receipt

```text
202 {"status":"accepted","root_id":…,"scope_key":…,"work_state":…,"signal_seq":…}
```

Panta requires the receipt root and scope to match the submitted Hint exactly.
`signal_seq` is observation-work metadata: it is never a canonical generation and
never a resource identity.

### Typed failures

Distinct, so a caller cannot confuse a retryable condition with an authorization or
protocol error:

```text
400 invalid request      401 unauthorized        413 request too large
415 unsupported type     429 backpressure        503 ingest unavailable
unexpected status        transport failure       malformed / trailing / oversized
```

A `429` preserves IndexCore's `Retry-After` for the caller and never sleeps on it
internally. A non-JSON body still classifies by status rather than becoming a
malformed-response error. The bearer token is never logged, returned, or embedded in
an error.

### Handoff and at-least-once window

Once IndexCore durably accepts the Hint, Panta atomically advances the same Manifest
and Job:

```text
Manifest AWAITING_VISIBILITY -> AWAITING_CANONICAL
Job      RUNNING             -> RETRY_WAIT
lease cleared, next_attempt_at = RetryAt, finished_at NULL
claim generation and failure budget unchanged
```

No second Job is created. Hint acceptance never marks the Job `SUCCEEDED` and never
marks the Manifest `READY`.

Between the `202` and Panta's commit there is an unavoidable window, and no
provider-style side-effect fence is created for it, deliberately: an IndexCore
Mutation Hint is at-least-once and coalescing and is not authoritative, so replaying
the same root/scope cannot duplicate a provider download, create Canonical truth, or
gain destructive authority.

```text
while Panta is still AWAITING_VISIBILITY     -> a retry may resend the same Hint
once Panta is AWAITING_CANONICAL + RETRY_WAIT -> exact replay Changed=false, no second Hint
```

### Deployment precondition

Automatic scoped verification requires IndexCore to run with its accepted Hint
transport and incremental runtime enabled. Panta does not own or reimplement that
worker. If the Hint endpoint accepts work while the IndexCore runtime is disabled,
Panta still must not bypass IndexCore by calling OpenList: canonical confirmation
remains pending until the IndexCore execution path runs.

The Hint listener is literal-loopback-only on IndexCore's side, and Panta enforces
the same rule on its side rather than trusting configuration:

```text
accepted   http://127.0.0.1:<port>   http://[::1]:<port>   https://127.0.0.1:<port>
rejected   localhost   127.0.0.2   0.0.0.0   ::ffff:127.0.0.1
           any non-loopback address   any hostname   any URL carrying userinfo
```

This mirrors IndexCore's own P9 gate exactly, because every Hint carries
`Authorization: Bearer <Hint token>`: accepting an arbitrary remote address would
let a single misconfiguration send the trusted token off-host. For MVP deployment
Panta's backend process must be colocated within the same trusted loopback network
namespace as the Hint listener.

The client also never follows an HTTP redirect. The Hint endpoint is a trusted
internal write ingress, so Panta must reach exactly the configured endpoint; a `3xx`
fails closed as an unexpected status instead of delivering the request and its token
to a second address.

Exposing the Hint transport publicly, or moving it across hosts, Docker bridges, or
Kubernetes Services, is out of scope: it needs its own security and deployment
decision and must not be opened implicitly by configuration.
