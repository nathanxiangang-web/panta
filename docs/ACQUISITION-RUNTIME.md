# Gate 3.15 acquisition runtime (D-038)

This gate adds `internal/runtime.NewRuntime`, the single explicit composition
boundary for the accepted acquisition worker. It does not deploy a service or
authorize a real 115 download. The stock `cmd/panta` remains disabled by default;
setting its worker-enabled flag without an external secret provider still fails
closed. An embedding process must inject a `contracts.SecretResolver` and exact
`RuntimeSession` registrations before calling `Run`.

## Modules and dependencies

```text
external secret boundary -> runtime.NewRuntime
                         -> Panta PostgreSQL v12 stores (product DB only)
                         -> 115 connection-scoped session registry
                         -> separate IndexCore Q5/Q8 read and trusted Hint clients
                         -> accepted Execution/Outcome/Refresh/Canonical services
                         -> accepted Journal Projector -> Copy
                         -> RunOnce -> serial Worker
```

`internal/runtime` may import both application services and adapters. Domain
modules remain independent of PostgreSQL, provider details, and HTTP clients.
The Projector alone creates Copy; Hint acceptance never establishes READY.

## Controlled startup and shutdown

1. Provision a **separate Panta product database** at schema v12 through the
   existing migration command before starting a worker. Runtime startup only
   reads migration status and refuses pending, modified, gapped, or unknown
   history. It never migrates a live database.
2. Obtain the IndexCore Q5/Q8 read URL and its distinct loopback-only trusted
   Hint URL/token from the operator's trusted channel. Do not put tokens in Git,
   Panta tables, examples, or diagnostic logs. The Hint listener must be
   `127.0.0.1` or `::1` from the worker's network namespace.
3. Inject a production `SecretResolver` whose `ResolveSecret` returns a fresh
   byte slice for each opaque `CredentialRef`. Do not use the test-only
   `StaticSecretResolver`. Register every intended 115 session by exact
   ProviderID, ConnectionID, and CredentialRef; startup checks each registration
   against the Panta `storage_connections` row before any Job can be claimed.
4. Call `runtime.NewRuntime(ctx, cfg, deps)` before accepting work. Treat any
   error as a startup failure; do not substitute mock clients or skip a failed
   session. The constructor owns the Panta pool and performs no provider or
   IndexCore network probe. Keep the read and Hint endpoints in separate
   configuration fields.
5. Run the returned runtime with a cancellation-aware context. On shutdown,
   cancel `Run`, wait for its return, then call `Close` (idempotent). Injected
   HTTP transports and the external secret backend remain caller-owned.

The production 115 adapter bounds each upstream HTTP request to five seconds
and status lookup to three pages. A deployment using the real adapter needs at
least a 30-second tick timeout and the existing lease safety margin. Exceeding
the page bound fails closed; it does not create a second task.

## Test and extension policy

`TestPostgresAcquisitionRuntime*` uses a disposable PostgreSQL 16 `_test`
database, local HTTP fakes for Q5/Q8/Hint, and a synthetic authenticated
downloader. It proves READY requires Q5 plus projected Copy, verifies Hint
rejection and Q5 ambiguity cannot produce READY, and checks canceled
`START_RESERVED` restart recovery without a second StartDownload. The CI job
never contacts a real 115 account or live IndexCore.

Future secret backends and provider adapters belong behind the injected
`SecretResolver` and `DownloaderFactory` ports. They must preserve exact
connection/credential selection, bounded calls, no secret logging, and
independent contract tests. Production provisioning, deployment, and real
account acceptance require a later gate.
