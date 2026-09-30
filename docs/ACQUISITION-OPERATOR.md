# Gate 3.16 protected acquisition operator bootstrap

This document describes an operator entrypoint, not permission to run a real
115 account or deploy Panta. The ordinary `panta` command remains the
side-effect-free skeleton. Only the explicit `start-acquisition` action can run
the accepted worker; `preflight-acquisition` constructs and closes its graph
without calling `Run`.

## Module boundaries

```text
cmd/panta
  -> platform/operator (action and startup timeout)
  -> platform/bootstrap (protected file config + SecretResolver)
  -> runtime.NewRuntime (one v12 DB / session / IndexCore graph)
  -> accepted app Worker and stage services
```

The bootstrap module does not import Job, Manifest, provider adapter or SQL.
The runtime module continues to own exact session validation, while the
projector remains the sole Copy writer. Later secret-manager backends can
replace the platform-edge resolver without changing acquisition or provider
contracts; independently test their permission and rotation behavior.

## Protected input contract

On Linux, place the bootstrap JSON and secret files in directories owned by
the Panta process user with mode `0700`. Files must be regular, same-owner and
mode `0600` (or stricter while owner-readable). Absolute paths are required;
symlinks, traversal, oversized or group/world-accessible files fail closed.
Non-Linux file-mount startup currently fails closed.

Example structure (placeholders are **not** credentials):

```text
/run/panta/private/          # owner-only directory
  bootstrap.json             # owner-readable config, no raw secrets
/run/panta/secrets/          # owner-only secret mount
  hint.token                 # trusted Hint token, provided outside Git
  connection.cookie          # 115 cookie, provided outside Git
```

`bootstrap.json` contains only addresses, opaque references and file names:

```json
{
  "secret_dir": "/run/panta/secrets",
  "hint_base_url": "http://127.0.0.1:9100",
  "hint_token_file": "hint.token",
  "sessions": [
    {
      "provider_id": "115",
      "connection_id": "<existing-storage-connection-uuid>",
      "credential_ref": "<opaque-ref-already-in-panta-db>",
      "secret_file": "connection.cookie"
    }
  ]
}
```

The trusted Hint endpoint is a separate loopback listener from the IndexCore
Q5/Q8 read URL. The Hint token and 115 cookie are **file contents**, never
JSON fields, CLI arguments, Git files or log values. The process environment
supplies `PANTA_DATABASE_URL` (Panta product DB only),
`PANTA_INDEXCORE_BASE_URL` (Q5/Q8),
`PANTA_ACQUISITION_BOOTSTRAP_FILE` (absolute path to protected JSON), and
`PANTA_ACQUISITION_WORKER_ENABLED=true`. Existing bounded worker interval,
timeout, lease and retry settings remain available through `config.Load`.
Keep any environment file carrying a DB password protected; do not paste its
value into a command line.

## Operator actions

Before either action, verify that the Panta product database was migrated to
schema v12 by the existing migration workflow, that the listed ACTIVE
StorageConnections match the configured ProviderID/ConnectionID/CredentialRef,
and that the Hint listener is reachable from the same network namespace.
Preflight validates local configuration, protected secret availability,
cookie syntax, schema and exact active-session coverage. It does **not**
authenticate with a real 115 account or guarantee external service health.

```text
panta preflight-acquisition
panta start-acquisition
```

The first action must leave Jobs, Manifests, provider-task fences, projector
cursors and Copy rows unchanged. The second action is an explicit opt-in to
provider mutations if queued acquisition work exists. Do not run it against a
real account under this Gate; controlled tests use synthetic credentials and
local HTTP fakes only. Additional CLI arguments, including cookie or token
flags, are rejected.

Startup has a 15-second bound. Once started, SIGINT or SIGTERM cancels the
single worker, waits for its current tick to return, then closes the owned
Panta pool. Normal cancellation is not an error. Recovery debt is a nonzero
fatal condition and stops new claims; inspect linked Manifest/Job and any
`START_RESERVED` provider task before an operator repair. Never clear the
reservation to force a second StartDownload.

To disable or roll back, send SIGTERM, wait for exit, then remove the explicit
start action or set `PANTA_ACQUISITION_WORKER_ENABLED=false`. This changes no
database schema or historical acquisition state. Replace/rotate mounted
secret files securely and restart the process: already-constructed 115
sessions intentionally keep their old credential until a validated restart.

Failure categories are intentionally redacted: invalid action/config,
unsafe or unavailable secret mount, invalid runtime binding/client, incompatible
Panta schema, and fatal acquisition recovery debt. Paths, cookie bytes, Hint
tokens and provider source URLs must not appear in operator logs.
