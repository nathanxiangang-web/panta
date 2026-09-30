# Acquisition worker lifecycle (Gate 3.14)

The worker is a lifecycle adapter in `internal/app`, not a second Job Engine.
It calls the accepted `AcquisitionRunOnce.RunOnce` synchronously. RunOnce owns
linked lease recovery, ACQUISITION-only claiming and one stage dispatch; the
stage services own all durable Manifest/Job transitions and provider effects.

## Module boundary

```text
process config (disabled by default)
    -> injected AcquisitionWorker
    -> one serial RunOnce at a time
    -> D-035 recovery + scoped claim + D-034 stage dispatcher
```

- `internal/platform/config` validates cadence, tick deadline, lease safety
  margin, retry timing and batch limits. It stores no credentials.
- `internal/app.AcquisitionWorker` generates one random process-instance owner,
  starts the first tick immediately, waits a full interruptible interval after
  each completed tick, and propagates cancellation to the current tick.
- Worker events contain only closed categories, counts, Job identity and stage;
  they do not contain provider URLs, references, cookies or credentials.
- Recovery debt stops the loop. Other integration errors are reported and paced
  by the same interval; the worker never performs Job-only failure repair.

## Opt-in configuration

`PANTA_ACQUISITION_WORKER_ENABLED` defaults to false. When true, the settings
below have validated safe defaults and may be overridden:

| Setting suffix | Default |
| --- | --- |
| `OWNER_PREFIX` | `panta-acquisition` |
| `INTERVAL` | `5s` |
| `TICK_TIMEOUT` | `30s` |
| `LEASE_DURATION` | `2m` |
| `RETRY_DELAY` | `10s` |
| `RECOVERY_LIMIT` | `10` |
| `PROJECTOR_PAGE_LIMIT` | `100` |

Prefix each suffix with `PANTA_ACQUISITION_WORKER_`. A lease must exceed the
tick timeout by at least five seconds. The retry delay cannot be shorter than
the interval. The owner adds a random per-instance suffix to the prefix.

Enabling the current `cmd/panta` without injected runtime dependencies fails
at application construction. This Gate does **not** wire real PostgreSQL,
IndexCore and authenticated 115 sessions into the process or authorize a
production deployment.

## Extension constraint

Future runtime composition may inject one fully built RunOnce through
`NewWithAcquisitionWorker`. Do not move claiming/recovery into the worker,
spawn a goroutine per tick, inspect OpenList directly, or infer READY from a
worker event. Keep the module independently testable with a fake RunOnce and
waiter; use PostgreSQL integration tests for durable transitions.
