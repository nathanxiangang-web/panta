# Gate 3.11 — one-claim acquisition stage dispatch

`internal/acquisition.StageDispatcher` accepts an already-claimed ACQUISITION Job. It reads the Job's frozen payload link and the durable Manifest, verifies the reverse link and current owner/claim generation, then invokes one stage selected by `Manifest.State`.

| Durable Manifest state | One invocation | Durable handoff owner |
| --- | --- | --- |
| `ACTIVE` | Provider execution, then provider outcome | `ExecutionStepService` / `ProviderOutcomeService` |
| `AWAITING_VISIBILITY` | One trusted IndexCore Hint | `RefreshStep` |
| `AWAITING_CANONICAL` | One Q5/projector/Copy confirmation attempt | `CanonicalConfirmation` |
| Terminal pair | Read-only committed result | No external stage |

The dispatcher owns routing and the closed provider outcome translation only. Stage services and their PostgreSQL repositories own writes and database-time lease fences. `internal/app.NewAcquisitionStageDispatcher` is the production composition point; it accepts the existing services through narrow ports. `internal/providers/115`, OpenList, IndexCore internals, and concrete PostgreSQL types do not enter the acquisition domain.

One invocation returns after its first durable handoff, including `RETRY_WAIT`. The caller must obtain a later distinct Job claim to enter the next stage. `ClaimAttempts` is the fencing generation; normal provider polling, Hint acceptance, and canonical pending do not consume `FailureCount`. An uncertain provider side effect commits the existing `RECOVERY_REQUIRED` outcome and cannot trigger another provider start.

To extend routing, first add an accepted durable Manifest milestone and a bounded stage service with its own persistence fence, then add one explicit dispatcher branch and a multi-claim regression. Do not add a second Job, a second Copy writer, a direct OpenList visibility check, or a worker loop to this module. Runtime scheduling is a separate Gate.
