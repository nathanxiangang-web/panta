# Gate 3.17 source intake and acquisition submission

This is an enqueue-only staging/operator boundary for Issue #58 / D-040. It is
not permission to run a real 115 download or deploy a worker. Schema remains
v12. No provider, IndexCore, OpenList, DNS, HTTP, search or Agent operation is
performed by source resolution or submission.

## Modules and dependency direction

```text
cmd/panta submit-acquisition
  -> platform/submission (protected request file, DB composition)
  -> acquisition.SubmissionService
       -> ResolveSource (pure classification)
       -> existing Service.CreateManifest (topology/catalog validation)
       -> existing ActivationService (atomic Job + ACTIVE transition)
  -> Panta PostgreSQL repositories only
```

`acquisition` owns the provider-neutral ports and does not import SQL, the
concrete 115 adapter or an external integration. `platform/submission` is the
replaceable CLI/DB adapter. A future API may call the same SubmissionService,
but must implement its own authorization and keep the exact source invariant;
this Gate does not add an API or an alternate worker.

Supported schemes are `magnet`, `http`, `https`, and `ed2k`. The scheme/type is
lower-cased; the original accepted `source_ref` is not trimmed, truncated,
percent-reencoded, query-filtered or reconstructed. HTTP(S) requires an
absolute host and rejects userinfo. Magnet and ed2k bodies remain opaque.
ExpectedName is optional explicit intent, never inferred from the source.

## Protected request contract

Place the JSON file under an absolute owner-only (`0700`) Linux directory.
The file must be owner-readable, regular and mode `0600` or stricter; symlinks,
traversal, excessive size and group/world access fail closed. The source is
file content, **not a process argument, environment value, log field or Job
payload**. The command needs `PANTA_DATABASE_URL` for the Panta product DB and
`PANTA_ACQUISITION_SUBMISSION_FILE` pointing to this file. It does not require
`PANTA_ACQUISITION_WORKER_ENABLED=true`, provider credentials, a Hint token or
an IndexCore URL.

Example shape, with deliberately nonfunctional placeholders:

```json
{
  "manifest_id": "<operator-stable-uuid>",
  "job_id": "<operator-stable-uuid>",
  "max_attempts": 3,
  "source_ref": "magnet:?xt=urn:btih:<placeholder>&dn=example",
  "target_storage_binding_id": "<existing-active-binding-uuid>",
  "target_path": "/downloads",
  "expected_name": "example"
}
```

Optional `user_id`, `asset_id`, `release_id`, and `variant_id` retain the
accepted Manifest lineage rules. The binding must already be acquisition-
capable and point to an active connection. The command does not create or
guess topology, catalog identities, target paths or provider save directories.

```text
panta submit-acquisition
```

It checks the Panta schema-v12 status but does not migrate. A successful first
submit prints only `CREATED` and the stable Manifest/Job IDs. Exact same-ID
replay prints `REPLAY`; a conflicting source, target, lineage or Job ID fails
closed. If activation fails after Manifest creation, the command reports
`PENDING_ACTIVATION` with IDs and exits nonzero: inspect the PENDING Manifest,
repair the underlying cause, then retry the **same file and IDs**. Never delete
the PENDING intent to hide a partial submission. The Job activation store
alone links one ACQUISITION Job and its idempotency key atomically.

The command never starts `start-acquisition`. A later, separately authorized
operator action is required to run the Gate 3.16 worker. To stop submitting,
stop invoking this one-shot command; no scheduler or schema change was added.
The test suite covers deterministic replay, rollback/PENDING recovery, exact
Manifest and ExecutionInput bytes, and a real CLI invocation against an
isolated PostgreSQL test database without provider or IndexCore calls.
