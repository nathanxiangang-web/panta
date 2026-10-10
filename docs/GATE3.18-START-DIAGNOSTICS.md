# Gate 3.18 first-start diagnostics (Issue #62)

## Current evidence and limits

The original acquisition (`659c0920-74aa-41ee-9820-a8a6718cb5ba`, Job
`2e07f3a9-cb71-4b26-abf7-d4ba0743595f`) remains `RECOVERY_REQUIRED`, with one
claim, failure count zero, `START_RESERVED`, and no provider reference. Its
upstream error was not retained: **original first_error_category = UNKNOWN**.
Do not retroactively label that original call a timeout or a parser failure.

Authenticated read-only checks succeeded with the same protected cookie. The
dedicated provider directory was readable, and each controlled HTTPS input was
reachable from the staging host. Task-list inspection read all 10 reported
pages, returning 283 entries while the API reported total 1500. No exact source
or destination match was found; the dedicated directory still contained only
the baseline marker. Assessment: **NOT_FOUND_WITH_LIMITATIONS**, not proof that
the first request could never have created a remote task.

Separate acquisitions used different source objects and IDs. Every failed pair
and reservation is retained; none was reset, requeued, or started a second time.

| Independent observation | Category | Elapsed | HTTP status | Cause type |
| --- | --- | ---: | ---: | --- |
| Initial diagnostic run | TIMEOUT | 5010 ms | not recorded | not recorded |
| Expanded diagnostic run | RESPONSE_DECODE_FAILURE | 484 ms | 200 | json.SyntaxError |
| After SDK User-Agent preservation | RESPONSE_DECODE_FAILURE | 397 ms | 200 | json.SyntaxError |

The final live result is **STOP**, with no known task reference, provider result,
canonical Copy, or READY. The malformed JSON may be at the HTTP response or at
the pinned driver's decrypted-response parser; the current evidence does not
distinguish those locations or establish account rejection, source rejection,
or remote-task absence. Gate 3.18 / Issue #60 and Issue #62 remain open.

## Module responsibilities and diagnostic path

- `providers/115`: one bounded submission, preserved pinned SDK default
  User-Agent, closed diagnostic category, duration, final HTTP status, and
  compiled Go cause type. No raw response body/error, source URI, cookie, or
  provider task reference is retained by the diagnostic.
- `acquisition.StageDispatcher`: commit the accepted linked recovery outcome
  first; carry non-durable diagnostic metadata in the returned stage result.
- `app.AcquisitionWorker`: forward that metadata through its event seam even
  when linked recovery was successfully committed and Dispatch returned nil.
  Category and cause type are allowlisted before emission.
- `platform/operator`: emit the safe metadata for explicit worker starts.
  Preflight/default startup remain silent and side-effect-free.

`errors.Is` remains available for the existing adapter sentinels and known
upstream/context causes. HTTP observation wraps the existing transport without
altering requests or inspecting response bodies. It records status zero when no
HTTP response is available.

The pinned SDK's `SetHttpClient` replaces its resty client and headers. Panta now
installs the HTTP client before applying `driver.UA()`. A fake wire transport
asserts the actual SDK default header. The live parser failure persists after
this correction, so it is not claimed as the resolution of the staging blocker.

No retry, migration, alternate SDK/downloader, source rewriting, lease change,
OpenList verifier, Copy writer, or IndexCore trust change is introduced. A future
category must be added to the emission allowlist with a redaction regression
test. Further live tests should follow a specifically evidenced correction;
existing uncertain reservations remain historical diagnostic records.
