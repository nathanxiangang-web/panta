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
canonical Copy, or READY. The malformed JSON may be at the lazy GetUser request,
the outer POST response, or the decrypted-response parser; historical evidence does not
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
altering requests. It transiently inspects at most 64 KiB of response data to
classify shape and test the pinned DownloadResp envelope, clears the buffer,
and retains only closed metadata. It records status zero when no
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

## Architect follow-up 6094823821: operation and parser boundaries

The authenticated read-only GetUser probe on 2026-10-10 used the same pinned
115driver v1.3.5, protected Cookie, 5-second HTTP timeout, SDK default User-Agent,
and staging host network. No proxy environment was set. It succeeded with
HTTP 200, one request, a valid JSON OBJECT (388 bytes), and a nonzero user ID,
in 391 ms. No user ID, user information, Cookie, body or raw error was emitted.
This proves current GetUser functionality, **not** the operation reached by any
historical failed submission. No new live StartDownload was attempted.

| Evidence | Reached | HTTP | Shape | Category / cause | Duration | Proven function / parser |
| --- | --- | --- | --- | --- | --- | --- |
| Live USER_INFO_GET | yes | 200 | OBJECT, valid JSON | OK / none | 391 ms | GetUser / UserInfoResp accepted |
| Historical failing submission | unknown operation | 200 in last two runs | not retained | RESPONSE_DECODE_FAILURE / json.SyntaxError | 484 / 397 ms | unknown; final status alone is insufficient |
| Live OFFLINE_POST after this follow-up | not attempted | n/a | n/a | n/a | n/a | unproven |
| Fake lazy GetUser failure | GET only; no POST | 200 | TEXT | json.SyntaxError | synthetic | GetUser outer JSON |
| Fake lazy GetUser success then POST failure | GET then POST | 200 | TEXT | json.SyntaxError | synthetic | OFFLINE_POST_OUTER_JSON |
| Fake outer schema mismatch | POST | 200 | OBJECT, valid JSON | json.UnmarshalTypeError | synthetic | OFFLINE_POST_OUTER_JSON |
| Fake malformed encrypted data | POST | 200 | OBJECT | base64.CorruptInputError | synthetic | OFFLINE_POST_BASE64_CRYPTO |
| Fake RSA ciphertext (integer 9) | POST | 200 | OBJECT | json.SyntaxError | synthetic | OFFLINE_POST_DECRYPTED_JSON |

Module additions remain diagnostic-only:

- The transport identifies only the exact pinned user-info and offline-submit
  operations, ignoring the SDK timestamp parameter. No URL or header is stored.
- An outer response is classified only after complete EOF within the bound.
  Partial/oversize bodies remain UNKNOWN and still reach the SDK unchanged.
- A valid JSON document is not sufficient to prove successful outer decoding:
  the exact pinned DownloadResp type must also parse. Only then may a later
  typed JSON error be labelled decrypted JSON. Base64 errors identify crypto
  decoding; transport failures before a response identify POST transport.
- Reference-validation failures are OFFLINE_POST_REFERENCE. Unknown causes
  remain UNKNOWN rather than guessing an API/crypto stage.
- Dispatcher, Worker and operator propagate parser_stage and response_shape;
  the Worker allowlists both. PostgreSQL recovery tests retain both metadata
  fields and still verify the reservation prevents a second StartDownload.

No production protocol correction is claimed. Synthetic tests prove diagnostic
classification, not the root cause of the live failure. All five uncertain
reservations remain intact. Issues #60 and #62 remain OPEN, Phase B remains STOP,
and Gate 3.19 must not start. A precisely observed live POST boundary is still
required before the architect can accept a root-cause fix and authorize a fresh
download; historical response bytes cannot be reconstructed from old logs.
