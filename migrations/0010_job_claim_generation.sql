-- Gate 3.7 Round 2: separate claim generation from the failure retry budget.
--
-- A single ACQUISITION Job now runs the whole acquisition workflow, so the same
-- Job is claimed repeatedly for provider polling, then visibility, then canonical
-- stages. Using one counter for both the stale-worker fence and the failure budget
-- meant polling could exhaust max_attempts and leave a Job stuck in RETRY_WAIT
-- forever, or worse, block the visibility stage after a successful download.
--
-- New meaning of the two counters:
--
--   claim_attempts  monotonically increasing claim generation; the fencing token
--                   for stale workers. Never bounded.
--   attempt_count   failure/retry budget consumed only by real failures. Bounded
--                   by max_attempts. The column name is kept for compatibility;
--                   the Go field is jobs.Job.FailureCount.
--
-- Before this migration attempt_count meant the claim generation, because every
-- claim incremented it. So the legacy value must MOVE to claim_attempts and the
-- new failure budget must start at zero. Merely adding claim_attempts defaulted to
-- 0 would rewind a legacy generation N back to 0 (letting an already superseded
-- worker pass the fence) and would misread N historical claims as N failures
-- (letting the next real failure terminate the Job early).
ALTER TABLE jobs
    ADD COLUMN claim_attempts integer NOT NULL DEFAULT 0
    CONSTRAINT jobs_claim_attempts_valid CHECK (claim_attempts >= 0);

-- Carry the legacy claim generation across and start the failure budget clean.
UPDATE jobs
SET claim_attempts = attempt_count,
    attempt_count = 0;

-- The application always supplies claim_attempts, so no database default remains.
ALTER TABLE jobs
    ALTER COLUMN claim_attempts DROP DEFAULT;
