-- Gate 3.7 Round 2: separate claim generation from the failure retry budget.
--
-- A single ACQUISITION Job now runs the whole acquisition workflow, so the same
-- Job is claimed repeatedly for provider polling, then visibility, then canonical
-- stages. Using one counter for both the stale-worker fence and the failure budget
-- meant polling could exhaust max_attempts and leave a Job stuck in RETRY_WAIT
-- forever, or worse, block the visibility stage after a successful download.
--
-- claim_attempts  = monotonically increasing claim generation; the fencing token
--                   for stale workers. Never bounded.
-- attempt_count   = failure/retry budget consumed only by real failures. Bounded
--                   by max_attempts. The column name is kept for compatibility;
--                   the Go field is jobs.Job.FailureCount.
--
-- Existing RUNNING/terminal rows keep attempt_count as their failure budget and
-- adopt the column default 0 for claim generation. IN_PROGRESS handling never
-- consults max_attempts, so a legacy row's first claim correctly becomes
-- generation 1 regardless of how much budget it already spent.
ALTER TABLE jobs
    ADD COLUMN claim_attempts integer NOT NULL DEFAULT 0
    CONSTRAINT jobs_claim_attempts_valid CHECK (claim_attempts >= 0);

ALTER TABLE jobs
    ALTER COLUMN claim_attempts DROP DEFAULT;
