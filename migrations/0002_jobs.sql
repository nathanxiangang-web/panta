CREATE TABLE jobs (
    job_id uuid PRIMARY KEY,
    job_type text NOT NULL,
    payload jsonb NOT NULL,
    state text NOT NULL CHECK (
        state IN (
            'QUEUED',
            'RUNNING',
            'RETRY_WAIT',
            'RECOVERY_REQUIRED',
            'SUCCEEDED',
            'FAILED',
            'CANCELED'
        )
    ),
    idempotency_key text NULL,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts >= 1),
    next_attempt_at timestamptz NULL,
    lease_owner text NULL,
    lease_expires_at timestamptz NULL,
    last_error text NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    started_at timestamptz NULL,
    finished_at timestamptz NULL,
    CHECK (
        (state = 'RUNNING' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (state <> 'RUNNING' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (state <> 'RETRY_WAIT' OR next_attempt_at IS NOT NULL)
);

CREATE UNIQUE INDEX jobs_idempotency_key_unique
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX jobs_claimable_idx
    ON jobs (state, next_attempt_at, created_at, job_id)
    WHERE state IN ('QUEUED', 'RETRY_WAIT');

CREATE INDEX jobs_expired_running_lease_idx
    ON jobs (lease_expires_at, job_id)
    WHERE state = 'RUNNING';
