-- Gate 3.4 (D-023): durable, provider-neutral provider-task linkage.
--
-- One Manifest maps to at most one provider task and one Job maps to at most one
-- provider task in the MVP. The provider remains authoritative for its own task
-- status, so no provider status column is stored here. provider_id and
-- provider_task_ref are opaque bounded text and are never parsed by Panta.
CREATE TABLE acquisition_provider_tasks (
    manifest_id uuid PRIMARY KEY
        REFERENCES acquisition_manifests(manifest_id),
    job_id uuid NOT NULL UNIQUE
        REFERENCES jobs(job_id),
    provider_id text NOT NULL CHECK (
        btrim(provider_id) <> '' AND char_length(provider_id) <= 128
    ),
    provider_task_ref text NOT NULL CHECK (
        btrim(provider_task_ref) <> '' AND char_length(provider_task_ref) <= 1024
    ),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
