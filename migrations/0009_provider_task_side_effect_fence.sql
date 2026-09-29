-- Gate 3.4 Round 2 (D-023): durable pre-side-effect fence.
--
-- provider_task_ref must stay NULL while a start attempt is only reserved, so the
-- durable state itself records that an external start may already have happened
-- without a known reference. Migration 0008 is already published, so this is an
-- append-only refinement of the same table.
--
-- Side-effect certainty only: this column never mirrors provider task lifecycle
-- status, and the provider remains authoritative for its own task state.
ALTER TABLE acquisition_provider_tasks
    ADD COLUMN state text NOT NULL DEFAULT 'REFERENCE_KNOWN'
    CONSTRAINT acquisition_provider_tasks_state_valid CHECK (
        state IN ('START_RESERVED', 'REFERENCE_KNOWN')
    );

ALTER TABLE acquisition_provider_tasks
    ALTER COLUMN state DROP DEFAULT;

ALTER TABLE acquisition_provider_tasks
    ALTER COLUMN provider_task_ref DROP NOT NULL;

-- A START_RESERVED row records a start attempt whose reference is not yet known.
-- A REFERENCE_KNOWN row records a committed opaque reference.
ALTER TABLE acquisition_provider_tasks
    ADD CONSTRAINT acquisition_provider_tasks_side_effect_state CHECK (
        (
            state = 'START_RESERVED'
            AND provider_task_ref IS NULL
        )
        OR (
            state = 'REFERENCE_KNOWN'
            AND provider_task_ref IS NOT NULL
            AND btrim(provider_task_ref) <> ''
            AND char_length(provider_task_ref) <= 1024
        )
    );
