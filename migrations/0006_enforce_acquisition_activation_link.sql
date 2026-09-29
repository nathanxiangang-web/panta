ALTER TABLE acquisition_manifests
    ADD CONSTRAINT acquisition_manifests_activation_link CHECK (
        (state <> 'PENDING' OR job_id IS NULL)
        AND (state <> 'ACTIVE' OR job_id IS NOT NULL)
    );
