CREATE TABLE acquisition_manifests (
    manifest_id uuid PRIMARY KEY,
    user_id uuid NULL,
    source_type text NOT NULL CHECK (
        btrim(source_type) <> '' AND char_length(source_type) <= 64
    ),
    source_ref text NOT NULL CHECK (
        btrim(source_ref) <> '' AND char_length(source_ref) <= 4096
    ),
    expected_name text NULL CHECK (
        expected_name IS NULL OR (
            btrim(expected_name) <> '' AND char_length(expected_name) <= 512
        )
    ),
    target_storage_binding_id uuid NOT NULL
        REFERENCES storage_bindings(storage_binding_id),
    target_path text NOT NULL,
    asset_id uuid NULL REFERENCES assets(asset_id),
    release_id uuid NULL REFERENCES releases(release_id),
    variant_id uuid NULL REFERENCES variants(variant_id),
    job_id uuid NULL REFERENCES jobs(job_id),
    state text NOT NULL CHECK (
        state IN (
            'PENDING',
            'ACTIVE',
            'AWAITING_VISIBILITY',
            'AWAITING_CANONICAL',
            'READY',
            'FAILED',
            'RECOVERY_REQUIRED',
            'CANCELED'
        )
    ),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT acquisition_manifests_target_path_normalized CHECK (
        char_length(target_path) <= 2048
        AND position(E'\\' IN target_path) = 0
        AND (
            target_path = '/'
            OR (
                left(target_path, 1) = '/'
                AND right(target_path, 1) <> '/'
                AND position('//' IN target_path) = 0
                AND target_path !~ '(^|/)\.{1,2}(/|$)'
            )
        )
    ),
    CONSTRAINT acquisition_manifests_lineage_shape CHECK (
        (release_id IS NULL OR asset_id IS NOT NULL)
        AND (variant_id IS NULL OR release_id IS NOT NULL)
    ),
    CONSTRAINT acquisition_manifests_job_unique UNIQUE (job_id)
);
