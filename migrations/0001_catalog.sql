CREATE TABLE assets (
    asset_id uuid PRIMARY KEY,
    canonical_name text NOT NULL,
    category text NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE releases (
    release_id uuid PRIMARY KEY,
    asset_id uuid NOT NULL REFERENCES assets(asset_id),
    version_raw text NOT NULL,
    version_normalized text NULL,
    version_scheme text NOT NULL CHECK (
        version_scheme IN ('SEMVER', 'DATE', 'REVISION', 'UPSTREAM_ID', 'CUSTOM', 'NONE')
    ),
    channel text NOT NULL,
    release_date date NULL,
    source_ref text NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE variants (
    variant_id uuid PRIMARY KEY,
    release_id uuid NOT NULL REFERENCES releases(release_id),
    variant_key text NOT NULL,
    attributes jsonb NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (release_id, variant_key)
);

CREATE TABLE copies (
    copy_id uuid PRIMARY KEY,
    variant_id uuid NULL REFERENCES variants(variant_id),
    indexcore_root_id text NOT NULL,
    indexcore_resource_id text NOT NULL,
    storage_binding_id uuid NOT NULL,
    availability text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (indexcore_root_id, indexcore_resource_id)
);
