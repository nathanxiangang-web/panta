CREATE TABLE storage_connections (
    storage_connection_id uuid PRIMARY KEY,
    provider_type text NOT NULL CHECK (provider_type <> ''),
    credential_ref text NULL,
    status text NOT NULL CHECK (status IN ('ACTIVE', 'DISABLED')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE storage_bindings (
    storage_binding_id uuid PRIMARY KEY,
    storage_connection_id uuid NOT NULL
        REFERENCES storage_connections(storage_connection_id),
    openlist_mount_path text NOT NULL,
    indexcore_root_id text NOT NULL CHECK (indexcore_root_id <> ''),
    status text NOT NULL CHECK (status IN ('ACTIVE', 'DISABLED')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT storage_bindings_mount_path_normalized CHECK (
        position(E'\\' IN openlist_mount_path) = 0
        AND (
            openlist_mount_path = '/'
            OR (
                left(openlist_mount_path, 1) = '/'
                AND right(openlist_mount_path, 1) <> '/'
                AND position('//' IN openlist_mount_path) = 0
                AND openlist_mount_path !~ '(^|/)\.{1,2}(/|$)'
            )
        )
    ),
    CONSTRAINT storage_bindings_indexcore_root_unique UNIQUE (indexcore_root_id),
    CONSTRAINT storage_bindings_connection_mount_unique
        UNIQUE (storage_connection_id, openlist_mount_path)
);
