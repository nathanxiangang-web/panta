ALTER TABLE storage_bindings
    ADD COLUMN provider_scope text NULL
    CONSTRAINT storage_bindings_provider_scope_valid CHECK (
        provider_scope IS NULL
        OR (
            btrim(provider_scope) <> ''
            AND char_length(provider_scope) <= 1024
        )
    );
