ALTER TABLE copies
    ADD CONSTRAINT copies_storage_binding_fk
    FOREIGN KEY (storage_binding_id)
    REFERENCES storage_bindings(storage_binding_id);

CREATE TABLE indexcore_projection_cursors (
    storage_binding_id uuid PRIMARY KEY
        REFERENCES storage_bindings(storage_binding_id),
    last_event_seq bigint NOT NULL CHECK (last_event_seq >= 0),
    updated_at timestamptz NOT NULL
);
