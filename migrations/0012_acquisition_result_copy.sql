-- Gate 3.10 (D-033): READY is anchored to one canonical projected Copy.
--
-- Gate 3.9 gave acquisition a deterministic pre-canonical locator
-- (indexcore_root_id + target_path + result_name). Gate 3.10 turns that coordinate
-- into one stable product result only after IndexCore canonical truth and Panta
-- Journal projection agree, and records that result durably:
--
--   Manifest.result_copy_id -> Panta Copy
--                          -> indexcore_root_id
--                          -> indexcore_resource_id
--                          -> storage_binding_id
--
-- result_copy_id is the historical acquisition result link. Later Copy availability
-- changes (rename, move, removal) never rewrite it, so acquisition history never has
-- to be re-derived by guessing from a path.
ALTER TABLE acquisition_manifests
    ADD COLUMN result_copy_id uuid NULL
        REFERENCES copies(copy_id);

-- No DEFAULT: a default would fabricate a result link for rows that never confirmed
-- one. No backfill either. Existing v11 rows keep result_copy_id = NULL, which is the
-- honest state of an acquisition that predates canonical confirmation.

-- A finalized acquisition must carry its result link, and no other state may carry
-- one, so a partially applied finalization can never be durable.
--
-- NOT VALID is deliberate: a pre-Gate-3.10 database may already hold a READY row
-- without a result link. Such a row is explicit recovery debt, not a migration
-- failure, and PostgreSQL still enforces this check for every new or updated row.
ALTER TABLE acquisition_manifests
    ADD CONSTRAINT acquisition_manifests_ready_requires_result_copy CHECK (
        (state = 'READY' AND result_copy_id IS NOT NULL)
        OR (state <> 'READY' AND result_copy_id IS NULL)
    ) NOT VALID;

COMMENT ON COLUMN acquisition_manifests.result_copy_id IS
    'Gate 3.10 D-033: the one canonical projected Copy that confirmed this acquisition. Immutable once set. Historical result link: later Copy availability changes do not rewrite it.';
