-- Gate 3.9 (D-032): add the durable acquired-result locator.
--
-- D-032 separates two facts that the earlier D-031 interpretation conflated:
--
--   expected_name  optional request-time expectation / fallback hint
--                  intent, never rewritten by provider execution
--   result_name    durable provider-stage top-level acquired-result locator
--                  frozen once persisted, immutable afterwards
--
-- The canonical candidate identity for the later confirmation gate is therefore:
--
--   StorageBinding.indexcore_root_id + Manifest.target_path + Manifest.result_name
--
-- This migration adds the locator column only.
ALTER TABLE acquisition_manifests
    ADD COLUMN result_name text NULL;

-- The locator must be exactly one direct path segment. Mirrors
-- acquisition.ValidateResultName and contracts.ValidateDirectChildName:
--   - nonblank, so whitespace is never a durable locator;
--   - bounded by the same 512-rune limit as the intent field;
--   - no forward or backslash separator, so it can never address outside
--     target_path;
--   - not "." or "..", so it can never name the scope itself or its parent.
--
-- A NULL result_name stays valid: it is the state of every Manifest that has not yet
-- reached a provider success.
ALTER TABLE acquisition_manifests
    ADD CONSTRAINT acquisition_manifests_result_name_direct_child CHECK (
        result_name IS NULL
        OR (
            btrim(result_name) <> ''
            AND char_length(result_name) <= 512
            AND position('/' IN result_name) = 0
            AND position(E'\\' IN result_name) = 0
            AND result_name <> '.'
            AND result_name <> '..'
        )
    );

-- No DEFAULT is permitted. A default would fabricate a locator for rows that never
-- observed one, which D-032 explicitly forbids.
COMMENT ON COLUMN acquisition_manifests.result_name IS
    'Gate 3.9 D-032: durable provider-stage acquired-result locator. NULL until a provider success resolves it, immutable once persisted. Locator evidence only: never Canonical truth, never READY.';

-- Existing v10 rows keep result_name = NULL. No historical row is backfilled and no
-- guess is recorded, because a locator that was never observed cannot be recovered
-- from timing, Journal order, or directory listing.
--
-- Enforcement that automatic progression to observation carries a resolved locator
-- lives in the provider-success transition, not here: the database cannot know which
-- historical rows were legitimately observed and which are pre-Gate-3.9 debt.
