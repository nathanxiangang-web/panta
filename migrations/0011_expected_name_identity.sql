-- Gate 3.9 (D-031): acquisition result identity is a direct child of target_path.
--
-- Manifest.expected_name is the one direct-child name expected under the scoped
-- refresh directory. It is the durable identity that lets a later canonical
-- confirmation gate identify the acquired resource without guessing from timing,
-- Journal ordering, "only new item" heuristics, provider file IDs, or OpenList
-- paths.
--
-- This migration is append-only and hardening only. It adds no column: it constrains
-- the existing expected_name so a persisted identity can only ever be one direct
-- path segment.

-- 1. Direct-child segment constraints for any non-null identity.
--
--    Mirrors acquisition.ValidateExpectedName and
--    contracts.ValidateDirectChildName:
--      - nonblank, so a whitespace-only name is never durable identity;
--      - no path separator, so the identity can never address outside target_path;
--      - not "." or "..", so it can never name the parent or current scope itself.
--
--    The 512-rune bound already exists in the application and in the pre-existing
--    column constraints; it is re-asserted here so the database is authoritative.
ALTER TABLE acquisition_manifests
    ADD CONSTRAINT acquisition_manifests_expected_name_direct_child CHECK (
        expected_name IS NULL
        OR (
            btrim(expected_name) <> ''
            AND char_length(expected_name) <= 512
            AND position('/' IN expected_name) = 0
            AND position(E'\\' IN expected_name) = 0
            AND expected_name <> '.'
            AND expected_name <> '..'
        )
    );

-- 2. A Manifest that has left the provider stage must carry its identity.
--
--    NOT VALID is deliberate and is the accepted mechanism from Issue #41: a
--    pre-Gate-3.9 database may already hold a row in one of these states without an
--    expected_name. Such a row is explicit recovery debt, not a migration failure,
--    so the constraint is added NOT VALID and left for the application to reconcile.
--    PostgreSQL still enforces a NOT VALID CHECK for every NEW or UPDATED row, which
--    is exactly the required fence: all future writes are constrained while
--    historical rows remain untouched and inspectable.
ALTER TABLE acquisition_manifests
    ADD CONSTRAINT acquisition_manifests_later_state_requires_expected_name CHECK (
        state NOT IN ('AWAITING_VISIBILITY', 'AWAITING_CANONICAL', 'READY')
        OR expected_name IS NOT NULL
    ) NOT VALID;

-- Record the intentional debt so an operator can find it without guessing.
COMMENT ON COLUMN acquisition_manifests.expected_name IS
    'Gate 3.9 D-031: the one direct-child name expected under target_path. AUTHENTICATED identity, not a hint. The later-state constraint is intentionally NOT VALID so rows predating Gate 3.9 are explicit recovery debt while every new or updated row is fenced.';
