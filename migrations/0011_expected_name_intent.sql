-- Gate 3.9 (D-032): expected_name is request intent, not the acquired-result locator.
--
-- This migration is deliberately scoped to expected_name only. D-032 established that
-- expected_name stays request intent and is never rewritten by provider execution, so
-- nothing here ties it to a later workflow state and nothing here rewrites a value.
--
-- What it does add is the one guarantee the intent itself needs: a persisted
-- expectation is bounded required text. It adds no separator or dot-component rule,
-- because a request expectation is not a path segment and rejecting "/" there would
-- over-constrain intent.
--
-- The direct-child rule belongs to result_name, which migration 0012 introduces.

-- Re-assert the intent bound so the database, not only the application, is
-- authoritative. The pre-existing table constraint already enforces this; the
-- explicit named constraint makes the guarantee auditable by name.
ALTER TABLE acquisition_manifests
    ADD CONSTRAINT acquisition_manifests_expected_name_intent CHECK (
        expected_name IS NULL
        OR (
            btrim(expected_name) <> ''
            AND char_length(expected_name) <= 512
        )
    );

COMMENT ON COLUMN acquisition_manifests.expected_name IS
    'Gate 3.9 D-032: optional request-time expectation and fallback only. Never rewritten by provider execution and never the acquired-result locator. The durable locator is acquisition_manifests.result_name.';
