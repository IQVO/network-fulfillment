-- Fleet REST API guideline section 2: the Idempotency-Key store behind every POST
-- (today only POST /network-orders/{networkRef}/shipment-confirmation).
--
-- The key alone is the primary key (a key is GLOBAL, not per route): the request
-- fingerprint (method + path + sha256 of the body) is compared against the stored
-- one, and a different fingerprint is a 409 idempotency-key-reused. A row with a
-- NULL status is a claim still being executed; a 5xx releases (deletes) it so a
-- retry re-executes. Rows are kept at least 24 hours.

CREATE TABLE idempotency_keys (
    idem_key     TEXT        PRIMARY KEY CHECK (idem_key <> ''),
    fingerprint  TEXT        NOT NULL CHECK (fingerprint <> ''),
    status       INTEGER,
    content_type TEXT        NOT NULL DEFAULT '',
    location     TEXT        NOT NULL DEFAULT '',
    body         BYTEA,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idempotency_keys_created_at_idx ON idempotency_keys (created_at);
