-- Transactional outbox. Every domain event a use case raises is written
-- here — one row per (event x topic) — in the SAME transaction as the
-- network_orders/network_order_lines change that caused it; a relay
-- drains unpublished rows onto Kafka afterwards. The store and the topic
-- can therefore never diverge.
--
-- One row per (event x topic) rather than one row per event: this
-- context fans each domain event out to both the integration topic and
-- the analytics topic (fanOutPublisher in cmd/netfulfil), and each needs
-- its own already-encoded payload (the analytics envelope differs from
-- the integration one).
CREATE TABLE outbox_events (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    key          BYTEA,
    value        BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);

-- The relay only ever asks "what is still unpublished, oldest first"; a
-- partial index keeps that scan tiny no matter how much published history
-- accumulates.
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
