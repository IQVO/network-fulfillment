-- network-fulfillment "Network Order Acknowledgement & Translation"
-- analytics read model.
--
-- This is the ANALYTICAL database, separate from the OLTP database. It is
-- written only by cmd/netfulfil-projector and read (read-only) by
-- cmd/netfulfil-reports. The tables here are projections derived from the
-- analytics event stream, not sources of truth.
--
-- Like process-path-management's catalogue-growth rollup, this report has
-- no spatial scope dimension worth grouping by, so the rollup is bucketed
-- by DAY alone.

-- Idempotency + freshness: every applied analytics event id is recorded
-- here exactly once. applied_at is wall-clock insert time; occurred_at is
-- the event's business time, used to compute the projection's freshness
-- lag.
CREATE TABLE analytics_processed_events (
    event_id    TEXT PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_analytics_processed_events_occurred_at
    ON analytics_processed_events (occurred_at DESC);

-- Consumer-level dedupe set, used by the inbound consumer's idempotency
-- gate. Kept SEPARATE from analytics_processed_events (which the
-- projection UPSERT claims) so the two idempotency layers do not race to
-- claim the same event_id: the consumer gate admits the event, the
-- projection then records its effect.
CREATE TABLE analytics_consumed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The acknowledgement & translation rollup fact table: one row per
-- day_bucket. Counters are UPSERTed as events arrive.
CREATE TABLE acknowledgement_rollup (
    day_bucket                           TIMESTAMPTZ PRIMARY KEY,
    orders_received                      BIGINT NOT NULL DEFAULT 0,
    orders_acknowledged                  BIGINT NOT NULL DEFAULT 0,
    -- Running sum/count rather than a stored average, so an idempotent
    -- re-application never has to "un-average" a prior value; the average
    -- is computed at read time (sum / count).
    sum_acknowledgement_latency_seconds  DOUBLE PRECISION NOT NULL DEFAULT 0,
    acknowledgement_latency_count        BIGINT NOT NULL DEFAULT 0,
    -- Rejections split by cause: a catalogue gap (untranslatable SKU) is a
    -- different operational signal than a genuine domain refusal
    -- (infeasible deadline) or an operational failure (deadline missed
    -- with no answer ever sent).
    orders_rejected_untranslatable_sku   BIGINT NOT NULL DEFAULT 0,
    orders_rejected_domain               BIGINT NOT NULL DEFAULT 0,
    acknowledgement_deadlines_missed     BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_acknowledgement_rollup_day_bucket
    ON acknowledgement_rollup (day_bucket);
