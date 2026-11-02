-- 0013: optimistic-locking version for the NetworkOrder aggregate.
--
-- Every mutation of a network order now runs as a conditional UPDATE:
-- the row is written only while aggregate_version still equals the
-- version the writer loaded. A writer that loses that race (a sweep
-- versus the poller, a REST confirmation versus either) sees zero rows
-- and retries on a fresh load instead of silently overwriting the
-- other writer's transition (lost update).
--
-- Backfill: existing rows are all at version 0 — the first conditional
-- write after this migration loads 0 and writes 1, so no data repair
-- is needed.

ALTER TABLE network_orders
    ADD COLUMN aggregate_version INT NOT NULL DEFAULT 0;
