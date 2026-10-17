-- ADR 0024 (gap OM-0020): record the order-management hold the moment it exists,
-- and how it ended, so the orphaned-hold sweep can find and free a hold nobody
-- decided.
--
-- Until now local_order_id could exist only from SUBMITTED on
-- (local_order_requires_answer, migration 0003). A hold raised for an
-- infeasible deadline, or one whose cancel failed, therefore had no row to
-- name it: the order stayed NEW (or became REJECTED) with local_order_id NULL
-- and order-management kept the reservations forever, because it has no
-- timeout of its own (its ADR 0020, "Accepted deferral").
--
-- Every state may now carry the id, so that CHECK has nothing left to say and
-- is dropped. What it guarded is the domain's rule (NetworkOrder.RecordHold:
-- only a NEW order takes a hold, once; LinkLocalOrder: the same id is a
-- no-op). The new CHECKs below guard the shape of the outcome itself.
ALTER TABLE network_orders DROP CONSTRAINT local_order_requires_answer;

-- How the hold ended, as far as this context has confirmed it. NULL = open:
-- raised, neither cancelled nor found already released.
ALTER TABLE network_orders ADD COLUMN hold_outcome    TEXT;
ALTER TABLE network_orders ADD COLUMN hold_settled_at TIMESTAMPTZ;

ALTER TABLE network_orders ADD CONSTRAINT hold_outcome_vocabulary
    CHECK (hold_outcome IS NULL OR hold_outcome IN ('CANCELLED', 'ALREADY_RELEASED'));

-- The outcome and the instant it was recorded travel together.
ALTER TABLE network_orders ADD CONSTRAINT hold_outcome_has_settled_at
    CHECK ((hold_outcome IS NULL) = (hold_settled_at IS NULL));

-- An outcome is about a hold, so there must be one recorded.
ALTER TABLE network_orders ADD CONSTRAINT hold_outcome_requires_hold
    CHECK (hold_outcome IS NULL OR local_order_id IS NOT NULL);

-- The sweep's candidates (ListOpenHolds): a recorded, unsettled hold on an
-- order that was never committed to. Partial, so it stays tiny: in a healthy
-- fleet it is empty almost always.
CREATE INDEX network_orders_open_holds
    ON network_orders (acknowledge_by)
    WHERE local_order_id IS NOT NULL
      AND hold_outcome IS NULL
      AND state IN ('NEW', 'REJECTED');
