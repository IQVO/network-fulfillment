-- Reverses 0005. The ids that migration let NEW and REJECTED rows carry are
-- cleared first, because the restored CHECK (local order only from SUBMITTED
-- on) would refuse them: this is the information the old schema could not hold.
-- A SUBMITTED, ACKNOWLEDGED or CONFIRMED order keeps its id.
DROP INDEX network_orders_open_holds;

ALTER TABLE network_orders DROP CONSTRAINT hold_outcome_requires_hold;
ALTER TABLE network_orders DROP CONSTRAINT hold_outcome_has_settled_at;
ALTER TABLE network_orders DROP CONSTRAINT hold_outcome_vocabulary;

ALTER TABLE network_orders DROP COLUMN hold_settled_at;
ALTER TABLE network_orders DROP COLUMN hold_outcome;

UPDATE network_orders SET local_order_id = NULL WHERE state IN ('NEW', 'REJECTED');

ALTER TABLE network_orders ADD CONSTRAINT local_order_requires_answer
    CHECK (local_order_id IS NULL OR state IN ('SUBMITTED', 'ACKNOWLEDGED', 'CONFIRMED'));
