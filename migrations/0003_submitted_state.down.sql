ALTER TABLE network_orders DROP CONSTRAINT local_order_requires_answer;
ALTER TABLE network_orders ADD CONSTRAINT local_order_requires_answer
    CHECK (local_order_id IS NULL OR state IN ('ACKNOWLEDGED', 'CONFIRMED'));

ALTER TABLE network_orders DROP CONSTRAINT network_orders_state_check;
ALTER TABLE network_orders ADD CONSTRAINT network_orders_state_check
    CHECK (state IN ('NEW', 'ACKNOWLEDGED', 'REJECTED', 'CONFIRMED'));
