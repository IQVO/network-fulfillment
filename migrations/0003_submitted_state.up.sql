-- Adds SUBMITTED to the network_orders.state vocabulary and to the
-- states a local_order_id may exist alongside.
--
-- ADR 0001 §5's asynchronous-submission model needed a real state for
-- "we decided to accept and told the network, but that submission is
-- not yet reconciled" (ReceiveNetworkDemand.Submit / LinkLocalOrder),
-- distinct from the settled ACKNOWLEDGED a later reconciliation pass
-- reaches (ReconcileSubmittedOrders.ConfirmAcknowledgement). The local
-- order is linked at SUBMITTED time — before reconciliation, not after
-- — because ReconcileSubmittedOrders needs it to release or cancel the
-- hold once the network's own transaction-status record resolves it.
ALTER TABLE network_orders DROP CONSTRAINT network_orders_state_check;
ALTER TABLE network_orders ADD CONSTRAINT network_orders_state_check
    CHECK (state IN ('NEW', 'SUBMITTED', 'ACKNOWLEDGED', 'REJECTED', 'CONFIRMED'));

ALTER TABLE network_orders DROP CONSTRAINT local_order_requires_answer;
ALTER TABLE network_orders ADD CONSTRAINT local_order_requires_answer
    CHECK (local_order_id IS NULL OR state IN ('SUBMITTED', 'ACKNOWLEDGED', 'CONFIRMED'));
