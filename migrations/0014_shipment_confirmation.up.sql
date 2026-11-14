-- The (shipment reference, order id) idempotency ledger behind
-- ConfirmNetworkOrderShipment (ADR 0014 addendum): one row per pair the
-- network was already told confirms that order. Written only inside the
-- use case's atomic scope, so a claimed row and the CONFIRMED order row
-- commit together. shipment_ref is outbound-shipping's load id on the
-- event path, the NetworkRef itself on the manual endpoint path;
-- order_id is the order-management OrderId when one is linked (the
-- ShipmentConfirmed join key), else the NetworkRef.

CREATE TABLE shipment_confirmations (
    shipment_ref TEXT        NOT NULL CHECK (shipment_ref <> ''),
    order_id     TEXT        NOT NULL CHECK (order_id <> ''),
    network_ref  TEXT        NOT NULL CHECK (network_ref <> ''),
    confirmed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (shipment_ref, order_id)
);

-- The join outbound-shipping's ShipmentConfirmed makes:
-- orders[].order_id -> NetworkOrder.localOrderId. Partial: rows that never
-- became a local order (rejected before the planner raised one) stay out
-- of the index entirely.
CREATE INDEX network_orders_local_order_id_idx
    ON network_orders (local_order_id)
    WHERE local_order_id IS NOT NULL;
