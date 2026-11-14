-- ADR 0014 addendum D7: a ShipmentConfirmed that arrives while the order is
-- still SUBMITTED is recorded on the order and confirmed once the order is
-- ACKNOWLEDGED, instead of being dead-lettered.
--
-- An order stays SUBMITTED until ReconcileSubmittedOrders hears the network's
-- verdict, and the acknowledgement window is 24 h, so a load can legitimately
-- depart in between. The first shipment noted wins (NetworkOrder
-- .NoteShipmentBeforeAcknowledge), so one order carries at most one: its
-- reference (outbound-shipping's load id), its package count (0 when the event
-- carried none) and when it was noted. ConfirmShipment clears all three.
ALTER TABLE network_orders ADD COLUMN pending_shipment_ref      TEXT;
ALTER TABLE network_orders ADD COLUMN pending_package_count     INTEGER;
ALTER TABLE network_orders ADD COLUMN pending_shipment_noted_at TIMESTAMPTZ;

-- The three travel together: a shipment is all of them or none of them.
ALTER TABLE network_orders ADD CONSTRAINT pending_shipment_all_or_none
    CHECK ((pending_shipment_ref IS NULL) = (pending_package_count IS NULL)
       AND (pending_shipment_ref IS NULL) = (pending_shipment_noted_at IS NULL));

ALTER TABLE network_orders ADD CONSTRAINT pending_package_count_not_negative
    CHECK (pending_package_count >= 0);

-- The reference is the confirmation's ledger key, so it is never blank.
ALTER TABLE network_orders ADD CONSTRAINT pending_shipment_ref_not_blank
    CHECK (pending_shipment_ref IS NULL OR pending_shipment_ref <> '');

-- The candidates of ConfirmPendingShipments (ListPendingShipments). Partial,
-- so it stays tiny: a shipment is pending only from an early departure to the
-- next reconcile, and in a healthy fleet the index is empty almost always.
CREATE INDEX network_orders_pending_shipments
    ON network_orders (pending_shipment_noted_at)
    WHERE pending_shipment_ref IS NOT NULL;
