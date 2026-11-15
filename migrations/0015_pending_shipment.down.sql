-- Reverses 0015. A shipment still pending at the time is dropped with its
-- columns: the confirmation it was waiting to make has to be replayed by hand
-- (the manual POST /orders/{ref}/shipment-confirmation) once the order is ACKNOWLEDGED.
DROP INDEX IF EXISTS network_orders_pending_shipments;

ALTER TABLE network_orders DROP CONSTRAINT IF EXISTS pending_shipment_ref_not_blank;
ALTER TABLE network_orders DROP CONSTRAINT IF EXISTS pending_package_count_not_negative;
ALTER TABLE network_orders DROP CONSTRAINT IF EXISTS pending_shipment_all_or_none;

ALTER TABLE network_orders DROP COLUMN IF EXISTS pending_shipment_noted_at;
ALTER TABLE network_orders DROP COLUMN IF EXISTS pending_package_count;
ALTER TABLE network_orders DROP COLUMN IF EXISTS pending_shipment_ref;
