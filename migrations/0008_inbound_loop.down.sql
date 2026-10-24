DROP TABLE IF EXISTS gtin_sku_cache;
DROP TABLE IF EXISTS inbound_poll_cursors;
DROP TABLE IF EXISTS inbound_shipments;
DELETE FROM network_submissions WHERE kind = 'INBOUND_RECEIPT';
ALTER TABLE network_submissions DROP CONSTRAINT network_submissions_kind_check;
ALTER TABLE network_submissions ADD CONSTRAINT network_submissions_kind_check
    CHECK (kind IN ('ACKNOWLEDGEMENT', 'SHIPMENT_CONFIRMATION', 'NODE_REGISTRATION', 'NODE_PROFILE'));
