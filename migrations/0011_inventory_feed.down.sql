-- Reverses 0011_inventory_feed.up.sql. Destroys the feed state, the rejected-listing
-- records and the INVENTORY ledger rows: a live adapter would then feed again from
-- scratch (the next feed is sent whatever it holds). Safe for the network: a feed
-- is idempotent in content, and the next complete feed restates every listing.
--
-- The ledger's kind check goes back to 0008's five kinds, after the rows of the
-- kind it no longer admits are removed. The rows of the other kinds survive.
DROP TABLE IF EXISTS network_rejected_listings;
DROP TABLE IF EXISTS network_inventory_feeds;
DELETE FROM network_submissions WHERE kind = 'INVENTORY';
ALTER TABLE network_submissions DROP CONSTRAINT network_submissions_kind_check;
ALTER TABLE network_submissions ADD CONSTRAINT network_submissions_kind_check
    CHECK (kind IN ('ACKNOWLEDGEMENT', 'SHIPMENT_CONFIRMATION', 'NODE_REGISTRATION', 'NODE_PROFILE', 'INBOUND_RECEIPT'));
