-- ADR 0027 (part 2): what the live retail-network adapter must remember to feed a
-- node's inventory.
--
-- 1. The submission ledger admits a new kind, INVENTORY. 0008 widened the kind
--    check to the five kinds that existed then (INVENTORY was not among them), so
--    the check is rebuilt here with all six. The list must stay a superset of 0008's:
--    dropping INBOUND_RECEIPT or NODE_PROFILE from it would fail on a ledger that
--    already holds such a row.
-- 2. network_inventory_feeds: where each part of a site's feed stands (the ledger
--    subject of the latest attempt, the fingerprint of what it carried and the
--    pass it described), so that "a feed is sent when its fingerprint changes and
--    an unchanged one at most hourly" survives a restart.
-- 3. network_rejected_listings: the listings the network refused as not on the node
--    (ErrListingNotOnNode), kept out of the next feeds so that one wrong listing
--    cannot silence the rest; a record older than a day is tried again.
--
-- Like the tables of 0006 and 0010 these belong to the adapter
-- (internal/adapters/outbound/retailnetwork). In NETWORK_MODE=stub they stay empty.

ALTER TABLE network_submissions DROP CONSTRAINT network_submissions_kind_check;
ALTER TABLE network_submissions ADD CONSTRAINT network_submissions_kind_check
    CHECK (kind IN ('ACKNOWLEDGEMENT', 'SHIPMENT_CONFIRMATION', 'NODE_REGISTRATION', 'NODE_PROFILE', 'INBOUND_RECEIPT', 'INVENTORY'));

CREATE TABLE network_inventory_feeds (
    site_code   TEXT        NOT NULL
        REFERENCES network_nodes (site_code),
    part        INTEGER     NOT NULL DEFAULT 1 CHECK (part >= 1),
    subject     TEXT        NOT NULL CHECK (subject <> ''),
    fingerprint TEXT        NOT NULL CHECK (fingerprint <> ''),
    as_of       TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (site_code, part)
);

CREATE TABLE network_rejected_listings (
    site_code   TEXT        NOT NULL
        REFERENCES network_nodes (site_code),
    listing_id  TEXT        NOT NULL CHECK (listing_id <> ''),
    reason      TEXT        NOT NULL DEFAULT '',
    rejected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (site_code, listing_id)
);
