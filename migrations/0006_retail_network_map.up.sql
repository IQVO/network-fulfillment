-- ADR 0026: what the live retail-network adapter must remember.
--
-- The fleet's identifiers and the network's identifiers meet ONLY here, as
-- stored rows: a site code and a node id, a NetworkRef and a purchase order
-- number, a listing and an opaque NetworkProductId, a write and the transaction
-- the network answered it with. Nothing is parsed out of a string and nothing is
-- derived: the NetworkRef is minted when a purchase order is first seen and kept.
--
-- These tables belong to the adapter (internal/adapters/outbound/retailnetwork).
-- In NETWORK_MODE=stub they stay empty. They have no foreign key to
-- network_orders: the aggregate must not depend on which gateway answered for it.

-- siteCode <-> nodeId.
CREATE TABLE network_nodes (
    site_code  TEXT        PRIMARY KEY,
    node_id    TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- NetworkRef <-> poNumber, with the vendor and node the order was issued to and
-- the network's own ackBy (the deadline the fleet's sweeps judge by).
CREATE TABLE network_purchase_orders (
    network_ref      TEXT        PRIMARY KEY,
    po_number        TEXT        NOT NULL UNIQUE,
    vendor_id        TEXT        NOT NULL,
    node_id          TEXT        NOT NULL,
    required_ship_by TIMESTAMPTZ NOT NULL,
    ack_by           TIMESTAMPTZ NOT NULL,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The lines, kept because an acknowledgement must echo every line number with
-- its ordered quantity, and because the listing the network named is what the
-- ACL was handed as an opaque NetworkProductId.
CREATE TABLE network_purchase_order_lines (
    network_ref        TEXT    NOT NULL
        REFERENCES network_purchase_orders (network_ref) ON DELETE CASCADE,
    line_no            INTEGER NOT NULL CHECK (line_no > 0),
    network_line_ref   TEXT    NOT NULL,
    listing_id         TEXT    NOT NULL,
    network_product_id TEXT    NOT NULL,
    quantity           INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (network_ref, line_no),
    UNIQUE (network_ref, network_line_ref)
);

-- The submission ledger. One row per (kind, subject): the idempotency key and the
-- exact request body are written BEFORE the first HTTP call and never change, so
-- a retry after a crash re-sends byte for byte what was first meant (a different
-- body under the same key would be refused as reuse), and a row that already has
-- a transaction id is never sent again. request_body is BYTEA, not JSONB: the
-- network fingerprints the raw bytes.
CREATE TABLE network_submissions (
    kind            TEXT        NOT NULL
        CHECK (kind IN ('ACKNOWLEDGEMENT', 'SHIPMENT_CONFIRMATION', 'NODE_REGISTRATION')),
    subject         TEXT        NOT NULL,
    idempotency_key TEXT        NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 128),
    request_body    BYTEA       NOT NULL,
    transaction_id  TEXT,
    status          TEXT        NOT NULL
        CHECK (status IN ('SENDING', 'PROCESSING', 'SUCCESS', 'FAILURE')),
    errors          TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, subject),
    -- Only a row that was never accepted, or that the network refused outright,
    -- can lack a transaction id.
    CONSTRAINT submission_transaction_matches_status
        CHECK (transaction_id IS NOT NULL OR status IN ('SENDING', 'FAILURE'))
);

-- The reconciliation candidates: writes the network has accepted and not settled.
CREATE INDEX network_submissions_unsettled
    ON network_submissions (kind, updated_at)
    WHERE status IN ('SENDING', 'PROCESSING');

-- {labelRef, trackingNumber, carrier} per package. A shipment confirmation names
-- exactly these tracking numbers, so a tracking number belongs to one package.
CREATE TABLE network_labels (
    network_ref     TEXT    NOT NULL
        REFERENCES network_purchase_orders (network_ref) ON DELETE CASCADE,
    package_no      INTEGER NOT NULL CHECK (package_no > 0),
    label_ref       TEXT    NOT NULL,
    tracking_number TEXT    NOT NULL UNIQUE,
    carrier         TEXT    NOT NULL,
    PRIMARY KEY (network_ref, package_no)
);
