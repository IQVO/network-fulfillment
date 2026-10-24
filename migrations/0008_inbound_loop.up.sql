-- ADR 0028: what the inbound loop must remember.
--
-- Sellers ship SSCC-labelled cartons to a site; the network tells us through the
-- Vendor API; we register an ASN in inbound-receiving and, when its receipt
-- closes, report the receipt back. Every step is restart-safe because the state
-- lives here, in this service's Postgres.

-- The receipt we submit is a write like the others: the submission ledger keeps
-- its body and idempotency key. The kind check is widened to admit it (and
-- NODE_PROFILE, which the supply loop's migration admits too, so the order of
-- the two migrations does not matter).
ALTER TABLE network_submissions DROP CONSTRAINT network_submissions_kind_check;
ALTER TABLE network_submissions ADD CONSTRAINT network_submissions_kind_check
    CHECK (kind IN ('ACKNOWLEDGEMENT', 'SHIPMENT_CONFIRMATION', 'NODE_REGISTRATION', 'NODE_PROFILE', 'INBOUND_RECEIPT'));

-- shipmentRef <-> ASN number <-> status. The shipment tree is kept as polled so a
-- shipment that could not be registered yet (an unknown GTIN) is retried from
-- here, with backoff, without asking the network again.
CREATE TABLE inbound_shipments (
    shipment_ref    TEXT        PRIMARY KEY,
    asn_number      TEXT        NOT NULL UNIQUE,
    site_id         TEXT        NOT NULL,
    owner_ref       TEXT        NOT NULL DEFAULT '',
    shipped_at      TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL
        CHECK (status IN ('PENDING', 'REGISTERED', 'REJECTED')),
    attempts        INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT        NOT NULL DEFAULT '',
    payload         JSONB       NOT NULL,
    registered_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX inbound_shipments_due
    ON inbound_shipments (next_attempt_at)
    WHERE status = 'PENDING';

-- The poll cursor per site: the shippedAt of the newest shipment recorded.
CREATE TABLE inbound_poll_cursors (
    site_id       TEXT        PRIMARY KEY,
    shipped_after TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- GTIN -> SKU answers of product-master, kept so a hot path never calls it twice
-- for the same GTIN. Only an answer is cached (never a miss or an error).
CREATE TABLE gtin_sku_cache (
    gtin           TEXT        PRIMARY KEY CHECK (gtin ~ '^[0-9]{14}$'),
    sku            TEXT        NOT NULL,
    level          TEXT        NOT NULL,
    units_per_pack INTEGER     NOT NULL CHECK (units_per_pack >= 1),
    active         BOOLEAN     NOT NULL,
    resolved_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
