-- ADR 0028 step 3: the local projection of inbound-receiving's receipt events and
-- the queue of closed receipts still to be reported to the network.
--
-- Everything is keyed by ASN number and exists only for ASNs this service
-- registered (inbound_shipments.asn_number): an event about any other ASN is
-- dropped by the consumer. The CloudEvents id of every applied event is kept, so
-- a redelivery is a no-op.

CREATE TABLE inbound_receipt_events (
    event_id   TEXT        PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A carton the dock accepted (ReceiptCartonReceived). carton_key is the SSCC, or
-- the internal LPN of a carton that arrived without a readable label.
CREATE TABLE inbound_receipt_cartons (
    asn_number  TEXT        NOT NULL REFERENCES inbound_shipments (asn_number) ON DELETE CASCADE,
    carton_key  TEXT        NOT NULL,
    sscc        TEXT        NOT NULL DEFAULT '',
    receipt_id  TEXT        NOT NULL,
    contents    JSONB       NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (asn_number, carton_key)
);

-- A carton discrepancy (CartonDiscrepancyRaised), one row per event.
CREATE TABLE inbound_receipt_findings (
    event_id   TEXT        PRIMARY KEY,
    asn_number TEXT        NOT NULL REFERENCES inbound_shipments (asn_number) ON DELETE CASCADE,
    receipt_id TEXT        NOT NULL,
    sscc       TEXT        NOT NULL DEFAULT '',
    kind       TEXT        NOT NULL,
    expected   TEXT        NOT NULL DEFAULT '',
    actual     TEXT        NOT NULL DEFAULT '',
    raised_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX inbound_receipt_findings_asn ON inbound_receipt_findings (asn_number);

-- A closed receipt and where its report to the network stands. PENDING until the
-- network has it (SENT), or REJECTED when it refused the receipt for good.
CREATE TABLE inbound_receipts (
    asn_number      TEXT        PRIMARY KEY REFERENCES inbound_shipments (asn_number) ON DELETE CASCADE,
    receipt_id      TEXT        NOT NULL,
    closed_at       TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'SENT', 'REJECTED')),
    attempts        INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT        NOT NULL DEFAULT '',
    sent_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX inbound_receipts_due ON inbound_receipts (next_attempt_at) WHERE status = 'PENDING';
