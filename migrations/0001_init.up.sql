-- network-fulfillment OLTP schema (ADR 0001).
--
-- This context owns the network PROTOCOL and nothing about the work:
-- there is no allocation, no promise, no release here. What must survive
-- a restart is the answer we owe the network, the deadline we owe it by,
-- and the mapping to the local order that work was raised under.

CREATE TABLE network_orders (
    -- The network's own identity for the demand (Amazon's purchase-order
    -- number). It is the PRIMARY KEY rather than a surrogate id because
    -- this context is Conformist upstream: the network addresses every
    -- acknowledgement and shipment confirmation BY this reference, so a
    -- second identity would be a second source of truth for the same fact.
    -- It stays an opaque TEXT — a future second network brings a different
    -- format and must not require a migration here.
    network_ref      TEXT        PRIMARY KEY,
    site_id          TEXT        NOT NULL,
    required_ship_by TIMESTAMPTZ NOT NULL,

    -- Derived at intake from received_at + the 24h window, then PERSISTED
    -- rather than recomputed on read. The window is anchored to when the
    -- demand actually arrived, and a poll that returns a batch after an
    -- outage may carry orders already near or past it — recomputing from
    -- row-load time would silently reset every one of those clocks.
    acknowledge_by   TIMESTAMPTZ NOT NULL,

    state            TEXT        NOT NULL
        CHECK (state IN ('NEW', 'ACKNOWLEDGED', 'REJECTED', 'CONFIRMED')),

    -- order-management's OrderId, NULL until acknowledged and linked.
    -- An explicit stored mapping, never a string convention: this fleet
    -- already got burned by an identifier that looked like an order
    -- reference and was not (order-management ADR 0018).
    local_order_id   TEXT,

    received_at      TIMESTAMPTZ NOT NULL,

    -- The aggregate permits a local order only once acknowledged
    -- (ErrNotAcknowledged). Restating it here means a bug in the
    -- application layer cannot quietly leave allocated work attached to
    -- demand we never committed to.
    CONSTRAINT local_order_requires_answer
        CHECK (local_order_id IS NULL OR state IN ('ACKNOWLEDGED', 'CONFIRMED'))
);

-- The sweep reads exactly this predicate: unanswered orders whose window
-- has closed. Partial, because NEW is a transient minority of the table
-- once the context has been running for any length of time.
CREATE INDEX idx_network_orders_unanswered
    ON network_orders (acknowledge_by)
    WHERE state = 'NEW';

-- Reverse lookup (local_order_id -> network order) for a future
-- PackageManifested-driven shipment-confirmation leg. NOTE: no code queries
-- this index today. ADR 0014 chose an explicit endpoint
-- (POST /network-orders/{networkRef}/shipment-confirmation, addressed by
-- networkRef) over correlating PackageManifested, because no persisted
-- WorkUnitId -> NetworkRef mapping exists; the index is kept for when it does.
CREATE INDEX idx_network_orders_local_order
    ON network_orders (local_order_id)
    WHERE local_order_id IS NOT NULL;

-- Lines already translated into OUR vocabulary. The network's product id
-- is retained ALONGSIDE the sku, not instead of it: acknowledgements and
-- shipment confirmations are addressed back in the network's terms, and
-- dropping it would make the outbound leg impossible without a reverse
-- lookup that may no longer agree.
--
-- Deliberately NO row requiring at least one line. An untranslatable
-- order is lineless by construction (ReceiveUntranslatable) and exists
-- precisely so the refusal is recorded rather than the demand dropped —
-- a NOT NULL-ish "must have lines" rule here would make that legitimate
-- case unstorable.
CREATE TABLE network_order_lines (
    network_ref        TEXT    NOT NULL
        REFERENCES network_orders (network_ref) ON DELETE CASCADE,
    -- Unique only WITHIN its order, which is why it is half a composite
    -- key rather than an id of its own.
    network_line_ref   TEXT    NOT NULL,
    network_product_id TEXT    NOT NULL,
    sku                TEXT    NOT NULL,
    quantity           INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (network_ref, network_line_ref)
);

CREATE INDEX idx_network_order_lines_sku ON network_order_lines (sku);
