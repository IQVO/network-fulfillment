-- CapabilityOffer (ADR 0001 §8): the latest recomputed advertisable
-- quantity per (sku, site). One row per pair, upserted on every
-- recompute pass — this is a replaceable snapshot, never a history.
CREATE TABLE capability_offers (
    sku                 TEXT        NOT NULL,
    site_id             TEXT        NOT NULL,
    advertised_quantity INTEGER     NOT NULL CHECK (advertised_quantity >= 0),
    basis               TEXT        NOT NULL
        CHECK (basis IN ('PHYSICAL', 'THROUGHPUT_CONSTRAINED')),
    computed_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sku, site_id)
);
