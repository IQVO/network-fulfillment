-- ADR 0027 (part 1): what the live retail-network adapter must remember to
-- declare a node's operational profile.
--
-- The profile revision is assigned by this service, per node, and must only ever
-- go up: a restart reads it back and continues above it, and no revision is ever
-- reused for different content. It is persisted here, before the first HTTP call
-- that carries it, by a guarded write (see PGStore.AdvanceNodeProfile).
--
-- Like the tables of 0006 this one belongs to the adapter
-- (internal/adapters/outbound/retailnetwork). In NETWORK_MODE=stub it stays empty.

-- The profile writes go through the same submission ledger as every other write
-- (kind NODE_PROFILE, subject <site>#<revision>#<attempt>), so the kind check
-- admits it.
ALTER TABLE network_submissions DROP CONSTRAINT network_submissions_kind_check;
ALTER TABLE network_submissions ADD CONSTRAINT network_submissions_kind_check
    CHECK (kind IN ('ACKNOWLEDGEMENT', 'SHIPMENT_CONFIRMATION', 'NODE_REGISTRATION', 'NODE_PROFILE'));

-- One row per node: the revision in force, which attempt at that revision is the
-- current one, the fingerprint of the content that revision carries, and that
-- content in the fleet's own terms (the declared ceilings are read back from it).
CREATE TABLE network_node_profiles (
    site_code   TEXT        PRIMARY KEY
        REFERENCES network_nodes (site_code),
    revision    INTEGER     NOT NULL CHECK (revision >= 1),
    attempt     INTEGER     NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    fingerprint TEXT        NOT NULL CHECK (fingerprint <> ''),
    content     JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
