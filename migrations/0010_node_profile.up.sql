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
--
-- Numbering: this migration was written as 0007 and renumbered to 0010 when the
-- inbound loop (0008, 0009) reached develop first. golang-migrate never runs a
-- file numbered below a database's current version, so a 0007 would have been
-- skipped on every database already at version 8 or 9.

-- The profile writes go through the same submission ledger as every other write
-- (kind NODE_PROFILE, subject <site>#<revision>#<attempt>). The ledger's kind check
-- already admits NODE_PROFILE: 0008 widened it to the five kinds, on purpose, so
-- that the order of the two ADRs' migrations would not matter. This migration must
-- NOT replace that constraint: re-creating the four-kind list it had as 0007 would
-- drop INBOUND_RECEIPT from it (or fail on a ledger that already holds such a row).

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
