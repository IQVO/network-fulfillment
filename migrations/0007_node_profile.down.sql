-- Reverses 0007_node_profile.up.sql. Destroys the persisted profile revisions and
-- the NODE_PROFILE ledger rows: after this, a live adapter would start again at
-- revision 1 and learn the network's real revision through the stale-revision
-- recovery of ADR 0027. Only for a context that never ran a profile declaration
-- against a real network.
DROP TABLE IF EXISTS network_node_profiles;
DELETE FROM network_submissions WHERE kind = 'NODE_PROFILE';
ALTER TABLE network_submissions DROP CONSTRAINT network_submissions_kind_check;
ALTER TABLE network_submissions ADD CONSTRAINT network_submissions_kind_check
    CHECK (kind IN ('ACKNOWLEDGEMENT', 'SHIPMENT_CONFIRMATION', 'NODE_REGISTRATION'));
