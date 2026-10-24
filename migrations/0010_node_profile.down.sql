-- Reverses 0010_node_profile.up.sql. Destroys the persisted profile revisions and
-- the NODE_PROFILE ledger rows: after this, a live adapter would start again at
-- revision 1 and learn the network's real revision through the stale-revision
-- recovery of ADR 0027. Only for a context that never ran a profile declaration
-- against a real network.
--
-- The ledger's kind check is left alone: it belongs to 0008, which admits
-- NODE_PROFILE and INBOUND_RECEIPT both, and the INBOUND_RECEIPT rows must survive.
DROP TABLE IF EXISTS network_node_profiles;
DELETE FROM network_submissions WHERE kind = 'NODE_PROFILE';
