-- Reverses 0006_retail_network_map.up.sql. Destroys the identifier maps and the
-- submission ledger: after this, a live adapter could no longer reconcile what it
-- already sent. Only for a context that never ran NETWORK_MODE=live.
DROP TABLE IF EXISTS network_labels;
DROP TABLE IF EXISTS network_submissions;
DROP TABLE IF EXISTS network_purchase_order_lines;
DROP TABLE IF EXISTS network_purchase_orders;
DROP TABLE IF EXISTS network_nodes;
