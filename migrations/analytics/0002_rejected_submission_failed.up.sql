-- Count rejections whose reason is SUBMISSION_FAILED (the network's own
-- transaction-status reconciliation reported FAILURE for an acknowledgement
-- we had submitted). Before this column the projection claimed such events
-- but contributed to no counter, so they vanished from the report.
--
-- Additive only: existing rows read 0 for the new counter. Historical
-- SUBMISSION_FAILED events already claimed in analytics_processed_events
-- are not back-filled (the rollup holds counters, not events); rebuild the
-- read model by replaying the analytics topic if history matters.
ALTER TABLE acknowledgement_rollup
    ADD COLUMN orders_rejected_submission_failed BIGINT NOT NULL DEFAULT 0;
