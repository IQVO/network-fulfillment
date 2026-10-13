---
id: 0023-analytics-claim-and-apply-in-one-transaction
slug: /adr/0023-analytics-claim-and-apply-in-one-transaction
title: "23. The analytics consumer's dedupe claim and projection write commit in one transaction"
sidebar_label: "23. Analytics claim + apply atomic"
description: "ADR 0023 — AnalyticsConsumer runs ProcessedEvents.MarkProcessed and the projection apply inside one analytics-database transaction (analyticsstore.UnitOfWork), so a process stop between them can no longer lose a projection update; the claim+apply step is retried as one unit. Amends ADR 0004 §6."
---

# 23. The analytics consumer's dedupe claim and projection write commit in one transaction

## Status

**Accepted** (2026-10-13). Amends [ADR 0004](./0004-resilience-circuit-breakers-retry-dlq-shutdown.md)
§6 (the separate per-phase retries). No migration, no contract change.

## Context

`kafka.AnalyticsConsumer` (`cmd/netfulfil-projector`) dedupes every
CloudEvent on its `id` in `analytics_consumed_events`
(`ConsumedEventsRepo.MarkProcessed`) and then applies the projection
(`PostgresProjection.Apply*`, which claims the id again in
`analytics_processed_events` and upserts `acknowledgement_rollup` in its own
transaction). The two were separate calls, each with its own commit.

ADR 0004 §6 saw one trap in that shape: retrying the whole handler after a
successful mark and a failed apply would see `isNew=false` and skip the
apply. It retried the two phases separately to avoid it. It did not cover
the process stopping (SIGTERM, eviction, OOM, crash) after the mark had
committed and before the apply committed. The offset is not committed in
that case, so Kafka redelivers the message, but the redelivery's
`MarkProcessed` returns `isNew=false` and the apply is skipped. The
projection update is lost, with no error and no dead letter.

The fleet already avoids this. labor-performance's `RecordTaskPerformance`
runs `MarkProcessed` and its writes in one `UnitOfWork`, and the
analytics projections of inventory-storage, workforce-management and
fulfillment-execution claim the id inside the projection's own transaction.
This repository's OLTP side already has the same tool:
`postgres.UnitOfWork` carries a `pgx.Tx` in the context, and the stores
join it.

## Decision

1. **One analytics transaction.** A new `analyticsstore.UnitOfWork`
   (`internal/adapters/outbound/analyticsstore/unit_of_work.go`) mirrors
   `postgres.UnitOfWork` on the analytical pool: `Execute(ctx, fn)` begins a
   transaction, puts it in the context (a key private to `analyticsstore`,
   so an OLTP transaction can never be joined by mistake) and commits only
   when `fn` returns nil. A nested `Execute` joins the outer scope. Rollback
   runs on a context without cancellation, so a shutdown that interrupted
   `fn` still releases the transaction.
2. **The stores join it.** `ConsumedEventsRepo.MarkProcessed` issues its
   insert through the context's transaction when one is present.
   `PostgresProjection.inTx` uses the context's transaction instead of
   beginning its own; the UnitOfWork owns commit and rollback. Outside a
   UnitOfWork both behave as before.
3. **The consumer claims and applies as one unit.**
   `AnalyticsConsumer.claimAndApply` runs `MarkProcessed` and, for a new
   id, the projection apply inside `Atomic.Execute`. `handleMessage` retries
   that whole step (jittered backoff 100ms to 2s, `maxHandlerAttempts` = 3),
   which is now safe because every failed attempt rolled its claim back.
   This replaces ADR 0004 §6's two separate retry loops; the DLQ log's
   `phase` field is now `claim_and_apply`. `NewAnalyticsConsumer` takes the
   UnitOfWork, and `cmd/netfulfil-projector` passes
   `analyticsstore.NewUnitOfWork(pool)`. A consumer built without one (the
   in-memory unit-test fakes) runs the step directly.
4. **DLQ and shutdown rules are unchanged.** A message that is not a valid
   CloudEvent is dead-lettered at once. An exhausted retry budget is
   dead-lettered and committed. Cancellation is never dead-lettered: the
   offset stays uncommitted, `Run` returns nil, and the redelivery now
   applies the event because the interrupted transaction left no claim.

## Consequences

- A stop at any point between the claim and the projection write leaves
  either both rows or neither. The redelivery applies the event exactly
  once.
- The two idempotency layers (`analytics_consumed_events`,
  `analytics_processed_events`) now commit together. Both are kept: the
  projection's own claim still protects a direct `Apply*` caller.
- One transaction per message instead of two. No new table, env var, topic
  or metric.

## Verification

- `internal/adapters/inbound/kafka/analytics_atomic_claim_integration_test.go`,
  `TestAnalyticsConsumer_CrashBetweenClaimAndApply_RedeliveryAppliesExactlyOnce`
  (testcontainers Kafka and Postgres). The first consumer's projection
  cancels the run context on its first apply, which simulates the process
  stopping after the claim. The test then asserts that no
  `analytics_consumed_events` row is left. A second consumer in the same
  group gets the redelivered message, and the test asserts
  `orders_received = 1` plus exactly one row in each claim table. Before
  this change it failed on both assertions: the claim row was left behind
  and the rollup stayed at 0.
- `internal/adapters/outbound/analyticsstore/unit_of_work_integration_test.go`:
  rollback removes the claim, the projection claim and the rollup row
  together; commit keeps all three; a nested `Execute` joins.
- `internal/adapters/inbound/kafka/analytics_consumer_atomic_test.go`: the
  claim and the apply run in the same UnitOfWork scope; an apply or claim
  error fails the UnitOfWork.
