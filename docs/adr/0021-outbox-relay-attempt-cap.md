---
id: 0021-outbox-relay-attempt-cap
slug: /adr/0021-outbox-relay-attempt-cap
title: 21. Outbox relay attempt cap
sidebar_label: 21. Outbox relay attempt cap
description: "ADR 0021 — the outbox relay excludes a row after OUTBOX_MAX_ATTEMPTS failed sends (default 20) and logs it at ERROR, so one poison row cannot stop the relay forever; per-key ordering is traded for that."
---

# 21. Outbox relay attempt cap

## Status

**Accepted** (2026-10-13, ecosystem gap fix D12). Amends the per-key ordering
consequence of [ADR 0003](./0003-transactional-outbox.md); everything else in
ADR 0003 (one row per event and topic, written in the use case's transaction,
drained by one relay in `id` order) is unchanged.

## Context

The outbox relay ([ADR 0003](./0003-transactional-outbox.md),
`internal/adapters/outbound/postgres/outbox_relay.go`) claims unpublished rows
in `id` order and stops a pass at the first row the sink rejects, so a later
row never overtakes an earlier one. It had no attempt limit at all: a failed
row was retried on every pass, forever.

That is right for a broker outage, which ends. It is wrong for a **poison
row**, one the broker will never accept (a message over the broker's size
limit, a topic that cannot be created, a value the broker rejects every
time). Such a row fails every pass, every pass stops at it, and every row
behind it, for every network order and both topics
(`warehouse.network-fulfillment.events` and `.analytics`), waits forever.
Nothing pages: the process stays healthy and `/readyz` stays green.

The fleet decision for this gap (D12) is the convention inventory-storage
already has: a per-row attempt cap, the row kept in the table for inspection,
an ERROR log when it is excluded, and an operator re-arm.

## Decision

1. **The relay package gets the option.** `postgres.WithMaxAttempts(n)`: a row
   whose `attempts` has reached `n` is excluded from `claimOutboxRows`
   (`AND attempts < $2`). `0` means unlimited (the old behaviour). The relay's
   own zero value stays 0, so the cap is always an explicit choice at the
   composition root.
2. **Every composition root that builds a relay passes it.** `cmd/netfulfil`
   (the only binary here that runs a relay) reads `OUTBOX_MAX_ATTEMPTS` with
   `envInt` and passes `postgres.WithMaxAttempts`. Default `20`
   (`postgres.DefaultOutboxMaxAttempts`); `0` = unlimited. An invalid or
   negative value logs a WARN and uses the default, so a typo cannot silently
   remove the cap. The chart exposes it as `config.outboxMaxAttempts`
   (ConfigMap and the api Deployment's env).
3. **The row is kept, not moved.** An excluded row stays in `outbox_events`
   with `published_at IS NULL`, its `attempts` and its `last_error`. There is
   no DLQ topic: a row the broker rejects would most likely be rejected on a
   DLQ topic of the same broker too, and the table already is the inspectable
   store. This repository has no outbox sweeper, so nothing deletes it.
4. **Logging.** A failed send below the cap is logged at WARN
   (`outbox relay: send failed, stopping this pass at the failed row`, with
   `outbox_id`, `topic`, `event_type`, `attempts`, `error`). The failure that
   reaches the cap is logged once at ERROR
   (`outbox relay: row reached the attempt cap and is excluded from future claims; ...`)
   with `outbox_id`, `topic`, `event_type`, `key`, `attempts`, `max_attempts`
   and `error`, only after the attempt is committed. `Run` no longer logs
   these send failures a second time as `outbox relay pass failed`; that
   ERROR is now reserved for database failures of a pass.
5. **Re-arming is an operator action**: after fixing the cause,
   `UPDATE outbox_events SET attempts = 0 WHERE id = <id>;` makes the row
   claimable again on the next pass (runbook, "An outbox row hit the attempt
   cap").
6. **Sensor.** The architecture fitness test `TestOutboxRelaysPassAttemptCap`
   fails when a non-test file under `cmd/` calls `NewOutboxRelay` without a
   `WithMaxAttempts(...)` argument.

## Consequences

- **Per-key ordering is no longer absolute.** Once a row is excluded, later
  rows overtake it, **including rows with the same key** (the same
  `networkRef`): a consumer can see `NetworkOrderAcknowledged` for an order
  whose `NetworkOrderReceived` never arrived, or see a re-armed earlier event
  after a later one. This is the deliberate trade: the cap exists so one
  poison row cannot stop the whole relay, for every order, forever. Consumers
  already dedupe on the CloudEvents `id` and must tolerate redelivery
  ([ADR 0003](./0003-transactional-outbox.md),
  [ADR 0008](./0008-cloudevents-mandatory-event-envelope.md)); a re-armed row
  is a late delivery of the same `id`.
- Ordering among rows that are **not** excluded is unchanged: within a pass
  the relay still stops at the first failure, so a transient failure (below
  the cap) never lets a later row overtake.
- With the defaults (1 s `OUTBOX_RELAY_INTERVAL`, cap 20) a row that fails
  every send is excluded after roughly 20 seconds of continuous failure. A
  broker outage longer than that also excludes the rows at the head of the
  queue, one per pass, as each reaches the cap: each is logged at ERROR and
  must be re-armed. Raise `OUTBOX_MAX_ATTEMPTS` (or set `0`) where long broker
  outages are expected and a stalled relay is preferable to manual re-arming.
- Excluded rows are visible with
  `SELECT id, topic, event_type, convert_from(key, 'UTF8') AS key, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND attempts >= 20;`

## Verification

- `go test -tags=integration ./internal/adapters/outbound/postgres/ -run TestOutboxRelay -count=1`:
  `TestOutboxRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes`,
  `TestOutboxRelay_MaxAttempts_ReArmedRowIsClaimedAgain`,
  `TestOutboxRelay_NoCap_PoisonRowBlocksLaterRows` (testcontainers Postgres).
- `go test ./cmd/netfulfil/ -run 'TestOutboxMaxAttempts|TestDefaultOutboxMaxAttemptsIsTwenty|TestWireEventPublisher_OutboxRelayIsCapped'`.
- `go test ./internal/architecture/ -run OutboxRelay`.
- `python3 charts/network-fulfillment/tests/test_data_files.py`
  (`check_outbox_max_attempts_reaches_the_container`).
