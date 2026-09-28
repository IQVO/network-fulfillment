---
id: 0003-transactional-outbox
slug: /adr/0003-transactional-outbox
title: 3. Transactional outbox for the network-fulfillment Published Language
sidebar_label: 3. Transactional outbox
description: "ADR 0003 — why saving a NetworkOrder and publishing its domain event(s) to Kafka now commit in one Postgres transaction (only when DATABASE_URL is configured), with a background relay draining the outbox to both the integration and analytics topics."
---

# 3. Transactional outbox for the network-fulfillment Published Language

## Status

Accepted — implemented in the same change that introduced this record.

## Context

`ReceiveNetworkDemand` and `SweepAcknowledgementDeadlines` both follow the
same shape wherever they answer the network:

```go
if err := uc.Orders.Save(ctx, o); err != nil { return err }
if err := uc.Events.Publish(ctx, event); err != nil { return err }
```

Two independent writes to two independent systems (Postgres, Kafka), no
compensation. A crash, a broker timeout, or a pod eviction between them
leaves a `NetworkOrder` recorded in the store with no corresponding event
ever reaching the topic — invisible to every downstream analytics
consumer, and to any future integration consumer of this context's
Published Language. The reverse failure (Publish succeeds, the process
dies before the HTTP/poll response completes) is already covered by this
context's own idempotency (`FindByRef` short-circuits a repeat poll), but
the first failure had no answer at all.

`process-path-management` found and fixed the identical problem in its
own ADR 0003, and four sibling services (`wes-work-planning`,
`fulfillment-execution`, `workforce-management`, `labor-performance`)
have since adopted the same pattern. This record adapts it to two things
specific to this context:

1. **Two topics, not one.** `cmd/netfulfil`'s `fanOutPublisher` already
   fans every event out to both the integration topic
   (`warehouse.network-fulfillment.events`) and the analytics topic
   (`warehouse.network-fulfillment.analytics`). Both must be enqueued
   atomically with the aggregate write, or the two streams can drift
   from each other exactly as easily as either can drift from the store.
2. **No Postgres by default.** ADR 0001 §4 requires this service to run
   fully in-memory with a stub network gateway when no configuration is
   given at all. The outbox cannot become a hard dependency; it must
   activate only in the existing `DATABASE_URL`-configured branch of the
   composition root, exactly mirroring the mode matrix that already
   exists for `NetworkOrderRepo`.

## Decision

Adopt the **transactional outbox** pattern, gated behind `DATABASE_URL`:

1. **`outbox_events` table** (migration `0002_outbox`). One row is one
   already-encoded Kafka message for one topic: `topic`, `event_type`,
   `key`, `value` (the envelope bytes, byte-for-byte what the topic will
   carry), `headers` (JSONB, for future trace propagation), plus
   `created_at`, `published_at`, `attempts`, `last_error` for the relay.
   One row per (event × topic) — not one row per event — because this
   context fans each event out to two topics with two different envelope
   shapes (`Envelope` vs `AnalyticsEnvelope`). A partial index over
   `published_at IS NULL` keeps the relay's scan tiny.

2. **`ports.UnitOfWork`** — a new driven port,
   `Execute(ctx, fn func(ctx) error) error`. `ReceiveNetworkDemand` and
   `SweepAcknowledgementDeadlines` wrap every `Orders.Save` +
   `Events.Publish` pair in one `atomically(ctx, uc.UnitOfWork, fn)` call
   (a small helper in the usecases package). The port is optional — `nil`
   means "run the pair back to back", which is exactly the in-memory /
   log-publisher / direct-Kafka configuration this context already
   supports. The domain and application layers gain no knowledge of
   transactions, Postgres, or Kafka; the arch-go fitness tests are
   unchanged.

   `ReceiveNetworkDemand` opens **two** such scopes per call (one for the
   `NetworkOrderReceived` publish at intake, one for the terminal
   `Acknowledged`/`Rejected` publish), not one — the use case already had
   two logically separate points where an aggregate save and its event
   must agree, and collapsing them into a single wider scope would change
   its documented ordering (translate → save+publish received → ask
   order-management → save+publish the answer → tell the network → hold
   commit/cancel).

3. **`postgres.UnitOfWork`** opens a `pgx.Tx`, binds it to the context,
   and commits or rolls back around `fn`. `NetworkOrderRepo.Save` (which
   already opened its own transaction for the order + its lines) and the
   new `OutboxPublisher` both resolve their querier from the context via
   `querierFrom`/`beginOrJoin`: the pool when standalone, the bound
   transaction when inside a unit of work. A nested `Execute` call joins
   the outer transaction rather than opening a second one.

4. **`postgres.OutboxPublisher`** implements `ports.EventPublisher`
   (`Publish(ctx, event any) error` — this context's existing signature,
   kept as-is rather than made variadic). For each configured
   `kafka.Encoder` (the integration `Publisher`, the `AnalyticsPublisher`)
   it calls `Encode` and `INSERT`s one row per encoded message. It never
   touches the broker, and the envelope bytes are produced by the exact
   same `Encode` the direct Kafka publishers use, so the outbox and the
   direct (no-`DATABASE_URL`) path can never disagree on wire format.

5. **`postgres.OutboxRelay`** runs as a goroutine inside the `netfulfil`
   process, next to the HTTP server, sweep and poller. Each pass claims
   up to `batchSize` (default 100) pending rows with
   `SELECT ... FOR UPDATE SKIP LOCKED ORDER BY id`, sends them to a Sink
   (either Kafka publisher's `Send`, which stamps each row's own `topic`
   per message) in order, and marks each `published_at`. On a send
   failure it stops the pass at that row — so a later event for the same
   aggregate can never overtake a failed earlier one — records the error
   on the row, commits what was already sent, and retries on the next
   tick. Sleep between empty passes is `OUTBOX_RELAY_INTERVAL` (default
   `1s`); a full batch is followed immediately by another pass.

6. **Composition root** (`cmd/netfulfil/main.go`) extends the existing
   mode matrix:

   | `DATABASE_URL` | `EVENT_PUBLISHER` | Publisher wired | Relay |
   |----------------|--------------------|-------------------------------|-------|
   | unset | unset (default) | log | none |
   | unset | `kafka` | direct fan-out (integration + analytics) | none |
   | set | unset (default) | log | none |
   | set | `kafka` | **outbox** (both topics enqueued atomically) | **yes** |

   The mode is logged at startup: `"event publisher configured"
   publisher=kafka mode=outbox|direct`. Graceful shutdown stops the HTTP
   server first, then cancels the relay and waits (bounded by the
   shutdown deadline) for its in-flight pass, so an event committed by a
   request that completed just before SIGTERM is not stranded until the
   next pod boots.

### Delivery semantics (what consumers may now rely on)

- **Atomicity**: a `NetworkOrder` change and its event(s), for BOTH
  topics, commit together or not at all. Verified by an integration test
  that forces the outbox insert to fail and asserts the `network_orders`
  row is absent.
- **At-least-once**: a crash between a successful `Send` and the row's
  `UPDATE` republishes that row on the next pass. Consumers must be
  prepared to see a duplicate keyed by the same aggregate ref.
- **Per-aggregate ordering**: preserved. Rows are drained in insertion
  (id) order within one relay pass, keyed by `NetworkRef`, and a failed
  row blocks everything behind it rather than being skipped.
- **Latency**: events reach the topics within one relay interval (≤1s by
  default) of the transaction committing, versus "before the HTTP/poll
  response returns" under the old direct-publish path.

## Consequences

**Positive**
- The store and both Kafka topics can no longer diverge for a reason
  this context's own code introduces.
- No broker dependency on the request/poll path in outbox mode: the
  aggregate write succeeds even if Kafka is briefly unreachable; the
  event is published once the relay catches up.
- Reusable directly by any future outbound publisher this context adds:
  only a new `kafka.Encoder` is needed, not a new outbox mechanism.
- `EVENT_PUBLISHER` and `DATABASE_URL` remain independently toggleable,
  so every existing local/dev/CI configuration (fully in-memory, Kafka
  without Postgres) keeps working unchanged.

**Negative / accepted**
- One more table, one more goroutine, one more failure mode (the relay)
  to observe. The relay logs every failed pass at ERROR with the row id,
  topic and broker error; `outbox_events.attempts`/`last_error` are
  queryable for operators.
- Events are no longer synchronous with the request/poll cycle in outbox
  mode. Acceptable: this context's own SLA is the 24h acknowledgement
  deadline to the *network*, not sub-second Kafka propagation.
- With two pods overlapping during a rolling deploy, `SKIP LOCKED`
  guarantees no double-claim within one pass but does not guarantee
  global cross-pod ordering for different aggregates — only per-aggregate
  ordering is preserved, which is what consumers actually depend on.
- `EVENT_PUBLISHER=kafka` without `DATABASE_URL` still publishes both
  topics directly, with no outbox. That mode exists only for in-memory
  local runs and is logged as `mode=direct`.

## Alternatives considered

- **Keep Save-then-Publish and add retry around Publish.** Does not fix
  a crash between the two writes, and a retry loop on the request/poll
  path makes the broker's latency this context's own SLA. Rejected.
- **Publish-then-Save.** Inverts the failure into an event for a
  `NetworkOrder` that never persisted — strictly worse. Rejected.
- **A single outbox row per event, re-derive both topics' encodings at
  relay time.** Would require the relay to hold both `Encoder`s and
  duplicate the fan-out decision outside the transaction; the chosen
  one-row-per-(event×topic) design instead keeps every encoded payload
  exactly as it was produced inside the original transaction, with no
  re-encoding step that could drift from what was actually committed.
  Rejected.
- **Change-data-capture (Debezium) on `network_orders`.** Correct, but
  adds a Kafka Connect deployment this context has no other reason to
  run, and moves envelope encoding out of the service's own code where
  ADR 0001's Anti-Corruption Layer wants the Published Language to live.
  Deferred.

## Verification

- Unit: `internal/application/usecases/unit_of_work_test.go` — both
  `ReceiveNetworkDemand`'s two publish points and
  `SweepAcknowledgementDeadlines`' single one run inside exactly one
  scope each, a publish failure rolls back only that scope (verified
  against `f.planner.calls` and the gateway to prove nothing downstream
  of a rolled-back scope ran), a `UnitOfWork.Execute` begin failure
  propagates without publishing, and a nil `UnitOfWork` still saves and
  publishes exactly as before this change.
- Integration (`-tags=integration`, testcontainers Postgres — the test
  owns its own throwaway database, never an external `DATABASE_URL`):
  `outbox_integration_test.go` — commit-together across both topics,
  rollback-on-encode-failure, relay ordering + marking with a no-op
  second pass, relay stops at a failed row and recovers on retry.
- `make check` (fmt-check, vet, build, `golangci-lint`, full test suite
  with `-race`) and `make arch-test` both pass unchanged.

## Rollout

Ships in this single change (`feature/transactional-outbox`): the port,
adapters, migration, use-case wiring, composition-root wiring, this
record, and both test suites above. No further phased rollout — unlike
ADR 0001's own multi-step rollout, this record does not introduce a new
domain concept, only a delivery guarantee around events already being
published.
