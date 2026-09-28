---
id: 0005-kafka-hash-balancer-for-partition-affinity
slug: /adr/0005-kafka-hash-balancer-for-partition-affinity
title: "5. Hash balancer for per-aggregate Kafka partition affinity"
sidebar_label: "5. Kafka Hash balancer"
sidebar_position: 5
description: "ADR 0005 — every outbound Kafka Writer in this repo used kafka-go's LeastBytes balancer, which ignores Message.Key entirely when choosing a partition. Message.Key was already set correctly (NetworkRef); the fix switches every Writer's Balancer to kafka-go's Hash so same-key messages actually land on the same partition, restoring per-NetworkRef ordering on the 8-partition topics warehouse-infra PR #42 created."
---

# 5. Hash balancer for per-aggregate Kafka partition affinity

## Status

Accepted — implemented in the same change that introduced this record.
Mirrors `order-management`'s ADR 0027 / PR #111, which found and fixed
the identical bug in that repo first; `inventory-storage` (PR #102) and
`workforce-management` (PR #106) also carried and fixed the same bug.
This record documents network-fulfillment's own instance and fix.

## Context

`internal/adapters/outbound/kafka/publisher.go`'s `NewPublisher` and
`analytics_publisher.go`'s `NewAnalyticsPublisher` both construct a
`*kafkago.Writer` with `Balancer: &kafkago.LeastBytes{}`. Both
publishers' `Encode` methods already set `Message.Key` correctly — to
`aggregateKey(event)`, which returns the raising `NetworkOrder`'s
`NetworkRef` for all four published event types — so on paper every
event for one `NetworkRef` looked like it should land on one partition,
preserving that aggregate's relative event order for any consumer.

That guarantee was never actually real. `kafka-go`'s `LeastBytes`
balancer picks the partition with the least cumulative bytes written so
far; it reads `len(msg.Key) + len(msg.Value)` only to update that
running total, and never hashes or otherwise routes on the key's
content. Setting a correct, non-nil `Message.Key` achieves nothing for
partition affinity under `LeastBytes` — two messages with an identical
key can and do land on different partitions. This is the opposite of
real Kafka's own default partitioner (a hash of the key) and of
`kafka-go`'s own `Hash`/`ReferenceHash`/`CRC32Balancer` types, but
`kafka-go`'s `Writer` does not switch to one of those automatically just
because a message happens to carry a key — `Balancer` is a fully
separate, independently wrong knob.

This was invisible for as long as `warehouse.network-fulfillment.events`
and `warehouse.network-fulfillment.analytics` ran at 1 partition each
(total order is preserved by construction with a single partition, so
the missing key-aware balancer had no observable effect). `warehouse-
infra` PR #42's Phase 3 scaleup took every business topic in the fleet,
including both of this repo's topics, from 1 to 8 partitions. From that
point on a consumer of either topic could observe, for example, a
`NetworkOrderShipmentConfirmed` before the `NetworkOrderReceived` for
the SAME `NetworkRef`, if the two messages happened to land on different
partitions — silently, with no error, no test failure under the
existing fake-writer unit tests (`publisher_test.go`,
`analytics_publisher_test.go`), because those tests assert `msg.Key ==
expectedKey`, which is necessary but not sufficient: it proves the key
is SET, not that the `Writer`'s `Balancer` actually USES it to choose a
partition. Only a real broker's partition-assignment logic exercises
that second, independently-breakable behaviour.

This repo's DLQ writer (`internal/adapters/inbound/kafka/
analytics_consumer.go`'s `AnalyticsConsumer.dlqWriter`, added in Phase 2
PR #21 / ADR 0004) has the same shape of gap: `dlqPublish` forwards
`msg.Key` (the same `NetworkRef` the source message carried) onto the
`.dlq` topic, but the writer left `Balancer` unset, which `kafka-go`
defaults to `RoundRobin` — a plain round-robin balancer that also
ignores `Key` entirely (see `kafka-go`'s `Writer.Config`).

## Decision

Every `*kafkago.Writer` in this repo's outbound Kafka adapters now sets
`Balancer: &kafkago.Hash{}` (FNV-1a over `Message.Key`, `kafka-go`'s
Sarama-hash-partitioner-compatible balancer):

- `internal/adapters/outbound/kafka/publisher.go`'s `NewPublisher` —
  the integration-topic writer, also reused as the transactional
  outbox relay's `Sink` (ADR 0003).
- `internal/adapters/outbound/kafka/analytics_publisher.go`'s
  `NewAnalyticsPublisher` — the analytics-topic writer.
- `internal/adapters/inbound/kafka/analytics_consumer.go`'s
  `dlqWriter` — the dead-letter writer.

No `Message.Key`-setting logic changed: `aggregateKey` in
`publisher.go` (shared by both publishers) already returns the correct
per-`NetworkRef` key for every event type that carries one, and the DLQ
path already forwards the source message's own key unmodified. This fix
is scoped entirely to the `Balancer` field — the knob that decides
whether a set key has any partition-routing effect at all.

**On the DLQ writer specifically**: dead-letter messages are consumed by
a manual replay tool, not by an ordering-sensitive live consumer, so
strict inter-aggregate ordering across the `.dlq` topic is not itself a
correctness requirement the way it is for the two live topics. `Hash`
was chosen anyway, for two reasons: (1) consistency — every writer in
this package uses the same balancer, so there is one behaviour to reason
about, not two; and (2) it gives the replay tool the same aggregate-
grouped-per-partition property the live topics get, meaning a poison
`NetworkRef`'s dead-lettered events stay together on one partition and,
read from that partition in offset order, come back out in their
original relative order — useful if the eventual replay tool cares
about that, and free to have even though nothing currently depends on
it. `RoundRobin` (the pre-fix default) would have scattered one
aggregate's dead letters across all of `.dlq`'s partitions despite the
key being set, for no benefit.

## Consequences

- Restores the per-`NetworkRef` partition affinity the code already
  looked like it had: every event for one `NetworkRef`, on either the
  integration or analytics topic, now provably lands on the same
  partition, so a consumer can never observe them out of relative
  order — the actual guarantee `warehouse-infra` PR #42's 1→8 partition
  scaleup was assumed (incorrectly, until this fix) to preserve.
- No wire-format or consumer-visible change: `Message.Key` was already
  being set and transmitted; only the producer-side partition
  ASSIGNMENT changes. Existing consumers (including any already
  running against the 8-partition topics) are unaffected other than
  now actually getting per-aggregate ordering.
- A fake-writer unit test asserting `msg.Key == expectedKey` is proven
  (again, fleet-wide) to be necessary but not sufficient for this class
  of bug — it cannot observe the `Writer`'s partition-assignment
  decision at all. `publisher_integration_test.go`'s new
  `TestPublisherKeysMessagesForSameNetworkRefOntoTheSamePartition` fills
  that gap: it runs a real `confluentinc/confluent-local` broker via
  Testcontainers, creates an 8-partition topic, publishes several events
  for the same `NetworkRef` plus one for a different `NetworkRef`, and
  asserts all of the first land on one partition. This test was run
  against both balancers during development: it FAILED under
  `LeastBytes` (1 of 3 same-key messages landed on the expected
  partition) and PASSED under `Hash` (3 of 3) — direct evidence this
  test class, not code inspection, is what actually catches the bug.
- Any future `kafkago.Writer{...}` literal added to this repo must set
  `Balancer: &kafkago.Hash{}` (or another key-aware balancer) from the
  start if the messages it carries have a meaningful `Key` — copying an
  existing writer literal without this field, or with `LeastBytes`, is
  the exact mistake this ADR fixes.

## References

- `order-management` PR #111 / ADR 0027 — first fix in the fleet for
  this exact bug class (the integration publisher there).
- `warehouse-infra` PR #42 — the topic partition scaleup (1→8) that
  turned this from a latent code-inspection finding into a real,
  observable ordering hazard.
- ADR 0004 (`0004-resilience-circuit-breakers-retry-dlq-shutdown.md`) —
  introduced the DLQ writer this record also fixes.
- ADR 0003 (`0003-transactional-outbox.md`) — the outbox relay that
  reuses `Publisher`'s writer as its `Sink`.
