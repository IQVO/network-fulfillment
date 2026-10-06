---
id: 0016-cloudevents-submitted-and-settle-time-acknowledged
slug: /adr/0016-cloudevents-submitted-and-settle-time-acknowledged
title: "16. NetworkOrderSubmitted at SUBMITTED; NetworkOrderAcknowledged only when the order settles (v2)"
sidebar_label: "16. Submitted + settle-time Acknowledged"
sidebar_position: 16
description: "ADR 0016 adds the NetworkOrderSubmitted event (raised when the order moves to SUBMITTED) and moves NetworkOrderAcknowledged to the SUBMITTED -> ACKNOWLEDGED settle, published as the breaking-change v2 type and dataschema per the fleet CloudEvents standard. The analytics projector keeps replaying historic v1 messages with unchanged report numbers."
---

# 16. `NetworkOrderSubmitted` at SUBMITTED; `NetworkOrderAcknowledged` only when the order settles (v2)

## Status

Accepted (2026-10-06). Resolves the "`NetworkOrderAcknowledged` timing" design
note that [ADR 0015](./0015-docs-audit-contract-corrections.md) deliberately
left undecided. Builds on [ADR 0001](./0001-network-fulfillment-bounded-context.md) §5
(the `SUBMITTED` state), [ADR 0003](./0003-transactional-outbox.md) (outbox)
and [ADR 0008](./0008-cloudevents-mandatory-event-envelope.md) (CloudEvents,
versioning). Amends the event contract only; no REST or MCP contract changes.

## Context

`ReceiveNetworkDemand` published `NetworkOrderAcknowledged` the moment the
order moved to `SUBMITTED`, i.e. before `ReconcileSubmittedOrders` had asked
the network's transaction-status record whether the submission was accepted.
The later `SUBMITTED -> ACKNOWLEDGED` settle raised no event, and a refused
submission raised `NetworkOrderRejected(SUBMISSION_FAILED)` for an order that
had already been announced as acknowledged.

In the ubiquitous language an event named "Acknowledged" that fires before
acknowledgement is a lie: `ACKNOWLEDGED` is defined as a *settled* commitment,
and AGENTS.md rule 8 releases work only after the settle. The fleet had no
consumer of `warehouse.network-fulfillment.events` (checked again on 2026-10-06
against every other repository's `develop`/`main`: only the topic names appear,
in `warehouse-infra` documentation), so this was the cheapest moment to fix it.

## Decision

1. **New fact `NetworkOrderSubmitted`** is raised in the same atomic
   `Save` + `Publish` scope that moves the order to `SUBMITTED`
   (`ReceiveNetworkDemand.acknowledge`). It is exactly what the old
   `NetworkOrderAcknowledged` announced and carries the same payload
   (`networkRef`, `siteId`, `localOrderId`, `receivedAt`, `at`).
   It is new, therefore **v1**: type
   `com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderSubmitted`,
   dataschema `urn:warehouse:network-fulfillment:<events|analytics>:NetworkOrderSubmitted:v1`.
2. **`NetworkOrderAcknowledged` is raised only when the order settles
   `ACKNOWLEDGED`.** `ReconcileSubmittedOrders.confirm` saves the aggregate as
   `ACKNOWLEDGED` and publishes the event in one atomic scope, *before* the
   held order is released. A pending submission raises nothing; a failed one
   raises `NetworkOrderRejected(SUBMISSION_FAILED)` only (never an
   Acknowledged first). The payload field set is unchanged; `at` is the settle
   time and `receivedAt` is still the original receipt.
3. **The changed meaning is a breaking contract change, versioned exactly as
   the fleet CloudEvents standard (§4) prescribes:** a *new type with a `.v2`
   suffix* and a *new dataschema version*, published as a new event and never by
   mutating the old one:
   - `type`: `com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderAcknowledged.v2`
   - `dataschema`: `urn:warehouse:network-fulfillment:<events|analytics>:NetworkOrderAcknowledged:v2`

   On both topics, through the outbox (the outbox persists the encoded v2
   bytes and `event_type`). Implemented by `cloudevents.TypeVersioned` (suffix
   for versions >= 2) and the publisher's per-event version. The unsuffixed
   `NetworkOrderAcknowledged` v1 type is **no longer published**; v1 messages
   already on a topic are never rewritten.
4. **The analytics projector handles all three.** `NetworkOrderSubmitted` is a
   recognised type: it is claimed on its CloudEvents `id` (so redelivery is a
   skip) and has **no report effect**. `NetworkOrderAcknowledged` v2 and the
   historic v1 both feed the existing acknowledgement counter and latency
   (`at - receivedAt`) with the same payload fields, so replaying the topic from
   the first offset keeps giving the same numbers for everything published
   before this change (v1 meant "submitted", and it still counts once, with the
   same latency). For new orders the counter now moves at the settle and the
   latency measures receipt -> settle, which is what the report always claimed
   to measure.
5. **Not added:** a submit -> settle measure (an `ordersSubmitted` counter or a
   submitted-to-settled latency). It needs a persisted `submittedAt` on the
   aggregate or a new report column plus REST/MCP/OpenAPI surface, which is not
   trivial or additive in this change. `NetworkOrderSubmitted` is now on the
   topic, so it can be added later without a contract change.

## Consequences

**Easier:** event names state facts; consumers can tell "told the network yes"
(`Submitted`) from "the network confirmed it" (`Acknowledged`); a refused
submission no longer follows an `Acknowledged` event.

**Harder / to watch:**
- Event count per accepted order is now three (`Received`, `Submitted`,
  `Acknowledged.v2`) instead of two.
- Any external consumer that dispatched on the v1 `NetworkOrderAcknowledged`
  type stops receiving it. None exists in the fleet (grep of every other
  repository, 2026-10-06); a future consumer must dispatch on the `.v2` type
  (and `Submitted` if it wants the earlier fact).
- The AsyncAPI mirror in `warehouse-docs` (`apis/network-fulfillment/asyncapi.yaml`)
  must be re-synced from this repository's `apis/asyncapi.yaml`.
- The v1 type remains in the projector's dispatch table permanently, because the
  analytics topic is a replayable read-model source.
