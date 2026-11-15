---
id: 0014-explicit-shipment-confirmation-endpoint
slug: /adr/0014-explicit-shipment-confirmation-endpoint
title: "14. Shipment confirmation is an explicit endpoint, not a PackageManifested correlation"
sidebar_label: "14. Explicit shipment confirmation"
sidebar_position: 14
description: "ADR 0014 amends ADR 0001 Rollout step 6: NetworkOrderShipmentConfirmed is raised by an explicit ConfirmNetworkOrderShipment use case/endpoint naming the NetworkRef directly, not by correlating fulfillment-execution's PackageManifested event, because no persisted NetworkRef<->WorkUnitId mapping exists yet and ADR 0001 Hard rule 4 forbids inventing a string-convention join."
---

# 14. Shipment confirmation is an explicit endpoint, not a PackageManifested correlation

## Status

Accepted (2026-10). Amends [ADR 0001](./0001-network-fulfillment-bounded-context.md)'s
Rollout step 6, which named the trigger as "driven by
`fulfillment-execution`'s existing `PackageManifested` event". Also adds
this context's first and only write endpoint, narrowing Hard rule 9
("REST is read-only... no write endpoint without a new ADR") for this
one case only.

Addendum (2026-11-06): the `ShipmentConfirmed` consumer is the primary
trigger; the explicit endpoint stays one release as the operator fallback,
both through one idempotent use case — see the Addendum section below.

## Context

`NetworkOrderShipmentConfirmed` (`internal/domain/shared/events.go`) and
`NetworkOrder.ConfirmShipment()` (`internal/domain/networkorder/network_order.go`)
have existed since this context's skeleton, but nothing called
`ConfirmShipment()` — ADR 0001's own Rollout step 6 sketched
`fulfillment-execution`'s `PackageManifested` event as the trigger,
implying a Kafka cache correlating that event's `WorkUnitId` back to this
context's `NetworkRef`.

That correlation does not exist, and building it as a same-moment string
join would be exactly the mistake ADR 0001's own **Hard rule 4**
("Correlation is a persisted mapping, never a string convention") was
written to prevent: `NetworkOrder.localOrderId` already IS the persisted
mapping to order-management's `OrderId`, but order-management's `OrderId`
and `fulfillment-execution`'s `WorkUnitId` are not the same identifier and
no table anywhere stores the second hop. Consuming `PackageManifested` and
guessing which `NetworkOrder` it closes (by `WorkUnitId`, by SKU+site+time
proximity, or any other heuristic) would be fragile by construction and
silently wrong under the first SKU collision or re-manifest.

## Decision

`ConfirmNetworkOrderShipment` (`internal/application/usecases/confirm_network_order_shipment.go`)
is a use case that takes a `NetworkRef` directly — no event correlation,
no heuristic matching — loads the `NetworkOrder`, calls
`ConfirmShipment()`, persists and publishes
`NetworkOrderShipmentConfirmed` in one atomic scope (`UnitOfWork`, ADR
0003), then submits the confirmation to the network gateway. It is
idempotent: an order already `CONFIRMED` is returned as-is rather than
re-confirmed, so a retried call never double-submits.

It is exposed as this context's first and only write endpoint:
`POST /network-orders/{networkRef}/shipment-confirmation` (empty body,
`204 No Content` on success). This deliberately narrows Hard rule 9 for
this one case — the rule's reasoning (no fictional inbound path; demand
itself only ever arrives by polling) does not apply here, because
shipment confirmation is not inbound *demand*, it is an outbound-facing
*fact this context is told* once fulfilment genuinely completes
elsewhere in the fleet (today: an operator or a script with the
`NetworkRef` in hand; the natural future caller is whatever process
eventually DOES persist the `NetworkRef`<->`WorkUnitId` mapping and can
therefore call this endpoint reliably instead of guessing from a Kafka
event).

A future `PackageManifested`-driven trigger remains straightforward to
add without touching `ConfirmNetworkOrderShipment` itself: once a
persisted `NetworkRef`<->`WorkUnitId` mapping exists (its own ADR, when
that correlation is actually built), a consumer calls this same use case
instead of a human or script calling the endpoint. This ADR picks the
correct trigger for what exists TODAY, not a trigger that requires
infrastructure this codebase does not have yet.

## Consequences

Easier: `NetworkOrderShipmentConfirmed` is now actually raised end to end
(provable by test), closing the one open gap ADR 0001's own rollout left
dangling; the endpoint is trivial to call from an operator runbook or a
script during the gap before a real correlation exists. Harder: shipment
confirmation now depends on SOMETHING outside this context knowing the
`NetworkRef` and choosing to call this endpoint — there is no automatic
trigger today, which is an honest reflection of the missing correlation,
not a regression hidden behind a plausible-looking but wrong automatic
one. The endpoint is unauthenticated like every surface in this fleet
(ADR 0001 §Decision 3), so anything able to reach it can confirm shipment
for any order; this is acceptable under the fleet's current flat-trust
network model and is the same exposure every other context's write
endpoints already carry.

## Addendum (2026-11-06): the event path is primary; the endpoint stays one release

The correlation this ADR said was missing now exists — but not the one its
Context paragraph guessed at. The fleet built `outbound-shipping`, and its
`ShipmentConfirmed` event (merged contract, outbound-shipping PR #2) lists
every shipped order as `orders[].order_id`, which IS order-management's
`OrderId` — exactly the identifier `NetworkOrder.localOrderId` has persisted
since the demand loop (the mapping this ADR's own Context named as "the
persisted mapping ... but no table anywhere stores the second hop": the
second hop is now outbound-shipping's, not a WorkUnitId join). This ADR's
Hard rule 4 reasoning holds untouched: no string convention anywhere, the
join is on a stored column.

**Decision (fleet plan D6, outbound-shipping-2026-11-05):**

1. **Primary path — consume the event.** `ShipmentConfirmedConsumer`
   (`internal/adapters/inbound/kafka/shipment_confirmed_consumer.go`)
   subscribes to `warehouse.outbound-shipping.events` under
   `OUTBOUND_SHIPPING_CONSUMER_GROUP` (default
   `network-fulfillment-outbound-shipping`), joins each
   `orders[].order_id` on `NetworkOrder.localOrderId`
   (`FindByLocalOrderId`), and confirms through the ONE use case. An
   `order_id` no NetworkOrder carries is a local order raised from
   customer demand sharing the load: skipped and logged, never an error.
   Unknown types are ignored; a message that is not a CloudEvent or breaks
   the contract is dead-lettered to `warehouse.outbound-shipping.events.dlq`.
2. **The manual POST stays, for exactly one release, as the operator
   fallback** — also the operator's way to close the gap a dead-lettered
   confirmation leaves. It deprecates when the event path has run clean in
   the fleet (its own ADR then).
3. **Neither path double-submits.** Both route through the one
   `ConfirmNetworkOrderShipment` (one instance in the composition root),
   whose idempotency is now two-legged: the aggregate's terminal CONFIRMED
   state (as before), plus a durable `(shipment reference, order id)`
   ledger (`shipment_confirmations`, migration 0014) — the load id and the
   order-management OrderId on the event path, the NetworkRef itself on
   the manual path. The ledger row commits in the same transaction as the
   CONFIRMED save and the `NetworkOrderShipmentConfirmed` outbox write, so
   a redelivered event, a re-published load, or the endpoint racing the
   event is a no-op returning the existing confirmation: one label request,
   one network submission, one event, proven by test in both orders
   (`confirm_shipment_paths_test.go`) and on the real stack
   (`shipment_confirmed_integration_test.go`).

**Known gap — package count.** The network is told `1` package per order
(`const singlePackage`, ADR 0026) because `ShipmentConfirmed.orders[]`
carries only `order_id` today; the per-order package count is an additive
producer change that has not landed. The consumer's decode already
tolerates `package_count` and the use case threads it to `RequestLabel`,
so the real count flows the day the producer ships it — until then the
single-package behavior stands, and the spike's evidence (one package per
order is structural today) keeps that honest.


## Addendum D7 (2026-11-14): a confirmation that arrives before the acknowledgement is deferred, not dead-lettered

**The defect.** The Primary path above says the consumer "confirms through the
ONE use case". That holds only for an order that is `ACKNOWLEDGED`.
`ConfirmNetworkOrderShipment` refuses any other state
(`ErrConfirmBeforeAcknowledge`), and an order stays `SUBMITTED` until
`ReconcileSubmittedOrders` hears the network's verdict, inside a 24 h
acknowledgement window. A load can legitimately depart in that window. The
consumer retried the confirmation three times, dead-lettered the message to
`warehouse.outbound-shipping.events.dlq`, committed past it, and the
confirmation was lost until an operator replayed it with the manual POST. The
DLQ is for messages that break the contract, not for this race.

**Decision.** The shipment is recorded on the order and confirmed once the order
is `ACKNOWLEDGED`. No new topic, no new environment variable, no event schema
change.

1. **The order carries one pending shipment.** `NetworkOrder.pendingShipment`
   is the shipment reference (the load id), the package count (`0` when the
   event carried none) and when it was noted. `NoteShipmentBeforeAcknowledge`
   accepts it only in `SUBMITTED`; the first shipment noted wins (the same ref
   again, or a different one while one is pending, is a no-op success).
   `ConfirmShipment` clears it, and `DropPendingShipment` clears it without
   confirming. It is persisted by migration 0015 (three nullable columns on
   `network_orders`, a `CHECK` that they are all `NULL` or all set, and a
   partial index on the rows that carry one).
2. **The consumer defers.** When `ExecuteForShipment` fails with
   `ErrConfirmBeforeAcknowledge`, the consumer calls
   `DeferShipmentConfirmation`. For a `SUBMITTED` order that records the
   shipment, the consumer logs `shipment confirmed before the order was
   acknowledged; deferred` at Info (`ce_id`, `load_id`, `order_id`), goes on to
   the next order of the load, and the offset is committed with no dead letter.
   For any other state, deferring returns `ErrConfirmBeforeAcknowledge`
   unchanged, so `NEW` and `REJECTED` keep the retry-then-DLQ path: a shipped
   order the network never took is a genuine anomaly for an operator.
3. **A ticker settles it.** `ConfirmPendingShipments` (`runConfirmPending`, every
   `POLL_INTERVAL` like `runReconcile`, always on) lists the orders that carry a
   shipment and, per order: `ACKNOWLEDGED` is confirmed through the same
   `ExecuteForShipment` (so the `(shipment reference, order id)` ledger still
   keeps it to one label request, one network submission and one
   `NetworkOrderShipmentConfirmed`); `SUBMITTED` is skipped; `CONFIRMED` or
   `REJECTED` has its shipment dropped. One order's failure is logged and
   counted, and never aborts the pass.
4. **The manual endpoint is unchanged.** `POST .../shipment-confirmation` on a
   `SUBMITTED` order still answers `409 confirm-before-acknowledge`; it records
   nothing.

**Why not the alternatives.** Retrying longer in the consumer would hold the
partition (and every load behind it) for up to 24 h. Letting the manual path
confirm a `SUBMITTED` order would tell the network a shipment left for an order
it has not accepted yet, which is what `ErrConfirmBeforeAcknowledge` exists to
prevent.

**Consequences.** A shipment noted for an order that is later `REJECTED` is
dropped, not confirmed. A second load noted while one is pending is logged and
ignored: an order is confirmed once, by whichever shipment is noted first.
Rolling back migration 0015 drops any shipment still pending; the
operator replays it with the manual POST once the order is `ACKNOWLEDGED`.
