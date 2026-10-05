---
id: 0009-explicit-shipment-confirmation-endpoint
slug: /adr/0009-explicit-shipment-confirmation-endpoint
title: "9. Shipment confirmation is an explicit endpoint, not a PackageManifested correlation"
sidebar_label: "9. Explicit shipment confirmation"
sidebar_position: 9
description: "ADR 0009 amends ADR 0001 Rollout step 6: NetworkOrderShipmentConfirmed is raised by an explicit ConfirmNetworkOrderShipment use case/endpoint naming the NetworkRef directly, not by correlating fulfillment-execution's PackageManifested event, because no persisted NetworkRef<->WorkUnitId mapping exists yet and ADR 0001 Hard rule 4 forbids inventing a string-convention join."
---

# 9. Shipment confirmation is an explicit endpoint, not a PackageManifested correlation

## Status

Accepted (2026-10). Amends [ADR 0001](./0001-network-fulfillment-bounded-context.md)'s
Rollout step 6, which named the trigger as "driven by
`fulfillment-execution`'s existing `PackageManifested` event". Also adds
this context's first and only write endpoint, narrowing Hard rule 9
("REST is read-only... no write endpoint without a new ADR") for this
one case only.

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
