---
id: 0036-purchase-order-keyed-read
slug: /adr/0036-purchase-order-keyed-read
title: "36. A purchase order is read by its number: GET /purchase-orders/{poNumber}"
sidebar_label: "36. Keyed read by purchase-order number"
sidebar_position: 36
description: "ADR 0036: GET /purchase-orders/{poNumber} returns the NetworkOrder (with its localOrderId) of one purchase order, found through the persisted poNumber-to-networkRef map of ADR 0026. A keyed read, not a listing and not a filter: no query parameters, no paging; 404 purchase-order-not-found, 400 invalid-purchase-order-number. The minted NetworkRef does not change."
---

# 36. A purchase order is read by its number: `GET /purchase-orders/{poNumber}`

## Status

**Accepted** (2026-11-15). **Extends [ADR 0026](./0026-live-retail-network-adapter-demand-loop.md)**:
the `NetworkRef` stays minted and opaque (Decision 4), and the alternative "use the `poNumber` as the
`NetworkRef`" stays rejected. This ADR adds a read beside the minted ref, not instead of it. It is a
read, so the "one write endpoint" rule of [ADR 0014](./0014-explicit-shipment-confirmation-endpoint.md)
is untouched. It narrows how hard rule 1 of `CLAUDE.md` is applied (see Decision 8); the guide itself
is not edited here.

## Context

The end-to-end "retail day" has to follow a storefront order to the order-management order that was
raised for its purchase order. No published route of this context can answer that:

- `GET /network-orders` lists only orders in `NEW` and rejects every query parameter except `limit`
  and `cursor`. That is deliberate: "a general all-orders endpoint would answer no question".
- `GET /network-orders/{networkRef}` needs the `networkRef`, which this context mints
  (`NR-<uuid>`, ADR 0026 Decision 4) the first time it sees a purchase order. Nobody outside can
  know it or work it out.
- The purchase-order number, in contrast, is known outside. order-sourcing publishes
  `(assignmentId, releaseNo)`, and vendor-connectivity derives `poNumber` as `PO-` plus the first 16
  hex digits of `sha256(assignmentId + ":" + releaseNo)` (its ADR 0002, A1; and "no consumer parses a
  `poNumber`").

This context already keeps the map from one to the other, because an acknowledgement must name the
purchase order: `network_purchase_orders (network_ref PRIMARY KEY, po_number UNIQUE, ...)` (migration
0006, ADR 0026 Decision 4), and it keeps the order's own link to the order-management order,
`network_orders.local_order_id`. The two joined answer the question. Per the e2e program's rule
(its ADR 0001, D5: "only published REST; a missing route is added where it belongs"), a missing route is added in the repository that owns the data.

## Decision

1. **A keyed read: `GET /purchase-orders/{poNumber}`.** It answers with the **same** `NetworkOrder`
   body as `GET /network-orders/{networkRef}` (`operationId` `getNetworkOrderByPurchaseOrder`, tag
   `network-orders`): `state`, `siteId`, `requiredShipBy`, `acknowledgeBy`, `receivedAt`,
   `acknowledgementOverdue`, `networkRef`, the lines, and `localOrderId` once the order was
   acknowledged (absent for `NEW` and `REJECTED`, exactly as the sibling). No new schema and no
   `poNumber` field in the answer: the caller already has the number.
2. **Why keyed by the natural key, and not a list or a filter.** The keyed read answers one question
   with one order or a clear "no such purchase order". It cannot be used to enumerate. A
   `GET /network-orders?poNumber=...` would be a collection filter: a list that holds zero or one
   item, which reopens the "all orders" listing ADR 0026's stance refuses, needs paging
   semantics for a result that can never page, and turns "unknown purchase order" into an empty
   `200` that a polling caller cannot tell from "not yet received". `GET /network-orders` stays the
   one list and still rejects every parameter but `limit` and `cursor`. The wording of its
   description ("only this list is exposed") is reconciled: it is the only **listing**; single
   orders are read by key.
3. **It is not "the `poNumber` is the `NetworkRef`".** The ref is still minted and still the
   primary key; the route resolves the number to the ref through the persisted map, then reads the
   order by ref. The map, not a string convention, is the correlation (hard rule 4).
4. **One read, no duplicated mapping.** A new narrow port,
   `ports.PurchaseOrderIndex.FindNetworkRefByPONumber(ctx, poNumber) (ref, ok, err)`, implemented by
   `retailnetwork.PGStore` over `network_purchase_orders` (the `UNIQUE (po_number)` constraint is
   already the index; **no migration**). The handler then runs the same `FindByRef` and the same
   `toNetworkOrderResponse` as `GET /network-orders/{networkRef}` (one shared function), so the two
   routes cannot disagree about an order. It is a separate port, not a method on
   `NetworkOrderRepo`, because those tables belong to the `retailnetwork` adapter (migration 0006),
   whose `Store` stays in that package so purchase-order numbers do not spread; the Postgres order
   repository would otherwise read another adapter's tables. The number crosses the port as an
   opaque string.
5. **Malformed number: 400 `invalid-purchase-order-number`.** The number is validated for its shape
   as a URL path segment and nothing else: 1 to 128 characters, each one of `A-Z a-z 0-9 - . _ ~`.
   The `PO-` plus 16 hex shape is another context's detail and is **not** checked; no context may
   parse a `poNumber`. Rejecting the rest keeps a percent-encoded or exotic value from reaching
   SQL and the logs. (The sibling answers an empty `networkRef` with 422 `empty-network-ref`; that
   stays as it is.)
6. **Unknown number: 404 `purchase-order-not-found`.** A new error, `usecases.ErrPurchaseOrderNotFound`,
   mapped in both tables of `errors.go` (`errors_test.go` keeps them one for one). It also answers
   when the map knows the number but the order is not stored yet (the adapter records the map row
   when it polls the purchase order, the use case then saves the order): there is no order to show,
   and the caller retries. The problem document says so in its description.
7. **No query parameters, no paging.** Any query parameter, `limit` and `cursor` included, is a 400
   `unknown-query-parameter`, the slug the collections already use. The route is registered only
   when the index is wired (the composition root of `cmd/netfulfil` always wires it).
8. **The network's vocabulary, narrowly.** Hard rule 1 keeps `poNumber` out of the domain and out of
   anything published to the fleet. This route is a deliberate, bounded exception, and the bound is
   the decision: the number appears in **one** path parameter of **one** inbound route, in the
   signature of **one** port and in the Postgres adapter that owns the table. It does not appear in
   `internal/domain`, in the route's answer, in events or in logs beyond the access log's path. The
   reason is that the number is not only the network's: the fleet derives it itself (vendor-connectivity
   ADR 0002 A1), which is why a caller outside can hold it at all.

## Consequences

- The e2e collector (`e2e-tests`, the retail day) is the first caller: it derives the number from
  the `(assignmentId, releaseNo)` that order-sourcing published, calls this route and reads
  `localOrderId`, which is the order-management order id. Any other client that holds a purchase-order
  number can do the same.
- **Stub mode answers 404 for every number.** The map tables are written only by the live adapter
  and "stay empty in `NETWORK_MODE=stub`" (migration 0006). A stub run reads its demand by
  `networkRef`, as before. Nothing is faked to hide that.
- A purchase order the network issued and this context has not polled yet is a 404 too. A caller that
  expects the order retries; the poll interval is the bound.
- Additive on the wire: a new path, one new `400` slug and one new `404` slug. No existing route,
  status or body changes. Consumers that pin this repository's `apis/openapi.yaml` copy it
  byte for byte from `develop` (see `contracts/pinned.json`): this repository's own self-pin
  (`internal/adapters/inbound/mcp/testdata/network-fulfillment.openapi.yaml`) is refreshed in the
  same change.
- MCP and A2A are **not** extended. Their charters are curated read tools; nothing asked for a
  lookup by number there, and a later tool can call the same port.
- The route inherits the surface's posture: unauthenticated like every fleet REST route (no ship-to
  PII is in the answer), CORS `GET`, otelchi metrics named by the route pattern
  (`/purchase-orders/{poNumber}`, never once per number).

## Alternatives considered

- **Use the `poNumber` as the `NetworkRef`.** Rejected by ADR 0026 and still rejected: it makes the
  network's identifier the fleet's key by convention.
- **`GET /network-orders?poNumber=`.** Rejected, Decision 2: a filter on a list that must stay the
  unanswered working set, with an empty `200` for "unknown".
- **A method on `NetworkOrderRepo` joining `network_orders` to `network_purchase_orders`.** Rejected,
  Decision 4: it moves the adapter's tables, and the number, into the order repository and into
  every fake of that port.
- **Carry the purchase-order number on the published events**, so an outside consumer can correlate
  them. Rejected: it publishes the network's identifier to the fleet, the leak hard rule 1 exists
  to prevent, and it changes a Published Language (a new event version, ADR 0008) to serve one
  reader that only needs a keyed lookup.
