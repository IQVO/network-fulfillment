---
id: 0029-fulfillment-site-on-the-held-order
slug: /adr/0029-fulfillment-site-on-the-held-order
title: "29. The fulfilling site rides on the held order"
sidebar_label: "29. Site on the held order"
sidebar_position: 29
description: "ADR 0029 (Accepted) sends the site a network order is destined for to order-management as the optional fulfillmentSiteId on the held order, so demand is projected to that site instead of the one static site."
---

# 29. The fulfilling site rides on the held order

## Status

**Accepted** (2026-10-25). Builds on [ADR 0026](./0026-live-retail-network-adapter-demand-loop.md)
(the demand loop and the held order) and on order-management ADR 0044 (the optional,
caller-assigned `fulfillmentSiteId`, which amends its ADR 0035 and 0031).

## Context

order-management projects every order's demand (`SiteSkuDemandChanged`) to ONE static
site (`DEMAND_PROJECTION_SITE_ID`, assignment version `static-site-v1`), because its
orders carried no site. Network-inventory-planning needs demand facts at BOTH ends of a
transfer, so a second site never had any. order-management now accepts an OPTIONAL
`fulfillmentSiteId` on `POST /orders`, assigned by the caller and never inferred there,
and projects to it (`order-site-v1`).

A network order already names its site: `NetworkOrder.SiteId` is the node the network
addressed the demand to, mapped through the node map (ADR 0026), and it is already part
of `contract.HeldOrderRequest`. The outbound adapter simply did not send it.

## Decision

1. **`RaiseHeldOrder` sends `fulfillmentSiteId`** with the order's `SiteId`. It is the
   site this service already decided on; it is never invented here and never looked up
   from another service.
2. **Omit rather than fail.** An empty site, or one that is not a site code (uppercase
   ASCII letters and digits, at least one character: the shape facility-layout and
   order-management ADR 0044 use, mirrored here, not called), is left out of the body
   and logged. The field is optional there, so the hold and the network's answer must
   not fail on it. An order with no usable site projects to the static site exactly as
   before, and its request body is byte-identical to what was sent before this ADR.
3. **One site per order.** A network order has exactly one site, so there is no
   multi-site case to resolve; had it more than one, nothing would be sent.
4. **Nothing else changes.** The public Vendor API, the events this service publishes
   and the ports are unchanged. The change is one additive field on one outbound
   request. No new environment variable and no chart value: infra values required are
   none.

## Consequences

- Demand from network orders now reaches the site the network chose, which is what lets
  a transfer between two sites find demand at both ends.
- The body of `POST /orders` is hashed by order-management's idempotency middleware.
  A raise that was recorded but not yet answered before this change shipped, and is
  retried after it, is sent with a different body under the same key and is answered
  `422 idempotency-key-reused`. That is the same window as any other change to this
  body, it can only touch orders in flight at the moment of the rollout, and it needs no code.
- A malformed site is visible only in the log (`network order site is not a site code`).
  The node map (ADR 0026) is where site codes are declared; fixing the code there fixes
  every later order.
- customer-ordering is the other caller named in order-management ADR 0044 and sends its
  own site on its own change.
