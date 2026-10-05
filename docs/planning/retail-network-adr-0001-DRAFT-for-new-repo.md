---
id: 0001-retail-network-bounded-context
slug: /adr/0001-retail-network-bounded-context
title: "1. retail-network as its own organization: Open Host Service upstream of network-fulfillment"
sidebar_label: "1. retail-network bounded context"
sidebar_position: 1
description: "ADR 0001 (Accepted) — why the ecosystem's own stand-in for an external retail network (not a real e-commerce retailer's SP-API) is a new, separate bounded context: one context (not a marketplace/sourcing split), Direct-Fulfillment-like protocol fidelity in the network's own vocabulary, a declared-plus-measured capability model, proportional-to-stock capacity sharing, release-on-reconciled-acceptance, and no auth."
---

> **STAGING NOTE — read this before touching anything below.**
> This ADR is drafted here, inside `network-fulfillment`, at
> `docs/planning/retail-network-adr-0001-DRAFT-for-new-repo.md`,
> **temporarily**, because the `retail-network` repository does not exist
> yet (it is scaffolded in Phase 2 of
> `2026-09-25_231500-retail-network-in-ecosystem.md`). When that repo is
> created, move this file **verbatim** (frontmatter included) to
> `retail-network`'s own `docs/docs/adr/0001-retail-network-bounded-context.md`
> and delete it from here. Nothing in this file should change in that
> move except the file's location and, if `retail-network`'s ADR
> numbering has moved on for some other reason, the number.

# 1. retail-network as its own organization: Open Host Service upstream of network-fulfillment

## Status

**Accepted** (2026-09-26). Companion to `network-fulfillment` ADR 0009 (drafted at
`network-fulfillment/docs/adr/0009-retail-network-not-amazon-counterpart.md`
in that repo's real ADR sequence; originally drafted as ADR 0002 there,
renumbered to 0009 in 2026-10 because that repo had a duplicate 0002).
Neither is meaningful without the
other: this record defines the organization `network-fulfillment` ADR
0009 says it is now conformist to, and ADR 0009 records the rename and
narrowing (`adapters/outbound/network/` → `adapters/outbound/retailnetwork/`,
no ship-to PII in that context at all) that only make sense once this
context is agreed to exist. Raised together so the boundary can be
accepted or rejected as one decision.

## Context

### This supersedes the real-SP-API plan, not just adds to it

The prior plan (`2026-09-25_224823-amazon-network-fulfillment-readiness.md`)
assumed `network-fulfillment` would eventually integrate against a real
e-commerce retailer's Selling Partner API. The user's correction was
explicit: **not the real one, our own ecosystem.** The counterpart that
issues purchase orders, measures our on-time performance, and promises a
delivery date to a (synthetic) shopper is a new service in this fleet,
playing that same structural role — not the real retailer, no real SP-API,
no real vendor credentials, no real customer PII anywhere.

Two independent facts made this worth building rather than left as a
permanent stub:

1. **A stub cannot exercise the failure modes it exists to test.**
   `network-fulfillment`'s crash-safety work (orphaned holds, un-retried
   recoverable failures, submissions treated as done on HTTP 202, an
   oversellable throughput-constrained availability computation) needs a
   counterpart that can actually delay, fail, rate-limit and redeliver on
   purpose. `StubGateway` never does any of that.
2. **Owning both sides closes gaps a real integration could never
   close.** A real e-commerce retailer's SP-API accepts no declaration of
   capability (no operation takes "my cutoffs are 11:00 and 18:00 and my
   handling time is 95 min"); our own network can. A real network's
   promise floor is a departure time; our own network can own the missing
   half, `deliverBy = shipBy + transit(zone)`. A real network requires
   real customer PII in `network-fulfillment`; our own network can hold
   it instead and never forward it inward, which is a *stronger* boundary
   than the original ADR 0001 could get against a real network.

### Why a new, separate context — not an adapter, not a shared module

`retail-network` is, deliberately, **a different organization** from the
`warehouse-systems` fleet, exactly as a real e-commerce retailer's network
would be. It:

- runs its own Postgres, its own repo, its own CI, scaffolded from
  `warehouse-harness-template` like every other context in this fleet;
- never imports fleet Go packages, never consumes a `warehouse.*` Kafka
  topic, never reads fleet data by any channel other than its own vendor
  API. Everything it knows about `network-fulfillment` arrives the way a
  real retailer would learn it from any vendor: through calls that vendor
  makes to its API and the accuracy it measures afterward. This is
  enforced by a fitness test inside `retail-network` (no `warehouse.*`
  imports, topics or URLs) — if this rule is ever broken, the simulation
  stops being an honest stand-in for a real external network and becomes
  a second copy of the fleet's own domain logic wearing a costume.
- deliberately uses **its own vocabulary** — `poNumber`, `listingId`,
  `nodeId`, its own reason codes — chosen to differ from
  `network-fulfillment`'s fleet vocabulary on purpose (see D2 below), so
  that `network-fulfillment`'s Anti-Corruption Layer does real
  translation work instead of ceremonial pass-through.

### Decisions made and how they were resolved (D1–D6)

Six decisions were raised to the user and each was resolved with a
specific, non-default answer. Recording the alternative considered for
each, because a reader six months from now needs to know it was weighed,
not missed:

**D1 — Shape of the network: one context, not a split.**
Chosen: a single bounded context, `retail-network`, covering storefront,
sourcing and transit together. Alternative considered: splitting into
`marketplace` (storefront/promise) and `network-sourcing` (vendor
protocol) — rejected because it is a more textbook-faithful split but
adds a third repository for no new behavior in v1; the storefront and the
vendor protocol share the same aggregates (`Listing`, `PurchaseOrder`)
and the same lifecycle, so splitting them would mean either duplicating
that lifecycle or introducing a synchronous call between two repos this
project doesn't otherwise need.

**D2 — Protocol fidelity: Direct-Fulfillment-like semantics, in our own
vocabulary.**
Chosen: keep the shape that makes the real protocol worth learning from
— poll-driven demand delivery, a 24-hour fill-or-kill acknowledgement
window, asynchronous submission-then-reconciliation for every write —
but expressed entirely in `retail-network`'s own nouns
(`poNumber`, `listingId`, `nodeId`, its own reason codes), never a real
retailer's own terms (`purchaseOrderNumber`, `buyerProductIdentifier`/ASIN,
`acknowledgementStatus`, `sellingParty`). Alternative considered: a
simpler, fleet-native Kafka integration between `network-fulfillment` and
`retail-network` — rejected because it turns `network-fulfillment`'s
Anti-Corruption Layer into ceremony (translating between two nearly
identical vocabularies proves nothing) and because the hard, valuable
paths — a crash mid-poll, an unacknowledged purchase order expiring, a
submission that never reconciles — are exactly the paths a same-vocabulary
Kafka integration would never force anyone to handle correctly.

**D3 — Capability model: declared profile plus measured scorecard.**
Chosen: `FulfillmentNode` carries both a **declared** profile
(`cutoffs[]`, `handlingTime`, `tz` — set by `network-fulfillment` via
`PUT /vendor/nodes/{nodeId}/profile`) and a **measured** scorecard
(`onTimeShipRate`, `lateAckRate`, `cancelRate`, computed by
`retail-network` itself from what actually happened). Alternative
considered: infer-only, which is what a real retailer's SP-API does
today (no operation accepts a capability declaration) — rejected because
it would lose the entire point of owning both sides: the ability to
expose our real planning time (PPM's `cycleTimeP95` and `CPTSchedule`) to
the network instead of forcing it to guess. Declared-plus-measured is
more realistic in the literal sense too — a real vendor's relationship
with such a retailer also has both, just not over an API; Vendor Central
config *is* a declaration, made through a different channel.

**D4 — Capacity share: proportional to physical stock.**
Chosen: when a process path's remaining throughput capacity is less than
the combined physical stock of the SKUs eligible on that path, the
remaining capacity is split across those SKUs **in proportion to each
SKU's own physical stock**, not equally and not first-come-first-served.
This is `retail-network`'s consumption of `network-fulfillment`'s
`CapabilityOffer` inventory feed (which already carries
`basis: Physical | ThroughputConstrained`, per `network-fulfillment` ADR
0001 §"the one genuinely new domain concept") — `retail-network` itself
does not recompute throughput; it receives already-throttled
per-SKU quantities and its own `Listing.availableQty` is exactly that
number. This decision governs how `network-fulfillment`'s
`CapabilityOffer` divides one path's capacity across several eligible
SKUs before it ever reaches `retail-network` as a listing.

**D5 — Release timing: on reconciled acceptance, not on ack.**
Chosen: `network-fulfillment` (and, by extension, `order-management`'s
`ReleaseHeldOrder`) treats a purchase order as accepted only once the
acknowledgement submission is **reconciled** (its async transaction
resolves to `SUCCESS`), not the instant the 202-accepted response for the
submission arrives. Alternative considered: release immediately on
receiving the 202 — rejected because a submission accepted-for-processing
is not a completed commitment (this fleet already learned this lesson
once, in `network-fulfillment` ADR 0001 §5); releasing work before
`retail-network` has actually confirmed the acknowledgement would risk
releasing work for a purchase order whose acknowledgement later fails to
reconcile. The cluster's own processing delay (`TX_PROCESSING_DELAY`,
default 5s) is seconds, so there is no material latency cost to waiting
for the real signal instead of the optimistic one.

**D6 — Auth: none.**
Chosen: `retail-network`'s Storefront and Vendor APIs are unauthenticated,
consistent with the fleet-wide unauthenticated decision already standing
across every other context (`order-management` ADR 0012 and its
siblings). Alternative considered: requiring auth because this context
holds the fleet's only PII — rejected because that PII is **synthetic**
(a fabricated shopper, not a real person) and it **stays inside
`retail-network`**, never crossing into `network-fulfillment` or any
other fleet context. The risk that made PII-bearing systems elsewhere in
a real deployment require auth does not apply to synthetic data that
never leaves this one context's own database. This is a study project;
if it ever needed to model a real customer's data, this decision would
need to be revisited from scratch, not extended.

## Decision

We will create `retail-network` as a new, separate bounded context: a
**separate organization** from the `warehouse-systems` fleet, upstream of
`network-fulfillment`, and an **Open Host Service** with its own
Published Language. `network-fulfillment` stays **Conformist + Anti-
Corruption Layer**, exactly as its own ADR 0001 already established for
"an external retail fulfillment network" — this record specifies that the
network is `retail-network`, not a real e-commerce retailer's SP-API.

### Aggregates

```
FulfillmentNode   nodeId, declaredProfile{cutoffs[], handlingTime, tz},
                  scorecard{onTimeShipRate, lateAckRate, cancelRate, window}

Listing           listingId -> nodeId, availableQty, updatedAt
                  (full or partial inventory feeds; a full feed zeroes
                  every SKU it omits)

CustomerOrder     orderId, shopper (synthetic), shipTo (PII — stays here,
                  never forwarded), lines, promise{shipBy, deliverBy},
                  status

PurchaseOrder     poNumber, nodeId, lines, requiredShipBy,
                  ackBy = issued + 24h
                  states: NEW -> ACCEPTED | REJECTED{reason} | EXPIRED
                          -> SHIPPED -> DELIVERED
                  invariants: fill-or-kill (a partial acknowledgement is
                  refused, not partially applied); acknowledged exactly
                  once; no shipment confirmation before acceptance

Submission        transactionId, kind{ack, inventory, shipment},
                  states: PROCESSING -> SUCCESS | FAILURE{errors}
                  (asynchronous; processing delay is a configured knob,
                  not instantaneous)

Shipment          poNumber, label{trackingNumber, carrier, labelRef},
                  shippedAt, deliveredAt (driven by the transit simulator)
```

`PurchaseOrder`'s fill-or-kill invariant and `Submission`'s
pending-until-reconciled modeling both mirror a real Direct-Fulfillment-
style program's real semantics (D2) — deliberately, since the whole point
of building this counterpart is to force `network-fulfillment` to survive
exactly these paths.

### The network's own promise and sourcing logic

```
checkout(listing, qty, shipTo):
  node      = a node whose listing covers qty            (listing availability)
  shipBy    = first declared cutoff >= now + handlingTime,
              pushed one cutoff later if scorecard.onTimeShipRate < threshold
  deliverBy = shipBy + transit(originZone, destZone)
  -> CustomerOrder(promise) + PurchaseOrder(requiredShipBy = shipBy)
```

This closes the loop a real integration could never close:
`network-fulfillment` declares (D3), `retail-network` dictates the
promise, `order-management` checks it against real feasibility
(`PromisePolicy.FeasibleBy`, `network-fulfillment` ADR 0001 §7), the
floor executes it, `retail-network` measures the outcome, and the next
promise reflects that measurement.

### Vendor API — the Open Host Service (`apis/openapi.yaml`)

```
GET  /vendor/purchase-orders?createdAfter=&nextToken=     poll (paged)
POST /vendor/acknowledgements            -> 202 {transactionId}
POST /vendor/inventory                   -> 202 {transactionId}  (isFullUpdate)
PUT  /vendor/nodes/{nodeId}/profile      declared capability
POST /vendor/shipping-labels             -> {labelRef, trackingNumber, carrier}
POST /vendor/shipment-confirmations      -> 202 {transactionId}
GET  /vendor/transactions/{transactionId}
GET  /vendor/nodes/{nodeId}/scorecard
```

No operation in this list is authenticated (D6). This is
`network-fulfillment`'s only channel to this context, and the only
channel through which this context learns anything about
`network-fulfillment` — never a Kafka topic, never a direct database
read.

### Storefront API

```
GET  /catalog
POST /checkout
GET  /orders/{id}
```

Driven by `e2e-tests` and, later, a storefront micro-frontend — the
entry point that exercises the whole ecosystem starting from a shopper's
perspective rather than from `network-fulfillment`'s poll.

### Simulation knobs (environment, per deployment)

```
TX_PROCESSING_DELAY   default 5s   (a real vendor program's own guidance
                                    is ~15 min for acknowledgements; the
                                    cluster runs a compressed simulation,
                                    not real time)
TX_FAILURE_RATE       default 0    (fraction of submissions that resolve
                                    FAILURE instead of SUCCESS)
RATE_LIMIT_RPS        default well-behaved (no throttling)
PO_REDELIVERY_RATE    default 0    (fraction of polls that redeliver an
                                    already-delivered purchase order)
TRANSIT_TABLE_FILE    zone -> transit-time lookup for deliverBy
```

Every knob defaults to "well-behaved." `e2e-tests` sets them per
scenario to force the specific failure mode that scenario is proving
`network-fulfillment` survives.

### Background sweeps

```
ExpireUnacknowledgedPOs   a PO past ackBy with no acknowledgement moves
                          to EXPIRED, its CustomerOrder is cancelled, and
                          the owning node's scorecard takes the hit
SimulateDelivery          advances SHIPPED purchase orders to DELIVERED
                          on the transit-table schedule
```

### Not part of the warehouse data mesh

`retail-network` is another organization, so it gets no analytics
projector and no `analytics_services` entry in `warehouse-infra`. The
scorecard **is** its read model — there is no separate reporting surface
to build.

## Consequences

**Easier**

- `network-fulfillment`'s crash-safety and reconciliation work becomes
  genuinely testable: a counterpart that can delay, fail, rate-limit and
  redeliver on purpose replaces a stub that never does any of those
  things.
- The customer promise gets its missing half. `network-fulfillment`
  keeps owning the departure-time floor (`requiredShipBy`);
  `retail-network` becomes the natural owner of
  `deliverBy = shipBy + transit(zone)`, closing a gap a real integration
  could never close from our side.
- Capability can be genuinely declared and then measured against, which
  a real e-commerce retailer's SP-API structurally cannot support.
- Customer PII (synthetic) is contained to exactly one context in the
  whole fleet, and it is a *new* context built for this purpose, not an
  existing one being retrofitted to hold it.

**Harder**

- This is a second repository's worth of domain logic, CI, chart and
  infra wiring to build and maintain (Phase 2 onward), for a system whose
  only "customer" is this fleet's own e2e proof and demonstration goals.
- Two demand-shaping systems now exist that must agree on the ackBy
  clock's meaning without ever sharing a database or a Kafka topic — any
  drift between them (e.g. a clock-skew bug) is a cross-organization
  debugging problem, not a same-transaction one.
- `retail-network`'s fitness test (no `warehouse.*` imports/topics/URLs)
  must be maintained honestly forever; any future contributor tempted to
  "just import the fleet's SKU type to save time" would quietly break the
  entire premise of the simulation.

**Deliberately out of scope for this record**

- Payments and invoicing.
- Returns.
- Multi-node sourcing across several warehouses. The model allows more
  than one `FulfillmentNode`, but v1 has exactly one — mirroring
  `order-management`'s existing single-site simplification
  (`network-fulfillment` ADR 0001, "Deliberately out of scope").
- A second network (a distinct organization playing a second retailer).
- A real e-commerce retailer's SP-API adapter. Explicitly **deferred, not
  dropped** — see the companion `network-fulfillment` ADR 0009 — as an
  adapter swap behind the same `NetworkGateway` port, should this project
  ever want to integrate against the genuine article.

## Rollout

Sequenced by the parent plan
(`2026-09-25_231500-retail-network-in-ecosystem.md` §6), the phase this
ADR belongs to is Phase 1 (docs only, this record and its companion).
Phase 2 scaffolds the real `retail-network` repository from
`warehouse-harness-template` with `PurchaseOrder`, `Submission` and
`FulfillmentNode` (profile only), the vendor poll/acknowledgement/
transaction API, a `POST /checkout` with a fixed `shipBy`, Postgres,
chart and infra wiring, and the no-`warehouse.*` fitness test. This
record is not itself an implementation plan; the parent plan's §6 is.
