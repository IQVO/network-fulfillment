---
id: 0001-network-fulfillment-bounded-context
slug: /adr/0001-network-fulfillment-bounded-context
title: 1. Network Fulfillment as a bounded context — conformist to the network, anti-corruption layer for the fleet
sidebar_label: 1. Network Fulfillment bounded context
description: "ADR 0001 — why the integration with an external retail fulfillment network (a major e-commerce retailer's Selling Partner API, Vendor Direct Fulfillment) is its own bounded context rather than an adapter inside order-management: it is Conformist upstream, holds the only customer PII and the only external SLA in the fleet, and owns the one genuinely new domain concept — advertised availability constrained by throughput, not just by stock."
---

# 1. Network Fulfillment as a bounded context — conformist to the network, anti-corruption layer for the fleet

## Status

Accepted (2026-09-23). Companion to `order-management` ADR 0020 (network-originated
demand: `releaseOnAllocation`, `PromisePolicy.FeasibleBy`, and the
`Network` promise basis). Neither is meaningful without the other: this
context cannot honour the network's 24-hour fill-or-kill acknowledgement
deadline without the two capabilities that record adds, and those
capabilities have no caller until this context exists. Raised together
so the boundary can be accepted or rejected as one decision.

## Context

### What "selling capability to a network" actually means

The `warehouse-systems` fleet models a building that knows, in real
terms, what it can do. `process-path-management` owns the fulfillment
capability contract (its ADR 0010): per-path `cycleTimeP95`,
eligibility rules, and a site-scoped `CPTSchedule` of recurring daily
cutoffs, published as `CPTScheduleChanged`. `wes-work-planning`
publishes remaining capacity per `(path, CPT)` as `PathCapacityChanged`.
`order-management` consumes both to derive a real delivery promise
(ADR 0014 and its successors), and `fulfillment-execution` reports
missed CPTs back so the promise can be corrected.

The obvious next question — "can we offer that capability to an external
retail network?" — has a non-obvious answer, and it shaped this whole
record. **There is no API that accepts a declaration of capability.**

A major e-commerce retailer's Selling Partner API, across Vendor Direct Fulfillment
(Orders / Shipping / Inventory / Payments / Transactions) and the
Fulfillment Outbound family, exposes no operation to which a
fulfilling party can say "my cutoffs are 11:00 and 18:00 local, and my
pick path takes 95 minutes at p95". Lead times and warehouse operating
hours are Vendor Central configuration, not an API surface.

Capability reaches a network through exactly three signals, all of them
*consequences* rather than declarations:

```
submitInventoryUpdate        what we claim we can ship
submitAcknowledgement        what we commit to — within 24h, whole order
submitShipmentConfirmations  whether we actually did
```

The network infers our capability from the gap between the second and
the third, and prices its customer-facing delivery promise accordingly.
That is why this is a domain problem and not a REST-mapping exercise:
**the interesting decision is what to advertise and what to commit to,
and the fleet already owns every fact needed to decide both well.**

### The one genuinely new domain concept

Most integrations of this shape feed advertised availability straight
from physical stock on hand. This fleet can do better, because it knows
something a stock ledger does not — whether the building can actually
*move* those units before the next departure.

```
advertisedQuantity = min(
    physicalAvailable,                       inventory-storage
    throughputFeasibleBefore(nextCutoff)     process-path-management
                                             cycleTimeP95 + CPTSchedule,
                                             × wes-work-planning remaining
                                               PathCapacity for that (path, CPT)
)
```

If the building holds 400 units of a SKU but the eligible path can only
carry 120 more before the 18:00 cutoff, advertising 400 sells a promise
the floor cannot keep — and the network charges for that in lead-time
ratings, cancellation rates, and ultimately in the delivery promise it
shows a customer. Advertising 120 is selling capability honestly.

Every input already exists and is already on this fleet's Kafka bus.
Nothing new needs to be computed; it needs to be *aimed outward* for the
first time. This is the feature. The rest of this context is the
plumbing that makes it deliverable and honest.

### Why this cannot live inside `order-management`

`order-management` is where orders live, so the pull to put the
integration there is real. Four facts make it the wrong home:

1. **The relationship is Conformist.** This fleet has exactly zero
   influence over the network's contract. Its vocabulary —
   `purchaseOrderNumber`, `itemSequenceNumber`, `buyerProductIdentifier`
   (an ASIN, not our SKU), `acknowledgementStatus` codes,
   `sellingParty`/`shipFromParty`, `ShippingSpeedCategory` — is not
   ours. Every other upstream relationship in this fleet is
   Customer/Supplier with a negotiable Published Language. This one
   cannot be negotiated, only translated, and translation needs a place
   to happen.
2. **Customer PII crosses the boundary for the first time.** Network
   orders carry ship-to name, address and phone; retrieving them is a
   restricted operation requiring a Restricted Data Token. No context in
   this fleet holds PII today, and every REST/MCP endpoint in the fleet
   is currently unauthenticated by deliberate decision (`order-management`
   ADR 0012 and its siblings). Putting PII into the aggregate that four
   other contexts already read would be the largest blast-radius change
   available, made for the narrowest reason.
3. **The acknowledgement clock is an external SLA.** The network expects
   a complete acknowledgement within 24 hours, covering every line,
   accept-or-reject in full. Missing it is an external penalty, not an
   internal inconvenience. No context in this fleet carries an
   SLA-bearing responsibility today.
4. **Advertised availability is not an order concept.** It spans
   inventory, path capability and live capacity, and it is published
   whether or not any order exists. It has no natural home on the
   `Order` aggregate.

### Where the network's model collides with what the fleet already shipped

Two collisions are real and are resolved here and in the companion
record rather than discovered later in production:

- **Fill-or-kill versus per-shipment-group promising.** The network is
  explicit: partial shipments are not allowed; a purchase order is
  confirmed or rejected in its entirety, and a partial acknowledgement
  is not accepted. `order-management` ADR 0017 shipped precisely the
  opposite capability for ordinary customer orders — grouping lines by
  the cutoff each can make and promising each group separately. Both
  rules are correct for their own demand class. Network demand must
  therefore be ship-complete, and the companion ADR makes that an
  enforced invariant rather than a convention this context is trusted to
  observe.
- **Commit-before-work.** `order-management` receives, allocates and
  releases in one call (its ADR 0005). Releasing before we have
  acknowledged would commit work to the floor for an order we may still
  reject, and `order-management` ADR 0004 makes release the cancellation
  boundary past which work cannot be clawed back. The companion record
  adds a hold — allocate without releasing — so rejection stays on the
  correct side of that boundary.

### Which program, and why

Three candidate integrations were considered. They are not variations;
they are different businesses:

- **Vendor Direct Fulfillment** — the network sends demand to a
  warehouse it does not own; that warehouse acknowledges, ships and
  confirms. **Chosen.** It is the only program in which *our building*
  is the fulfilling node and our capability is the product being sold.
- **External Fulfillment** — newer, shipment- and package-centric
  rather than purchase-order-centric, and arguably a closer structural
  fit. Rejected for now on maturity and documentation grounds, not on
  design grounds; §"Alternatives" records what would make us revisit.
- **Multi-Channel Fulfillment as a seller** — the network fulfils
  *for us*. This is the opposite direction (buying fulfilment rather
  than selling it) and belongs to a different decision entirely.

## Decision

We will create `network-fulfillment` as a new bounded context: a
**Supporting Subdomain**, **Conformist** to the network upstream, and an
**Anti-Corruption Layer** for everything downstream of it in this fleet.

### 1. Two aggregates

```
NetworkOrder
  networkRef        the network's own purchase-order identity
  requiredShipBy    the deadline that arrives WITH the demand
  lines             networkLineRef, networkProductId, quantity
  localOrderId      order-management's OrderId, once received there
  state             NEW -> ACKNOWLEDGED | REJECTED -> CONFIRMED
  acknowledgeBy     networkRef's arrival + 24h — the SLA instant

  invariants
   - acknowledged in full or rejected in full; never partially
   - an acknowledgement may never exceed the quantity demanded
   - no shipment confirmation before acknowledgement
```

```
CapabilityOffer
  sku, siteId
  advertisedQuantity
  basis             Physical | ThroughputConstrained
  computedAt

  invariant
   - advertisedQuantity <= physicalAvailable, always
```

`CapabilityOffer.basis` is not decoration. An offer throttled below
physical stock is a deliberate, explainable act — "we are advertising
120 of 400 because the path cannot carry more before the cutoff" — and
recording which rule produced the number is what makes it auditable
rather than a silent haircut. This mirrors the discipline
`order-management`'s `PromiseBasis` already establishes for promises.

### 2. Conformist upstream, Anti-Corruption Layer downstream

The network's vocabulary stops at this context's outbound adapter.
`order-management` receives demand as SKUs, quantities and a deadline,
and never learns what a purchase order, an ASIN, an acknowledgement
code or a selling party is. The translation table
(`networkProductId -> SKU`, `acknowledgementStatus -> our order state`)
lives here and only here.

**Correlation is translated explicitly, never assumed.** This fleet has
already been bitten once by an identifier that looked like an order
reference and was not: `fulfillment-execution`'s `order_ref` carries
`wes-work-planning`'s per-LINE `WorkUnitId`, not `order-management`'s
`OrderId` (see `order-management` ADR 0018). A network purchase-order
line is one hop further out and the same class of mistake is available.
`NetworkOrder` therefore stores `localOrderId` as an explicit, persisted
mapping rather than deriving it by string convention.

### 3. Customer PII stops here

Ship-to name, address and phone are held by this context and by nothing
else in the fleet. `order-management` receives demand without them. The
address reaches a carrier label from this context directly, at label
time.

Consequently this is the first context in the fleet that genuinely
cannot run unauthenticated, and the first that must treat its
persistence and its logs as PII-bearing. That is a real cost and it is
the price of the boundary being drawn in the right place; it is also
strictly smaller than the alternative, which is PII in the aggregate
four contexts read.

### 4. Every network call goes through one adapter, with a mode switch

```
NETWORK_MODE = live | sandbox | stub        default: stub
```

`stub` is a deterministic in-process implementation: no credentials, no
egress. The kind cluster, `e2e-tests` and every CI job run in `stub` and
must never need a credential. `sandbox` targets the network's own
sandbox environment. `live` is opt-in per deployment.

This is the fleet's established `*_MODE` convention, with one lesson
applied from it: **every `*_MODE` in this fleet defaults to `permissive`
and several sat wrong in the cluster for weeks because nothing set
them.** The mode and its base URL must therefore be wired into
`warehouse-infra`'s `local.sync_edge_env` in the same change that
introduces them, and the binary must log the mode it chose at startup so
the running value can be verified rather than assumed.

### 5. Inbound is a poll; outbound submissions are asynchronous

The network offers no push to us, so demand arrives by polling
`getOrders` on an interval. Submissions (`submitAcknowledgement`,
`submitShipmentConfirmations`, `submitInvoice`) are accepted
asynchronously and reconciled afterwards via a transaction-status
query — the network's own guidance is to allow roughly 15 minutes for
an acknowledgement's status and 10 for a shipment's.

`NetworkOrder` therefore models submission as *pending until
reconciled*, never as done-on-HTTP-200. A submitted-but-unreconciled
acknowledgement is a distinct, visible state, because treating an
accepted-for-processing response as a completed commitment is exactly
how an integration silently misses the deadline it believes it met.

### 6. The acknowledgement clock is swept, and a miss is a reported fact

A recurring sweep finds `NetworkOrder`s approaching `acknowledgeBy`
without an acknowledgement and raises `AcknowledgementDeadlineAtRisk`.

Structurally this mirrors `fulfillment-execution`'s `SweepCPTMisses`
(its ADR 0025), including the property that made that design right: the
sweep **never mutates the aggregate**. A deadline at risk is a reported
fact, not a state transition, and re-firing on each pass is intended —
the condition is still true.

### 7. Feasibility is asked of `order-management`, never recomputed here

When demand arrives with a `requiredShipBy`, this context receives it
into `order-management` with `releaseOnAllocation=false`, reads the
resulting promise, and asks whether the building can meet the deadline
via that service's `PromisePolicy.FeasibleBy` (companion ADR 0020).

This context has the deadline and could consume the same three Kafka-fed
caches itself. It deliberately does not: duplicating ADR 0014's promise
math in a second repository guarantees the two copies drift, and the
promise is `order-management`'s responsibility — the same reasoning
ADR 0014 used when it declined to move promise computation into
`wes-work-planning`, which likewise had the data.

The acknowledgement then follows from states `order-management` already
produces, rather than from a decision procedure invented here:

```
allocated and deadline-feasible   -> accept in full
line backordered / out of stock   -> reject, out-of-stock reason
unknown product identifier        -> reject, invalid-SKU reason
not feasible by the deadline      -> reject, capability reason
```

### 8. Capacity-aware advertised availability

`CapabilityOffer` is recomputed from three Kafka-fed local caches —
inventory availability, `CPTScheduleChanged`, `PathCapacityChanged` —
using the same cache-and-replay pattern the fleet already uses for the
process-path catalogue, and published outward on a schedule.

One operational rule from hard-won fleet experience applies directly:
**a consumer that replays a topic from the first offset on every start
must use a consumer group id unique per process instance.** A shared
group id means a fresh process resumes from an earlier instance's
committed offset and can be marked ready holding an empty cache. This
has bitten this fleet before, in two different services, and is cheap to
get right at the start.

### Alternatives considered

- **An outbound adapter inside `order-management`.** Fewer moving parts,
  no new repository. Rejected: it puts a Conformist translation, the
  fleet's only PII, and the fleet's only external SLA inside the
  aggregate four other contexts read. The four reasons in §Context are
  each individually sufficient.
- **Publish the CPT schedule and cycle times outward as an API for the
  network to consume.** This is the literal reading of "expose planning
  time to the network provider", and it was the starting intent.
  Rejected on evidence: no such consumer endpoint exists in the
  network's API surface. Capability is inferred from inventory,
  acknowledgement and confirmation accuracy, so those three signals are
  the product.
- **Advertise physical stock, like everyone else.** Simplest, and what a
  naive integration does. Rejected: it discards the one thing this fleet
  knows that a stock ledger does not, and it is penalised by the network
  precisely to the extent that it is wrong.
- **External Fulfillment instead of Vendor Direct Fulfillment.**
  Structurally closer to our model (shipment/package-centric, explicitly
  aimed at third-party warehouses fulfilling network orders). Deferred
  on maturity, not on design. We revisit if its documentation and
  availability mature, or if the purchase-order grain proves a poor fit
  for how demand actually arrives; `NetworkOrder`'s own vocabulary is
  deliberately program-neutral so that swap is an adapter change.
- **Skip the hold; release optimistically and cancel on rejection.**
  Rejected in the companion record for the same reason repeated here:
  `order-management` ADR 0004 makes release the cancellation boundary
  and documents no-clawback as a known gap. This would make an
  unrecoverable event routine, to satisfy an external party's deadline.

## Consequences

**Easier**

- The fleet can sell fulfillment capability to an external network
  without any other context learning that the network exists.
- Advertised availability becomes honest in a way most integrations of
  this shape are not, using signals the fleet already publishes.
- The accept/reject decision is derived from real building capability
  rather than from optimism, and the reason for every rejection is a
  domain fact rather than a guess.
- Program choice stays reversible: the aggregates are named in our
  vocabulary, so switching to a different network program is an adapter
  change, not a redesign.

**Harder**

- This context holds credentials and customer PII. It cannot run
  unauthenticated, which makes it the first exception to the fleet's
  current posture, and its logs and database need care no other service
  here needs today.
- Polling has a latency floor and a rate-limit ceiling, and the 24-hour
  clock runs against wall time regardless of whether our poller is
  healthy. A poller outage is an SLA breach, not a delayed batch.
- Asynchronous submission means every outbound call has two outcomes
  separated in time, and the reconciliation step is easy to skip and
  expensive to have skipped.
- Network demand is ship-complete while ordinary demand is not. Two
  demand classes with genuinely different promising rules now coexist,
  and the difference must stay enforced rather than remembered.
- An order held in `order-management` pending our acknowledgement is
  holding real inventory reservations. The companion record names the
  orphaned-hold sweep as an open gap; this context owns the clock that
  bounds it, and must not let that gap become permanent.

**Deliberately out of scope for this record**

- Invoicing and payments. They follow shipment confirmation and add no
  new domain concept; they are a later phase.
- Returns.
- Any change to how ordinary customer demand is promised. ADRs 0014–0019
  in `order-management` stand unmodified.
- Modelling which site an order ships from. `order-management` ADR 0014's
  single-site simplification still holds and this context inherits it.

## Rollout

Each step is its own feature branch and PR into `develop`, CI green
before merge, in this order:

1. **This ADR and its companion**, accepted together (docs only).
2. **`order-management`:** `BasisNetwork` and `PromisePolicy.FeasibleBy`
   (pure domain), then `releaseOnAllocation` + `ReleaseHeldOrder` + the
   ship-complete invariant. Per its ADR 0020's own rollout.
3. **This context, skeleton:** hexagonal scaffold matching the fleet's
   shape, `NetworkOrder`, the ACL, `NETWORK_MODE=stub`. No live network
   call anywhere in this step.
4. **`CapabilityOffer` and capacity-aware advertised availability** —
   the actual value, still entirely in `stub`.
5. **The acknowledgement loop against the network's sandbox**, including
   transaction-status reconciliation and the deadline sweep.
6. **Shipment confirmation**, driven by `fulfillment-execution`'s
   existing `PackageManifested` event.
7. **`e2e-tests`:** "a saturated path reduces advertised availability
   below physical stock", and "demand whose required ship-by precedes
   the earliest feasible cutoff is rejected rather than accepted".
   Neither can pass by accident.
