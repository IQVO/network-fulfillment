---
id: 0002-retail-network-not-amazon-counterpart
slug: /adr/0002-retail-network-not-amazon-counterpart
title: "2. The counterpart is retail-network, our own ecosystem service — not a real external network's Selling Partner API"
sidebar_label: "2. retail-network, not the real network"
sidebar_position: 2
description: "ADR 0002 (Accepted) amends ADR 0001: the external retail fulfillment network this context is Conformist to is retail-network, a new service inside our own ecosystem playing that role — not an integration against a real e-commerce retailer's Selling Partner API. Renames the ACL adapter package, defers (not drops) a real SP-API adapter, and tightens the PII rule to zero ship-to data in this context at all."
---

# 2. The counterpart is retail-network, our own ecosystem service — not a real external network's Selling Partner API

## Status

**Accepted** (2026-09-26). Companion to `retail-network` ADR 0001 (drafted at
`docs/planning/retail-network-adr-0001-DRAFT-for-new-repo.md` in this
repo, pending `retail-network`'s own creation as a repository in Phase 2
of `2026-09-25_231500-retail-network-in-ecosystem.md`; move that file
verbatim into `retail-network`'s own `docs/docs/adr/` once that repo
exists). Neither is meaningful without the other: this record narrows
ADR 0001's "external retail fulfillment network" to a concrete,
in-ecosystem counterpart and changes this repo's own naming and PII rule
accordingly; `retail-network` ADR 0001 defines the organization this
record now points at. Raised together so the boundary can be accepted or
rejected as one decision.

## Context

### What ADR 0001 assumed, and what changed

This repo's [ADR 0001](./0001-network-fulfillment-bounded-context.md)
(Accepted 2026-09-23) established `network-fulfillment` as **Conformist**
to "an external retail fulfillment network (a major e-commerce retailer's
Selling Partner API, Vendor Direct Fulfillment)" and an **Anti-Corruption
Layer** for everything downstream in this fleet. Its reasoning — the
Conformist relationship, the PII boundary, the external SLA, the
throughput-constrained advertised-availability concept, fill-or-kill
versus per-shipment-group promising, commit-before-work versus a hold —
is all sound and **unchanged by this record**. What ADR 0001 assumed, and
this record corrects, is *which* network sits on the other side of that
boundary.

A prior planning pass
(`2026-09-25_224823-amazon-network-fulfillment-readiness.md`) took ADR
0001 at face value and scoped the next phase as a real integration
against the actual retailer's Selling Partner API: real vendor
credentials, a real sandbox environment, real product identifiers. The
user corrected this explicitly: **not the real one, our own ecosystem.**
The counterpart that issues purchase orders, watches our acknowledgement
clock, and measures our shipping accuracy is to be a **new service inside
this same fleet's orbit**, `retail-network`, built specifically to play
that real network's structural role — poll-driven demand, a 24-hour
fill-or-kill acknowledgement window, asynchronous submission-then-
reconciliation — in that new service's **own** vocabulary, not the real
network's.

### Why this is an amendment, not a rewrite

Every structural reason ADR 0001 gave for this being its own bounded
context still holds when the upstream is `retail-network` instead of a
real e-commerce retailer's network:

- **Still Conformist.** `retail-network`'s vocabulary (`poNumber`,
  `listingId`, `nodeId`, its own reason codes) is deliberately *not* this
  fleet's vocabulary, precisely so translation still has to happen for
  real (see `retail-network` ADR 0001, D2). This context has zero
  influence over `retail-network`'s contract, the same as it would have
  zero influence over a real retailer's.
- **Still the fleet's only PII-bearing context — except now there is
  strictly less PII here, not more.** In the real-network plan, ship-to
  name/address/phone would have had to reach *this* context to reach a
  carrier label. With `retail-network` as the counterpart, that PII never
  needs to cross into `network-fulfillment` at all: `retail-network`
  holds the (synthetic) shopper's address, requests a label from its own
  transit simulator, and hands this context only a `labelRef` and
  tracking number. This is a **strictly stronger** PII boundary than ADR
  0001 could achieve against a real network — see the tightened rule 2
  below.
- **Still an external SLA.** `retail-network`'s 24-hour fill-or-kill
  acknowledgement window is real to this context regardless of who
  operates the clock on the other end — a missed deadline still expires
  the purchase order and still costs this context's own scorecard
  standing, simulated or not.
- **Still asynchronous submission semantics.** `retail-network`'s
  `Submission` aggregate (`PROCESSING -> SUCCESS | FAILURE`) exists
  specifically so this repo's crash-safety and reconciliation work has
  something real to survive against, mirroring a real vendor program's
  own guidance on submission latency structurally, even though the actual
  delay is a configurable simulation knob (`TX_PROCESSING_DELAY`, default
  5s) instead of the real program's actual ~15 minutes.

### A real SP-API adapter is deferred, not dropped

Nothing about choosing `retail-network` as the near-term counterpart
forecloses a real integration against the actual retailer's Selling
Partner API later. The port this repo already sketched, `NetworkGateway`
(`PollDemand`, `SubmitAnswer`, `SubmitAvailability`, `DeclareCapability`,
`RequestLabel`, `SubmitShipmentConfirmation`, `SubmissionStatus`), is
written in this fleet's own vocabulary specifically so a real SP-API
adapter could be added later as a second implementation behind the same
port — an adapter swap, not a redesign.
`references/network-fulfillment-sp-api-boundary.md` (this fleet's own
operating skill) already carries the real finding that would matter if
that work is ever picked up: no SP-API operation accepts a capability
declaration, so a real integration would need to fall back to
inference-only (dropping `DeclareCapability`'s live effect, not the call
itself). That reference stays valid and is not superseded by this
record — it describes the real Selling Partner API, which is simply not
being built against right now.

## Decision

We amend ADR 0001 as follows. ADR 0001's Decision section (the two
aggregates, the ACL, the mode switch, the crash-safe lifecycle, the
sweep, the feasibility delegation to `order-management`) is otherwise
unchanged and still governs.

### 1. The counterpart is named: `retail-network`

Every place ADR 0001 or this repo's own docs said "an external retail
fulfillment network (a major e-commerce retailer's Selling Partner API,
Vendor Direct Fulfillment)" now means, concretely: `retail-network`, a
bounded context in this same fleet's GitHub organization, built to play
that structural role, reachable only over its own Vendor API
(`GET /vendor/purchase-orders`, `POST /vendor/acknowledgements`, etc. —
see `retail-network` ADR 0001), never over a `warehouse.*` Kafka topic
and never via a direct database read. `retail-network` is, deliberately,
still **a separate organization** from this fleet for every purpose this
context's own ADR 0001 already cared about (Conformist relationship,
translation boundary, PII isolation) — it is simply an organization we
also happen to operate.

### 2. Adapter package renamed: `outbound/network/` -> `outbound/retailnetwork/`

AGENTS.md's current rule 1 and the repo's actual tree both name the ACL
adapter `internal/adapters/outbound/network/`. That name is imprecise now
that the counterpart is concretely `retail-network`, and the package is
renamed to say so directly:

```
internal/adapters/outbound/retailnetwork/     was outbound/network/
```

This is the only package that knows `poNumber`, `listingId`, `nodeId` or
`retail-network`'s reason codes — the direct successor of ADR 0001's "the
only package that knows `purchaseOrderNumber`, ASIN, or the real
network's acknowledgement codes." The rename covers the whole current
`outbound/network/` package (`NETWORK_MODE` switch, `StubGateway`,
`NETWORK_SEED_FILE` loader) as one move in the Phase 3 implementation PR
that also adds the first live gateway implementation calling
`retail-network`'s real Vendor API — this ADR fixes the name the
Phase 3 PR must land under, it does not itself move any code.

### 3. PII rule tightened: zero ship-to data reaches this context, period

ADR 0001's rule 2 said "Customer PII stops here" — meaning ship-to
name/address/phone would live in `network-fulfillment` and travel no
further inward. With `retail-network` as the counterpart, that
data never needs to arrive in `network-fulfillment` **at all**:
`retail-network` holds the (synthetic) shopper and their `shipTo` inside
its own `CustomerOrder` aggregate, and this context's `NetworkOrder`
receives a `poNumber`, line items, quantities and a `requiredShipBy` —
never a name, address or phone number, in either direction. When this
context calls `retail-network`'s `POST /vendor/shipping-labels`, it gets
back `{labelRef, trackingNumber, carrier}` and nothing else; the address
that label encodes stays inside `retail-network`.

**AGENTS.md rule 2 is replaced, not merely reworded:**

```
Old (ADR 0001): "Customer PII stops here." (this context holds it)
New (ADR 0002): "No ship-to PII reaches this context at all." (this
                 context never holds it; retail-network does)
```

The practical consequence: this context no longer needs to be "the first
context in the fleet that genuinely cannot run unauthenticated" for PII
reasons — it holds none. (It still runs unauthenticated for the same
fleet-wide reason every other context does, per the standing no-auth
decision — see `retail-network` ADR 0001's D6 for the twin decision on
the other side of this boundary. This is a convergence, not a new
argument: both contexts land on no-auth, for slightly different but
compatible reasons — `network-fulfillment` because it never holds PII,
`retail-network` because what PII it holds is synthetic and contained.)

### 4. `NETWORK_MODE=stub|live` — `sandbox` removed

ADR 0001 and this repo's AGENTS.md both specified
`NETWORK_MODE=live|sandbox|stub`. `sandbox` only ever meant "a real
vendor program's own sandbox environment" and has no referent now that
the counterpart is `retail-network`, which has no separate sandbox
tier — its deployed instance in the kind cluster **is** the only
instance, well-behaved by default (every simulation knob defaults to
unstressed) and stressed only by explicit `e2e-tests` configuration of
its own knobs (`TX_FAILURE_RATE`, `RATE_LIMIT_RPS`,
`PO_REDELIVERY_RATE`). The mode switch is therefore:

```
NETWORK_MODE = live | stub        default: stub
```

`live` means `NETWORK_BASE_URL` points at the deployed `retail-network`
service. `stub` remains the default for unit tests and bare local runs,
unchanged from ADR 0001.

### 5. Everything else in ADR 0001 stands

The two aggregates (`NetworkOrder`, `CapabilityOffer`), the
Conformist/ACL relationship shape, the crash-safe lifecycle states, the
acknowledgement-deadline sweep, the delegation of feasibility math to
`order-management`'s `PromisePolicy.FeasibleBy`, and every "hard rule"
in AGENTS.md not explicitly amended above (correlation as a persisted
mapping, ship-complete demand, per-process-unique Kafka consumer groups,
asynchronous submissions modeled as pending-until-reconciled) are
unchanged by this record.

## Consequences

**Easier**

- This repo's crash-safety, reconciliation and throughput-constrained-
  availability work becomes testable against a counterpart that can
  actually fail, delay, rate-limit and redeliver on purpose — none of
  which `StubGateway` or a real, well-behaved vendor sandbox would ever
  do routinely.
- The PII boundary gets strictly simpler: this context never receives
  ship-to data in the first place, rather than receiving it and being
  trusted to keep it from leaking further inward.
- The mode switch loses a state (`sandbox`) that never had a real
  implementation to switch to.

**Harder**

- This repo's own historical docs (ADR 0001's text, `README.md`'s
  framing, existing prose referencing a major e-commerce retailer's
  Selling Partner API as the *actual* near-term integration target) now
  needs care when read: ADR 0001's *reasoning* stands, but its literal
  upstream description is superseded by this record for the near term.
  Future readers should read ADR 0001 for the *shape* of the boundary and
  this ADR for *who* is actually on the other side of it today.
- A real SP-API adapter, if ever built, is now explicitly a second, later
  effort behind the same `NetworkGateway` port — deferred work that must
  not be silently assumed done because `retail-network` exists and looks
  similar.

**Deliberately out of scope for this record**

- Any change to `network-fulfillment`'s own aggregates, use cases, or
  crash-safe lifecycle — ADR 0001 already specifies those and this
  record does not touch them.
- Scaffolding the `retail-network` repository itself — that is Phase 2 of
  `2026-09-25_231500-retail-network-in-ecosystem.md`, a separate,
  future PR.
- Reversing the deferral of a real SP-API adapter into an outright
  cancellation. It stays available as a future adapter swap.

## Rollout

This ADR and its companion (`retail-network` ADR 0001) are accepted or
rejected together, docs only, per the parent plan's Phase 1. Once
accepted, this repo's own AGENTS.md is updated in the same PR (rule 1 and
rule 2, as specified in the Decision section above) so the working
convention matches the accepted record immediately — not left to drift
until the Phase 3 implementation PR that will actually create
`internal/adapters/outbound/retailnetwork/`.
