---
id: subdomain-classification
title: Subdomain classification
sidebar_label: Subdomain classification
---

# Subdomain classification

**network-fulfillment is a Supporting subdomain, in the `wes` tier.**

- Classification source: ADR 0001 (Decision: "a Supporting Subdomain"), this
  repo's [Core Domain Chart](core-domain-chart.md) and
  [Bounded Context Canvas](bounded-context-canvas.md), and the fleet table in
  `warehouse-docs` (`docs/strategic-design/subdomain-classification.md` on
  `main`), which files it as Supporting.
- Tier source: `Subdomain = "wes"` in
  `internal/adapters/kafka/cloudevents/cloudevents.go`, so every event type it
  publishes starts `com.warehouse.wes.network-fulfillment.`. The tier
  (`wms`/`wes`) is a grouping of the event namespace, not the
  Core/Supporting/Generic verdict.

## Why Supporting

**Not Core.** The business needs this context to sell the building's
capability through an external retail network, but the knowledge that
decides what can be sold belongs to other contexts:

- Deadline feasibility is asked of `order-management`
  (`ports.FulfillmentPlanner.RaiseHeldOrder`) and never recomputed here
  (ADR 0001 §7). No use case in `internal/application/usecases` does
  arithmetic on cutoffs, capacities or cycle times except the opt-in
  capability offer, which sums figures other contexts publish.
- Cutoffs and cycle times come from `process-path-management`, path capacity
  from `wes-work-planning`, usable stock from `inventory-storage`.
- The acknowledgement protocol (one answer, in full, within 24 h) is the
  network's rule set, and the context conforms to it.
- The one new idea, `CapabilityOffer` (advertise
  `min(physicalAvailable, throughputFeasible)` instead of raw stock), is
  opt-in, single-site and not yet sent to the network
  (`SubmitAvailability` is never called), so it does not differentiate
  anything today.

**Not Generic.** It cannot be bought off the shelf: the Anti-Corruption Layer
dictionary, the held-order / answer / reconcile sequence with
order-management (whose ADR 0020 was written for this Customer), and the
24 h acknowledgement clock with its sweep and overdue rejection are specific
to this fleet.

**Where it could move.** The Core Domain Chart notes that if a live network
adapter ships and the throughput-constrained offer is actually submitted,
`CapabilityOffer` could become Decisive (short-term Core). That would need a
new ADR; nothing in the code puts it there now.

## Neighbours

Copied from the fleet table in `warehouse-docs`
(`docs/strategic-design/subdomain-classification.md`, `origin/main`, read
2026-10-09). Only contexts this one has an edge with are listed; see
[ecosystem/integration.md](../ecosystem/integration.md) for the edges.

| Context | Classification (fleet table) | Tier | Relationship to network-fulfillment |
| --- | --- | --- | --- |
| `order-management` | Generic/Supporting | `wes` | We are its Customer: held orders, feasibility, release and cancel over REST |
| `inventory-storage` | Core | `wms` | Usable quantity over REST for capability offers (opt-in) |
| `process-path-management` | Generic | `wes` | Path cycle times and the CPT schedule over Kafka (opt-in) |
| `wes-work-planning` | Core | `wes` | Remaining path capacity over Kafka (opt-in) |
| `fulfillment-execution` | Core | `wes` | No edge: shipment confirmation is an explicit REST call, not a `PackageManifested` consumer (ADR 0014) |

The counterpart network (played by `retail-network`, ADR 0009) is outside
the fleet's fifteen contexts and has no row in that table.
