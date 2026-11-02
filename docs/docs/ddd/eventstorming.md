# EventStorming (design level)

Uses the ddd-crew [EventStorming glossary & cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet)
notation. Each sticky is backed by code. The hotspots are real gaps,
taken from ADRs or from the code itself, and none is invented.

## Legend

```mermaid
flowchart LR
    A["Actor"]:::actor
    C["Command"]:::command
    G["Aggregate"]:::aggregate
    E["Domain Event"]:::event
    P["Policy"]:::policy
    R["Read Model"]:::readmodel
    X["External System"]:::external
    H["Hotspot"]:::hotspot
    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#fff
    classDef aggregate fill:#f7d84a,stroke:#b59f00,color:#000
    classDef event fill:#f6a04d,stroke:#b5651d,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#b03a2e,color:#000
    classDef hotspot fill:#e74c3c,stroke:#922b21,color:#fff
```

Source: the ddd-crew EventStorming notation only; the legend has no code behind
it. The code files behind every sticky are named under each section's diagram.

## 1. Demand intake and answer

```mermaid
flowchart LR
    T["Poll timer<br/>POLL_INTERVAL"]:::actor
    NW["retail-network<br/>stub gateway"]:::external
    C1["Receive network demand"]:::command
    POL0["Policy: answered or overdue NetworkRef<br/>returns existing order,<br/>an unanswered NEW one resumes"]:::policy
    PT["Product translation<br/>dictionary"]:::readmodel
    G1["NetworkOrder"]:::aggregate
    E1["NetworkOrderReceived"]:::event
    OM["order-management"]:::external
    C2["Raise held order"]:::command
    POL1["Policy: unknown product<br/>rejects whole order"]:::policy
    POL2["Policy: null promiseDate<br/>means infeasible"]:::policy
    C3["Submit acceptance"]:::command
    C4["Reject"]:::command
    E2["NetworkOrderSubmitted"]:::event
    E3["NetworkOrderRejected"]:::event
    C5["Cancel held order"]:::command
    POL4["Policy: record the hold on the order<br/>the moment it exists<br/>(decided 2026-10-17, ADR 0024)"]:::policy

    T --> C1
    NW --> C1
    C1 --> POL0
    C1 --> PT
    PT --> G1
    C1 --> G1
    G1 --> E1
    PT --> POL1
    POL1 --> C4
    E1 --> C2
    C2 --> OM
    OM --> POL2
    POL2 -->|feasible| C3
    POL2 -->|infeasible| C5
    C5 --> C4
    C3 --> G1
    C4 --> G1
    G1 --> E2
    G1 --> E3
    C2 --> POL4
    POL4 --> G1

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#fff
    classDef aggregate fill:#f7d84a,stroke:#b59f00,color:#000
    classDef event fill:#f6a04d,stroke:#b5651d,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#b03a2e,color:#000
    classDef hotspot fill:#e74c3c,stroke:#922b21,color:#fff
```

Source: `internal/adapters/inbound/poller/poller.go`,
`internal/application/usecases/receive_network_demand.go`,
`internal/adapters/outbound/ordermanagement/planner.go`,
`internal/domain/networkorder/network_order.go`.

Omitted: the submission to the network that follows each answer
(`SubmitAcknowledgement(ref, true|false)`), shown in
[sequence-diagrams.md](/docs/ddd/sequence-diagrams); and the outbox write. The
ex-hotspot "crash after raising hold leaves a NEW order with no localOrderId"
is resolved by recording the hold right after the raise and by the
orphaned-hold sweep (diagram 2); what remains open is listed in the sticky
inventory.

## 2. Reconciliation and the acknowledgement clock

```mermaid
flowchart LR
    T1["Poll timer"]:::actor
    T2["Sweep timer<br/>SWEEP_INTERVAL"]:::actor
    RM1["Submitted orders<br/>ListSubmitted"]:::readmodel
    RM2["Unanswered orders<br/>ListUnanswered"]:::readmodel
    NW["retail-network<br/>SubmissionStatus"]:::external
    POL1["Policy: SUCCESS settles,<br/>FAILURE rejects, PENDING waits"]:::policy
    C1["Confirm acknowledgement"]:::command
    C2["Release held order"]:::command
    C3["Cancel held order"]:::command
    C4["Reject"]:::command
    G1["NetworkOrder"]:::aggregate
    E3["NetworkOrderRejected"]:::event
    E5["NetworkOrderAcknowledged v2<br/>(decided 2026-10-06, ADR 0016)"]:::event
    POL2["Policy: overdue NEW order<br/>is reported, never mutated"]:::policy
    E4["AcknowledgementDeadlineAtRisk"]:::event
    POL3["Policy: overdue NEW order<br/>is refused, never answered late"]:::policy
    T3["Orphan sweep timer<br/>SWEEP_INTERVAL"]:::actor
    RM3["Open holds<br/>ListOpenHolds"]:::readmodel
    POL4["Policy: an open hold past<br/>acknowledgeBy + grace is cancelled,<br/>the order state is never touched<br/>(decided 2026-10-17, ADR 0024)"]:::policy
    E6["OrphanedHoldCancelled"]:::event
    OM["order-management"]:::external
    H5["Hotspot: at-risk fires only after<br/>the deadline, ADR says approaching"]:::hotspot

    T1 --> RM1
    RM1 --> NW
    NW --> POL1
    POL1 -->|SUCCESS| C1
    C1 --> G1
    G1 --> E5
    C1 --> C2
    C2 --> OM
    POL1 -->|FAILURE| C3
    C3 --> OM
    C3 --> C4
    C4 --> G1
    G1 --> E3
    T2 --> RM2
    RM2 --> POL2
    POL2 --> E4
    RM2 --> POL3
    POL3 --> C3
    T3 --> RM3
    RM3 --> POL4
    POL4 --> C3
    POL4 -->|cancelled or gone| E6
    E4 --- H5

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#fff
    classDef aggregate fill:#f7d84a,stroke:#b59f00,color:#000
    classDef event fill:#f6a04d,stroke:#b5651d,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#b03a2e,color:#000
    classDef hotspot fill:#e74c3c,stroke:#922b21,color:#fff
```

Source: `internal/application/usecases/reconcile_submitted_orders.go`,
`sweep_acknowledgement_deadlines.go`, `reject_overdue_orders.go`,
`sweep_orphaned_holds.go`;
`internal/adapters/outbound/analyticsstore/postgres_projection.go`;
`docs/adr/0001-network-fulfillment-bounded-context.md` §5, §6.

Omitted: the per-order "skip on error, retry next pass" behaviour of all
three passes (four, with the orphaned-hold sweep). The overdue reject cancels the held order only when a
hold is recorded and still open. The orphaned-hold sweep also reads `409`/`422`
as "already released" (outcome recorded, no event) and `404` as "gone"; a 5xx or
an open breaker leaves the hold for the next pass.

## 3. Shipment confirmation

```mermaid
flowchart LR
    OP["Operator or upstream caller"]:::actor
    C1["Confirm shipment<br/>POST shipment-confirmation"]:::command
    POL1["Policy: already CONFIRMED<br/>returns unchanged"]:::policy
    G1["NetworkOrder"]:::aggregate
    E1["NetworkOrderShipmentConfirmed"]:::event
    NW["retail-network<br/>SubmitShipmentConfirmation"]:::external
    FE["fulfillment-execution<br/>PackageManifested"]:::external
    H1["Hotspot: no persisted WorkUnitId to<br/>NetworkRef mapping, ADR 0014"]:::hotspot

    OP --> C1
    C1 --> POL1
    C1 --> G1
    G1 --> E1
    E1 --> NW
    FE -.- H1
    H1 -.- C1

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#fff
    classDef aggregate fill:#f7d84a,stroke:#b59f00,color:#000
    classDef event fill:#f6a04d,stroke:#b5651d,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef external fill:#f1948a,stroke:#b03a2e,color:#000
    classDef hotspot fill:#e74c3c,stroke:#922b21,color:#fff
```

Source: `internal/application/usecases/confirm_network_order_shipment.go`,
`internal/adapters/inbound/http/server.go` (`handleConfirmShipment`),
`internal/adapters/inbound/http/errors.go` (`ErrConfirmBeforeAcknowledge`
maps to 409 `confirm-before-acknowledge`), `docs/adr/0014-explicit-shipment-confirmation-endpoint.md`.

Omitted: the 404 branch for an unknown `networkRef`.

## 4. Capability offer recompute

```mermaid
flowchart LR
    T["Recompute timer<br/>RECOMPUTE_INTERVAL"]:::actor
    PT["Known SKUs<br/>translation dictionary"]:::readmodel
    INV["inventory-storage<br/>GET usable"]:::external
    PPM["process-path-management<br/>events"]:::external
    WWP["wes-work-planning<br/>events"]:::external
    RM1["Process-path capability cache<br/>NextCutoff"]:::readmodel
    RM2["Path capacity cache<br/>RemainingCapacity"]:::readmodel
    C1["Compute capability offer"]:::command
    POL1["Policy: unknown capacity<br/>falls back to physical"]:::policy
    POL2["Policy: path eligible only if CycleTimeP95<br/>≤ cutoff − now; no data stays eligible<br/>(decided 2026-10-06, ADR 0017)"]:::policy
    G1["CapabilityOffer"]:::aggregate
    RM3["Capability offers<br/>GET /capability-offers"]:::readmodel
    H1["Hotspot: offer reaches the network only<br/>in live mode, via FeedInventory (ADR 0027 part 2)"]:::hotspot
    H3["Hotspot: single SITE_ID only"]:::hotspot

    PPM --> RM1
    WWP --> RM2
    T --> C1
    PT --> C1
    INV --> C1
    RM1 --> POL2
    POL2 --> C1
    RM2 --> C1
    C1 --> POL1
    POL1 --> G1
    G1 --> RM3
    RM3 --- H1
    C1 --- H3

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#fff
    classDef aggregate fill:#f7d84a,stroke:#b59f00,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#b03a2e,color:#000
    classDef hotspot fill:#e74c3c,stroke:#922b21,color:#fff
```

Source: `internal/application/usecases/recompute_capability_offers.go`,
`internal/domain/capabilityoffer/capability_offer.go`,
`internal/adapters/outbound/{processpathcache,pathcapacitycache,inventoryclient}/`,
`cmd/netfulfil/main.go` (`wireCapabilityOffer`, `siteId`).

Omitted: the boot-time `WaitReady` gate, which the always-on capability-offer
pipeline blocks on. `CapabilityOffer` raises no domain
event, so there is no orange sticky.

## Sticky inventory

| Sticky | Kind | Code evidence |
| --- | --- | --- |
| Poll / sweep / recompute timers | Actor | `poller.Run`, `runSweep`, `runRejectOverdue`, `runSweepOrphanedHolds`, `runReconcile` in `cmd/netfulfil/workers.go`; `runRecompute` in `cmd/netfulfil/capability.go` |
| Operator or upstream caller | Actor | `POST /network-orders/{networkRef}/shipment-confirmation` |
| Receive network demand | Command | `ReceiveNetworkDemand.Execute` |
| Raise / release / cancel held order | Command | `FulfillmentPlanner.RaiseHeldOrder` / `ReleaseHeldOrder` / `CancelHeldOrder` |
| Submit acceptance | Command | `NetworkOrder.Submit` + `LinkLocalOrder` |
| Record / settle the hold | Command | `NetworkOrder.RecordHold` / `SettleHold` |
| Reject | Command | `NetworkOrder.Reject` |
| Confirm acknowledgement | Command | `NetworkOrder.ConfirmAcknowledgement` |
| Confirm shipment | Command | `NetworkOrder.ConfirmShipment`, `ConfirmNetworkOrderShipment` |
| Compute capability offer | Command | `capabilityoffer.Compute` |
| NetworkOrder | Aggregate | `internal/domain/networkorder` |
| CapabilityOffer | Aggregate | `internal/domain/capabilityoffer` |
| NetworkOrderReceived / Submitted / Acknowledged / Rejected / ShipmentConfirmed / AcknowledgementDeadlineAtRisk / OrphanedHoldCancelled | Domain Event | `internal/domain/shared/events.go` |
| Already-answered or overdue NetworkRef returns existing order; an unanswered one resumes | Policy | `ReceiveNetworkDemand.Execute` (`FindByRef`, `resume`) |
| Unknown product rejects whole order | Policy | `ReceiveNetworkDemand.translate` → `rejectUntranslatable` |
| Null promiseDate means infeasible | Policy | `ordermanagement.Planner.RaiseHeldOrder` |
| SUCCESS / FAILURE / PENDING | Policy | `ReconcileSubmittedOrders.Execute` |
| Overdue reported, never mutated | Policy | `SweepAcknowledgementDeadlines` |
| Overdue refused, never answered late | Policy | `RejectOverdueOrders` |
| Open hold past acknowledgeBy + grace is cancelled, state untouched | Policy | `SweepOrphanedHolds`, `NetworkOrder.HoldOrphaned` (ADR 0024) |
| Hold recorded the moment it exists | Policy | `ReceiveNetworkDemand.answer` → `NetworkOrder.RecordHold` (ADR 0024) |
| Already CONFIRMED returns unchanged | Policy | `ConfirmNetworkOrderShipment.Execute` |
| Unknown capacity falls back to physical | Policy | `capabilityoffer.Compute` (`throughputKnown=false`) |
| Product translation dictionary | Read Model | `memory.ProductTranslation` (`PRODUCT_TRANSLATION_FILE`) |
| Submitted / unanswered orders | Read Model | `NetworkOrderRepo.ListSubmitted` / `ListUnanswered` |
| Open holds | Read Model | `NetworkOrderRepo.ListOpenHolds` (partial index `network_orders_open_holds`) |
| Process-path capability cache | Read Model | `processpathcache.Consumer.NextCutoff` |
| Path capacity cache | Read Model | `pathcapacitycache.Consumer.RemainingCapacity` |
| Capability offers | Read Model | `CapabilityOfferRepo.ListAll`, `GET /capability-offers` |
| retail-network | External System | `ports.NetworkGateway`, `network.StubGateway` |
| order-management | External System | `internal/adapters/outbound/ordermanagement` |
| inventory-storage | External System | `internal/adapters/outbound/inventoryclient` |
| process-path-management, wes-work-planning | External System | the two Kafka caches |
| fulfillment-execution | External System | named in ADR 0014 as the deliberately unused trigger |
| Decided 2026-10-06: Submitted at SUBMITTED, Acknowledged only on settle (ADR 0016) | Policy | `ReceiveNetworkDemand.acknowledge` publishes `NetworkOrderSubmitted`; `ReconcileSubmittedOrders.confirm` publishes `NetworkOrderAcknowledged` (v2) with the save (ex-hotspots "Acknowledged at SUBMITTED" and "No event on settle") |
| Orphaned hold after crash | Hotspot, resolved 2026-10-17 (ADR 0024, gap OM-0020) | The hold id is recorded right after `RaiseHeldOrder` (`RecordHold`) and `SweepOrphanedHolds` cancels the ones nobody decided, so `RejectOverdueOrders`'s cancel now has a hold to find. Still open, on purpose (ADR 0024): a hold never named because the process stopped between order-management's commit and the id being saved, if the window closes before a re-delivery repeats the raise; a `SUBMITTED` order whose network status stays `PENDING`; an `ACKNOWLEDGED` order whose release failed (not a cancel case); rows rejected before migration `0005` whose raise had succeeded |
| SUBMISSION_FAILED counted | Policy | `PostgresProjection.ApplyNetworkOrderRejected` increments `orders_rejected_submission_failed` (analytics migration 0002, ADR 0015; resolved 2026-10-06) |
| At-risk only after the deadline | Hotspot | `AcknowledgementOverdue` vs ADR 0001 §6 "approaching" |
| No WorkUnitId → NetworkRef mapping | Hotspot | ADR 0014 |
| Confirm-before-acknowledge → 409 | Policy | `errors.go` maps `ErrConfirmBeforeAcknowledge` to 409 `confirm-before-acknowledge` (resolved 2026-10-06) |
| Offer never submitted outward | Hotspot | `RecomputeCapabilityOffers` doc comment; ADR 0001 §8 "published outward on a schedule" |
| Decided 2026-10-06: path eligible only if CycleTimeP95 ≤ cutoff − now, missing/zero stays eligible (ADR 0017) | Policy | `RecomputeCapabilityOffers.throughputFeasible` / `cycleTimeFits` (ex-hotspot "CycleTimeP95 unused") |
| Single SITE_ID | Hotspot | `RecomputeCapabilityOffers.SiteId`; ADR 0001 Consequences (single-site inherited) |
