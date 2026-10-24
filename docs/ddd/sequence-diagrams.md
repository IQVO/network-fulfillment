# Sequence diagrams

UML sequence diagrams for every command use case, derived from the use-case
function bodies in `internal/application/usecases/`. `UoW` is
`ports.UnitOfWork`, which is `postgres.UnitOfWork` (ADR 0025: Postgres is the only runtime;
unit tests substitute a fake). `Publisher` is the wired
`ports.EventPublisher`, the outbox publisher, which writes `outbox_events`
rows for both topics inside the transaction.

There is no idempotency-key middleware on this context's own routes and no
optimistic-concurrency
version check anywhere in this context. Idempotency comes from the state
checks shown below. (The one `Idempotency-Key` in play is outbound: the raise
to order-management carries `netful-raise-<networkRef>`.)

## 1. ReceiveNetworkDemand (poller)

```mermaid
sequenceDiagram
    autonumber
    participant P as poller.Poller
    participant GW as NetworkGateway (stub)
    participant UC as ReceiveNetworkDemand
    participant R as NetworkOrderRepo
    participant TR as ProductTranslation
    participant A as NetworkOrder
    participant U as UoW
    participant PUB as Publisher
    participant OM as FulfillmentPlanner (order-management)
    P->>GW: PollDemand(since)
    GW-->>P: InboundDemand list
    loop each demand
        P->>UC: Execute(demand)
        UC->>R: FindByRef(networkRef)
        alt known, answered or overdue
            R-->>UC: existing order
            UC-->>P: existing order, no new answer
        else known, still NEW inside its window
            R-->>UC: existing order
            Note over UC: resumes at RaiseHeldOrder, the same Idempotency-Key replays the same order
        else new demand
            UC->>TR: ToSKU(productId) per line
            alt ErrUnknownProduct
                UC->>A: ReceiveUntranslatable(...)
                UC->>U: Execute: Save + Publish NetworkOrderReceived lineCount 0
                UC->>A: Reject()
                UC->>U: Execute: Save + Publish NetworkOrderRejected UNTRANSLATABLE_SKU
                UC->>GW: SubmitAcknowledgement(ref, false)
            else all lines translated
                UC->>A: Receive(ref, siteId, requiredShipBy, lines, now)
                UC->>U: Execute: Save NEW + Publish NetworkOrderReceived
                UC->>OM: RaiseHeldOrder(siteId, requiredShipBy, SKUQuantities)
                OM-->>UC: LocalOrderId, Feasible
                UC->>A: RecordHold(localOrderId)
                UC->>R: Save, hold recorded and order still NEW
                alt not feasible
                    UC->>OM: CancelHeldOrder(localOrderId)
                    UC->>A: SettleHold(CANCELLED)
                    UC->>A: Reject()
                    UC->>U: Execute: Save REJECTED + Publish NetworkOrderRejected INFEASIBLE_DEADLINE
                    UC->>GW: SubmitAcknowledgement(ref, false)
                else feasible
                    UC->>A: Submit()
                    UC->>A: LinkLocalOrder(localOrderId)
                    UC->>U: Execute: Save SUBMITTED + Publish NetworkOrderSubmitted
                    UC->>GW: SubmitAcknowledgement(ref, true)
                    Note over UC,OM: no Acknowledged event and no release here, ReconcileSubmittedOrders does both on settle
                end
            end
            UC-->>P: order or error
        end
    end
    Note over P: watermark advances only if no demand failed
```

Source: `internal/application/usecases/receive_network_demand.go`,
`internal/adapters/inbound/poller/poller.go`,
`internal/application/usecases/unit_of_work.go`.

Omitted: the error branches return early. A failed `RaiseHeldOrder` returns
`raise held order: ...` with the order already saved `NEW`; the next poll finds
it still `NEW` inside its window and resumes (first `alt` above). A failed
`CancelHeldOrder` returns `cancel held order: ...` with the hold already
recorded, so the order stays `NEW` and the poller, `RejectOverdueOrders` and
`SweepOrphanedHolds` can all still free it. A failed submit
returns `submit acknowledgement` or `submit rejection` after the record has
committed. Any error leaves the demand to be re-polled; the stub keeps it
pending until `SubmitAcknowledgement`. A non-`ErrUnknownProduct`
translation error aborts without saving anything. The untranslatable branch
and a lineless resumed order call no order-management method, so they have no
hold to free.

## 2. ReconcileSubmittedOrders (ticker, POLL_INTERVAL)

```mermaid
sequenceDiagram
    autonumber
    participant T as runReconcile ticker
    participant UC as ReconcileSubmittedOrders
    participant R as NetworkOrderRepo
    participant GW as NetworkGateway (stub)
    participant A as NetworkOrder
    participant U as UoW
    participant PUB as Publisher
    participant OM as FulfillmentPlanner (order-management)
    T->>UC: Execute()
    UC->>R: ListSubmitted()
    loop each SUBMITTED order
        UC->>GW: SubmissionStatus(ref)
        alt lookup error
            Note over UC: skip, retry next pass
        else SUCCESS
            UC->>A: ConfirmAcknowledgement()
            UC->>U: Execute: Save ACKNOWLEDGED + Publish NetworkOrderAcknowledged v2 (ADR 0016)
            UC->>OM: ReleaseHeldOrder(localOrderId)
        else FAILURE
            UC->>OM: CancelHeldOrder(localOrderId)
            UC->>A: SettleHold(CANCELLED)
            UC->>A: Reject()
            UC->>U: Execute: Save REJECTED + Publish NetworkOrderRejected SUBMISSION_FAILED
        else PENDING
            Note over UC: counted as pending, left for next pass
        end
    end
    UC-->>T: Examined, Confirmed, Failed, Pending
```

Source: `internal/application/usecases/reconcile_submitted_orders.go`,
`cmd/netfulfil/main.go` (`runReconcile`),
`internal/adapters/outbound/retailnetwork/stub.go` (`StubGateway.SubmissionStatus`).

Omitted: release and cancel run only when a `localOrderId` is linked, which
is always the case on this path. The stub reconciles any acknowledged
ref to `SUCCESS` immediately, so in stub mode `FAILURE` never occurs. A
per-order error is skipped and not counted.

## 3. SweepAcknowledgementDeadlines, RejectOverdueOrders and SweepOrphanedHolds (tickers, SWEEP_INTERVAL)

```mermaid
sequenceDiagram
    autonumber
    participant TS as runSweep ticker
    participant SW as SweepAcknowledgementDeadlines
    participant TR as runRejectOverdue ticker
    participant RO as RejectOverdueOrders
    participant TH as runSweepOrphanedHolds ticker
    participant SH as SweepOrphanedHolds
    participant R as NetworkOrderRepo
    participant A as NetworkOrder
    participant U as UoW
    participant PUB as Publisher
    participant OM as FulfillmentPlanner (order-management)
    TS->>SW: Execute()
    SW->>R: ListUnanswered()
    loop each NEW order
        SW->>A: AcknowledgementOverdue(now)
        opt overdue
            SW->>PUB: Publish AcknowledgementDeadlineAtRisk
        end
    end
    TR->>RO: Execute()
    RO->>R: ListUnanswered()
    loop each NEW order
        RO->>A: AcknowledgementOverdue(now)
        opt overdue
            opt hold recorded and still open
                RO->>OM: CancelHeldOrder(localOrderId)
                RO->>A: SettleHold(CANCELLED)
            end
            RO->>A: Reject()
            RO->>U: Execute: Save REJECTED + Publish NetworkOrderRejected ACKNOWLEDGEMENT_DEADLINE_MISSED
        end
    end
    TH->>SH: Execute()
    SH->>R: ListOpenHolds()
    loop each NEW or REJECTED order with an open hold
        SH->>A: HoldOrphaned(now, ORPHAN_HOLD_GRACE)
        opt orphaned
            SH->>OM: CancelHeldOrder(localOrderId)
            alt 2xx or 404
                SH->>A: SettleHold(CANCELLED)
                SH->>U: Execute: Save + Publish OrphanedHoldCancelled CANCELLED or NOT_FOUND
            else 409 or 422
                SH->>A: SettleHold(ALREADY_RELEASED)
                SH->>U: Execute: Save, no event
            else any other error
                Note over SH: counted failed, left open for the next pass
            end
        end
    end
```

Source: `internal/application/usecases/sweep_acknowledgement_deadlines.go`,
`reject_overdue_orders.go`, `sweep_orphaned_holds.go`, `hold.go`,
`cmd/netfulfil/workers.go` (`runSweep`, `runRejectOverdue`,
`runSweepOrphanedHolds`).

Omitted: the deadline sweep's publish is **not** wrapped in a UoW, because it is
not a state change. A per-order failure in any pass is skipped and retried on
the next pass. Nothing is sent to the network. `RejectOverdueOrders` calls the
cancel only while the hold is open, and fails the order for this pass on any
non-2xx answer; the orphaned-hold sweep is the one that reads `404` and
`409`/`422` as answers, and only after `acknowledgeBy + ORPHAN_HOLD_GRACE`
(default `1h`), so it normally finds nothing. It never calls `Reject()`. It is
not started when `ORPHAN_HOLD_SWEEP_ENABLED=false`.

## 4. ConfirmNetworkOrderShipment (REST)

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant H as http.Server
    participant UC as ConfirmNetworkOrderShipment
    participant R as NetworkOrderRepo
    participant A as NetworkOrder
    participant U as UoW
    participant PUB as Publisher
    participant GW as NetworkGateway (stub)
    C->>H: POST /network-orders/networkRef/shipment-confirmation
    H->>UC: Execute(ref)
    UC->>R: FindByRef(ref)
    alt not found
        UC-->>H: ErrOrderNotFound
        H-->>C: 404 problem+json network-order-not-found
    else already CONFIRMED
        UC-->>H: order unchanged
        H-->>C: 204
    else found
        UC->>A: ConfirmShipment()
        alt state is not ACKNOWLEDGED
            A-->>UC: ErrConfirmBeforeAcknowledge
            H-->>C: 409 problem+json confirm-before-acknowledge
        else ACKNOWLEDGED
            UC->>U: Execute: Save CONFIRMED + Publish NetworkOrderShipmentConfirmed
            UC->>GW: SubmitShipmentConfirmation(ref)
            UC-->>H: order
            H-->>C: 204
        end
    end
```

Source: `internal/application/usecases/confirm_network_order_shipment.go`,
`internal/adapters/inbound/http/server.go` (`handleConfirmShipment`),
`internal/adapters/inbound/http/errors.go`.

Omitted: an empty `networkRef` returns 422 (`ErrEmptyNetworkRef`). A failed
`SubmitShipmentConfirmation` returns 500 after the record has committed;
a retry is then a no-op that returns 204 without re-submitting. The 409
for `ErrConfirmBeforeAcknowledge` is a case in `statusFor`/`problemFor`
(`errors.go`), pinned by `TestHandleConfirmShipment_BeforeAcknowledgeReturns409`.
The route is registered only when `ConfirmShipment` is wired,
which `cmd/netfulfil` always does. CORS allows `GET/POST/OPTIONS`, so a
browser can call this route cross-origin.

## 5. RecomputeCapabilityOffers (ticker, RECOMPUTE_INTERVAL)

```mermaid
sequenceDiagram
    autonumber
    participant T as runRecompute ticker
    participant UC as RecomputeCapabilityOffers
    participant TR as ProductTranslation
    participant PP as ProcessPathCapability cache
    participant PC as PathCapacity cache
    participant INV as InventoryAvailability (inventory-storage)
    participant CO as capabilityoffer
    participant R as CapabilityOfferRepo
    T->>UC: Execute()
    UC->>TR: KnownSKUs()
    UC->>PP: NextCutoff(siteId, now)
    loop each SKU
        UC->>INV: UsableQuantity(sku)
        alt lookup error
            Note over UC: log and skip SKU
        else ok
            loop each eligible path of the next cutoff
                UC->>PC: RemainingCapacity(pathId, cutoffAt)
            end
            UC->>CO: Compute(sku, siteId, physical, feasible, known, now)
            CO-->>UC: offer PHYSICAL or THROUGHPUT_CONSTRAINED
            UC->>R: Save(offer) upsert
        end
    end
    UC-->>T: Examined, ThroughputConstrained
```

Source: `internal/application/usecases/recompute_capability_offers.go`,
`internal/domain/capabilityoffer/capability_offer.go`,
`cmd/netfulfil/main.go` (`wireCapabilityOffer`, `runRecompute`).

Omitted: there is no UoW and no event. The whole use case is always wired
at boot, after both caches pass `WaitReady`.
When `NextCutoff` reports no schedule, the capacity loop is skipped and
`known=false`.

## 6. Outbox relay and analytics projection

```mermaid
sequenceDiagram
    autonumber
    participant RL as postgres.OutboxRelay
    participant DB as outbox_events
    participant K as Kafka
    participant AC as kafka.AnalyticsConsumer
    participant CE as analytics_consumed_events
    participant PR as PostgresProjection
    participant DLQ as analytics.dlq topic
    loop every pass, OUTBOX_RELAY_INTERVAL when idle
        RL->>DB: SELECT unpublished AND attempts below OUTBOX_MAX_ATTEMPTS FOR UPDATE SKIP LOCKED LIMIT n
        RL->>K: WriteMessages to each row topic
        alt sent
            RL->>DB: UPDATE published_at = now, attempts + 1
        else send failed
            RL->>DB: UPDATE attempts + 1, last_error, COMMIT, stop this pass
            opt attempts reached OUTBOX_MAX_ATTEMPTS, default 20 (ADR 0021)
                RL->>RL: log ERROR, row excluded from later claims until re-armed
            end
        end
    end
    K->>AC: message on warehouse.network-fulfillment.analytics
    alt not a valid CloudEvent
        AC->>DLQ: publish poison message
    else known type
        Note over AC,PR: one analytics transaction, analyticsstore.UnitOfWork (ADR 0023)
        AC->>CE: MarkProcessed(id)
        alt first time
            AC->>PR: Apply Received / Acknowledged (v1 or v2) / Rejected, Submitted claimed with no effect
        else duplicate
            Note over AC: skip
        end
        Note over AC,PR: COMMIT both, or roll both back and retry (max 3) then DLQ
    else unknown type
        Note over AC: ignore
    end
```

Source: `internal/adapters/outbound/postgres/outbox_relay.go`,
`outbox_publisher.go`, `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/`.

Omitted: the relay, which is always on. Phase retries (up to 3, with exponential backoff)
precede a DLQ on infrastructure errors. `cmd/netfulfil-reports` reads
`acknowledgement_rollup` over a read-only pool.
