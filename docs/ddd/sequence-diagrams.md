# Sequence diagrams

UML sequence diagrams for every command use case, derived from the use-case
function bodies in `internal/application/usecases/`. `UoW` is
`ports.UnitOfWork`, which is `postgres.UnitOfWork` when `DATABASE_URL` is
set. Without Postgres, `atomically` calls the function directly and there
is no transaction. `Publisher` is the wired `ports.EventPublisher`:

- the outbox publisher, which writes `outbox_events` rows for both topics
  inside the transaction;
- the direct Kafka fan-out, when there is no database;
- the log publisher, when `EVENT_PUBLISHER` is unset.

There is no idempotency-key middleware and no optimistic-concurrency
version check anywhere in this context. Idempotency comes from the state
checks shown below.

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
        alt already known
            R-->>UC: existing order
            UC-->>P: existing order, no new answer
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
                alt not feasible
                    UC->>OM: CancelHeldOrder(localOrderId)
                    UC->>A: Reject()
                    UC->>U: Execute: Save REJECTED + Publish NetworkOrderRejected INFEASIBLE_DEADLINE
                    UC->>GW: SubmitAcknowledgement(ref, false)
                else feasible
                    UC->>A: Submit()
                    UC->>A: LinkLocalOrder(localOrderId)
                    UC->>U: Execute: Save SUBMITTED + Publish NetworkOrderAcknowledged
                    UC->>GW: SubmitAcknowledgement(ref, true)
                    Note over UC,OM: no release here, ReconcileSubmittedOrders releases
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
`raise held order: ...` with the order already saved `NEW`. A failed submit
returns `submit acknowledgement` or `submit rejection` after the record has
committed. Any error leaves the demand to be re-polled; the stub keeps it
pending until `SubmitAcknowledgement`. A non-`ErrUnknownProduct`
translation error aborts without saving anything.

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
            UC->>U: Execute: Save ACKNOWLEDGED, no event
            UC->>OM: ReleaseHeldOrder(localOrderId)
        else FAILURE
            UC->>OM: CancelHeldOrder(localOrderId)
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
`internal/adapters/outbound/network/gateway.go` (`StubGateway.SubmissionStatus`).

Omitted: release and cancel run only when a `localOrderId` is linked, which
is always the case on this path. The stub reconciles any acknowledged
ref to `SUCCESS` immediately, so in stub mode `FAILURE` never occurs. A
per-order error is skipped and not counted.

## 3. SweepAcknowledgementDeadlines and RejectOverdueOrders (tickers, SWEEP_INTERVAL)

```mermaid
sequenceDiagram
    autonumber
    participant TS as runSweep ticker
    participant SW as SweepAcknowledgementDeadlines
    participant TR as runRejectOverdue ticker
    participant RO as RejectOverdueOrders
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
            opt localOrderId linked
                RO->>OM: CancelHeldOrder(localOrderId)
            end
            RO->>A: Reject()
            RO->>U: Execute: Save REJECTED + Publish NetworkOrderRejected ACKNOWLEDGEMENT_DEADLINE_MISSED
        end
    end
```

Source: `internal/application/usecases/sweep_acknowledgement_deadlines.go`,
`reject_overdue_orders.go`, `cmd/netfulfil/main.go` (`runSweep`,
`runRejectOverdue`).

Omitted: the sweep's publish is **not** wrapped in a UoW, because it is not
a state change. A per-order failure in either pass is skipped and retried
on the next pass. Nothing is sent to the network.

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
            H-->>C: 500 problem+json internal-error
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
a retry is then a no-op that returns 204 without re-submitting. The 500
for `ErrConfirmBeforeAcknowledge` happens because `statusFor` has no case
for it. The route is registered only when `ConfirmShipment` is wired,
which `cmd/netfulfil` always does. CORS allows only `GET/OPTIONS`, so a
browser cannot call this route cross-origin.

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

Omitted: there is no UoW and no event. The whole use case is wired only
with `CAPABILITY_OFFER_ENABLED=true`, after both caches pass `WaitReady`.
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
        RL->>DB: SELECT unpublished FOR UPDATE SKIP LOCKED LIMIT n
        RL->>K: WriteMessages to each row topic
        alt sent
            RL->>DB: UPDATE published_at = now, attempts + 1
        else send failed
            RL->>DB: UPDATE attempts + 1, last_error
        end
    end
    K->>AC: message on warehouse.network-fulfillment.analytics
    alt not a valid CloudEvent
        AC->>DLQ: publish poison message
    else known type
        AC->>CE: MarkProcessed(id)
        alt first time
            AC->>PR: Apply Received / Acknowledged / Rejected
        else duplicate
            Note over AC: skip
        end
    else unknown type
        Note over AC: ignore
    end
```

Source: `internal/adapters/outbound/postgres/outbox_relay.go`,
`outbox_publisher.go`, `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/`.

Omitted: the relay runs only with both `DATABASE_URL` and
`EVENT_PUBLISHER=kafka`. Phase retries (up to 3, with exponential backoff)
precede a DLQ on infrastructure errors. `cmd/netfulfil-reports` reads
`acknowledgement_rollup` over a read-only pool.
