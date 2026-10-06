# Domain Message Flow

Following ddd-crew [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling).
Four key business scenarios. Participants are contexts and external
systems. Every arrow is a real message (port call, REST route, CloudEvents
type) and is prefixed `cmd:` (command), `qry:` (query) or `evt:` (event).

Kafka events are published only with `EVENT_PUBLISHER=kafka`. With
`DATABASE_URL` set they go through the transactional outbox. Every event
goes to both `warehouse.network-fulfillment.events` and
`warehouse.network-fulfillment.analytics`; the diagrams show one "Kafka"
participant for both.

## 1. Network demand accepted, settled and shipped

```mermaid
sequenceDiagram
    autonumber
    participant RN as retail-network (stub)
    participant NF as network-fulfillment
    participant OM as order-management
    participant K as Kafka
    actor OP as Operator
    NF->>RN: qry: PollDemand since watermark
    RN-->>NF: qry: reply InboundDemand networkRef, lines, requiredShipBy
    NF->>K: evt: networkorder.NetworkOrderReceived
    NF->>OM: cmd: POST /orders releaseOnAllocation false, allowPartialShipment false, requiredShipBy
    OM-->>NF: cmd: reply order id + promiseDate present = feasible
    NF->>K: evt: networkorder.NetworkOrderAcknowledged
    NF->>RN: cmd: SubmitAcknowledgement accepted true
    Note over NF: order is SUBMITTED, hold not released
    NF->>RN: qry: SubmissionStatus networkRef
    RN-->>NF: qry: reply SUCCESS
    NF->>OM: cmd: POST /orders/id/release
    Note over NF: order is ACKNOWLEDGED
    OP->>NF: cmd: POST /network-orders/networkRef/shipment-confirmation
    NF->>K: evt: networkorder.NetworkOrderShipmentConfirmed
    NF->>RN: cmd: SubmitShipmentConfirmation networkRef
    NF-->>OP: cmd: reply 204 No Content
```

Source: `internal/application/usecases/receive_network_demand.go`,
`reconcile_submitted_orders.go`, `confirm_network_order_shipment.go`;
`internal/adapters/outbound/ordermanagement/planner.go`;
`internal/adapters/inbound/http/server.go`.

Omitted: translation (local, no message); the poller's idempotency check;
the reconcile pass runs on its own `POLL_INTERVAL` ticker, minutes after
the submission; `FAILURE` and `PENDING` branches (see scenario 2 and
[sequence-diagrams.md](sequence-diagrams.md)). Full types are
`com.warehouse.wes.network-fulfillment.networkorder.<Event>`; REST path
parameters `{id}` / `{networkRef}` are written without braces.

## 2. Network demand rejected

```mermaid
sequenceDiagram
    autonumber
    participant RN as retail-network (stub)
    participant NF as network-fulfillment
    participant OM as order-management
    participant K as Kafka
    NF->>RN: qry: PollDemand since watermark
    RN-->>NF: qry: reply InboundDemand
    alt a product has no SKU mapping
        NF->>K: evt: networkorder.NetworkOrderReceived lineCount 0
        NF->>K: evt: networkorder.NetworkOrderRejected reason UNTRANSLATABLE_SKU
        NF->>RN: cmd: SubmitAcknowledgement accepted false
    else deadline infeasible
        NF->>K: evt: networkorder.NetworkOrderReceived
        NF->>OM: cmd: POST /orders held, requiredShipBy
        OM-->>NF: cmd: reply order id + promiseDate null = infeasible
        NF->>OM: cmd: DELETE /orders/id
        NF->>K: evt: networkorder.NetworkOrderRejected reason INFEASIBLE_DEADLINE
        NF->>RN: cmd: SubmitAcknowledgement accepted false
    end
    opt later, a submitted acceptance reconciles to FAILURE
        NF->>RN: qry: SubmissionStatus networkRef
        RN-->>NF: qry: reply FAILURE
        NF->>OM: cmd: DELETE /orders/id
        NF->>K: evt: networkorder.NetworkOrderRejected reason SUBMISSION_FAILED
    end
```

Source: `internal/application/usecases/receive_network_demand.go`
(`rejectUntranslatable`, `reject`), `reconcile_submitted_orders.go` (`fail`).

Omitted: the untranslatable path never calls `order-management` at all.
In the `FAILURE` path the network already knows its own refusal, so no
second `SubmitAcknowledgement` is sent.

## 3. Acknowledgement window missed

```mermaid
sequenceDiagram
    autonumber
    participant NF as network-fulfillment
    participant OM as order-management
    participant K as Kafka
    participant PJ as analytics projector
    Note over NF: an order is still NEW past acknowledgeBy
    loop every SWEEP_INTERVAL, SweepAcknowledgementDeadlines
        NF->>K: evt: networkorder.AcknowledgementDeadlineAtRisk
    end
    Note over NF: RejectOverdueOrders on its own ticker
    opt a localOrderId is linked
        NF->>OM: cmd: DELETE /orders/id
    end
    NF->>K: evt: networkorder.NetworkOrderRejected reason ACKNOWLEDGEMENT_DEADLINE_MISSED
    K->>PJ: evt: networkorder.NetworkOrderRejected on the analytics topic
    Note over PJ: acknowledgement_deadlines_missed + 1
```

Source: `internal/application/usecases/sweep_acknowledgement_deadlines.go`,
`reject_overdue_orders.go`, `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/postgres_projection.go`.

Omitted: the two passes are independent tickers. Whichever runs first
decides whether an at-risk event fires before the rejection. No message is
sent to the network: the window is closed and the order is never
acknowledged late. The projector ignores `AcknowledgementDeadlineAtRisk`.

## 4. Capability offer recomputed

```mermaid
sequenceDiagram
    autonumber
    participant PPM as process-path-management
    participant WWP as wes-work-planning
    participant NF as network-fulfillment
    participant INV as inventory-storage
    actor OP as Operator or MCP client
    PPM->>NF: evt: processpath.ProcessPathCreated / Updated / Deactivated
    PPM->>NF: evt: cptschedule.CPTScheduleChanged
    WWP->>NF: evt: workpool.PathCapacityChanged
    Note over NF: caches replayed, boot unblocked
    loop every RECOMPUTE_INTERVAL, per known SKU
        NF->>INV: qry: GET /inventory/sku/usable
        INV-->>NF: qry: reply usable quantity
        Note over NF: min of physical and summed remaining capacity
    end
    OP->>NF: qry: GET /capability-offers or MCP list_capability_offers
    NF-->>OP: qry: reply capabilityOffers with basis
```

Source: `internal/application/usecases/recompute_capability_offers.go`,
`internal/adapters/outbound/processpathcache/consumer.go`,
`internal/adapters/outbound/pathcapacitycache/consumer.go`,
`internal/adapters/outbound/inventoryclient/client.go`,
`internal/adapters/inbound/http/server.go`, `internal/adapters/inbound/mcp/tools.go`.

Omitted: full CE types are
`com.warehouse.wes.process-path-management.processpath.*`,
`com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged`
and `com.warehouse.wes.work-planning.workpool.PathCapacityChanged`. The
whole scenario runs only with `CAPABILITY_OFFER_ENABLED=true`. No message
goes to the network: `SubmitAvailability` is not called yet.
