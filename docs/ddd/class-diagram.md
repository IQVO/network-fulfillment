# Class diagrams

UML class diagrams of `internal/domain/**`, plus the hexagonal ports and
adapters. Type and member names are the real Go identifiers. Fields are
unexported in Go and shown with `-`; exported methods are shown with `+`.
Only state-changing methods and key queries are shown.

## 1. NetworkOrder aggregate

```mermaid
classDiagram
    direction LR
    class NetworkOrder {
        <<AggregateRoot>>
        -networkRef NetworkRef
        -siteId SiteId
        -requiredShipBy time.Time
        -acknowledgeBy time.Time
        -lines Line[]
        -state State
        -localOrderId LocalOrderId nullable
        -receivedAt time.Time
        +Receive(ref, siteId, requiredShipBy, lines, receivedAt)$ NetworkOrder
        +ReceiveUntranslatable(ref, siteId, requiredShipBy, receivedAt)$ NetworkOrder
        +Rehydrate(...)$ NetworkOrder
        +Submit() error
        +ConfirmAcknowledgement() error
        +Acknowledge() error
        +Reject() error
        +LinkLocalOrder(id LocalOrderId) error
        +ConfirmShipment() error
        +AcknowledgementOverdue(now) bool
        +SKUQuantities() Map~SKU,int~
        +Lines() Line[]
    }
    class Line {
        <<Entity>>
        -networkLineRef NetworkLineRef
        -networkProductId NetworkProductId
        -sku SKU
        -quantity int
        +NewLine(ref, productId, sku, quantity)$ Line
    }
    class State {
        <<Enumeration>>
        NEW
        SUBMITTED
        ACKNOWLEDGED
        REJECTED
        CONFIRMED
    }
    class NetworkRef {
        <<ValueObject>>
        string
    }
    class NetworkLineRef {
        <<ValueObject>>
        string
    }
    class NetworkProductId {
        <<ValueObject>>
        string
    }
    class SKU {
        <<ValueObject>>
        string
    }
    class LocalOrderId {
        <<ValueObject>>
        string
    }
    class SiteId {
        <<ValueObject>>
        string
    }
    class NetworkOrderRepo {
        <<Repository>>
        +Save(ctx, o) error
        +FindByRef(ctx, ref) NetworkOrder
        +ListUnanswered(ctx) NetworkOrder[]
        +ListSubmitted(ctx) NetworkOrder[]
        +ListAll(ctx) NetworkOrder[]
    }

    NetworkOrder "1" *-- "0..*" Line : lines
    NetworkOrder --> State : state
    NetworkOrder --> NetworkRef : identity
    NetworkOrder --> SiteId
    NetworkOrder --> LocalOrderId : 0..1
    Line --> NetworkLineRef
    Line --> NetworkProductId
    Line --> SKU
    NetworkOrderRepo ..> NetworkOrder : persists
```

Source: `internal/domain/networkorder/network_order.go`,
`internal/domain/shared/shared.go`, `internal/application/ports/ports.go`.

Omitted: the read accessors (`NetworkRef()`, `State()` and so on), the
`AcknowledgementWindow` constant (24h), and the error variables (listed on
[aggregate-design-canvas.md](aggregate-design-canvas.md)). A `Line` is
modelled as an entity because it has a local identity, `networkLineRef`,
which is unique within its order. In Go it is a struct value owned by the
aggregate. `0..*` lines reflects `ReceiveUntranslatable`; `Receive`
requires at least one.

## 2. CapabilityOffer aggregate

```mermaid
classDiagram
    direction LR
    class CapabilityOffer {
        <<AggregateRoot>>
        -sku SKU
        -siteId SiteId
        -advertisedQuantity int
        -basis Basis
        -computedAt time.Time
        +New(sku, siteId, advertisedQuantity, physicalAvailable, basis, computedAt)$ CapabilityOffer
        +Compute(sku, siteId, physicalAvailable, throughputFeasible, throughputKnown, computedAt)$ CapabilityOffer
        +AdvertisedQuantity() int
        +Basis() Basis
    }
    class Basis {
        <<Enumeration>>
        PHYSICAL
        THROUGHPUT_CONSTRAINED
    }
    class SKU {
        <<ValueObject>>
        string
    }
    class SiteId {
        <<ValueObject>>
        string
    }
    class CapabilityOfferRepo {
        <<Repository>>
        +Save(ctx, o) error
        +ListAll(ctx) CapabilityOffer[]
    }
    CapabilityOffer --> Basis
    CapabilityOffer --> SKU : identity part
    CapabilityOffer --> SiteId : identity part
    CapabilityOfferRepo ..> CapabilityOffer : upserts
```

Source: `internal/domain/capabilityoffer/capability_offer.go`,
`internal/application/ports/ports.go`.

Omitted: the five `Err...` variables, and the other read accessors
(`SKU()`, `SiteId()`, `ComputedAt()`). The type has no mutators: `New` and
`Compute` are the only constructors.

## 3. Domain events

```mermaid
classDiagram
    direction TB
    class DomainEvent {
        <<interface>>
        +EventName() string
        +OccurredAt() time.Time
    }
    class NetworkOrderReceived {
        <<DomainEvent>>
        +NetworkRef NetworkRef
        +SiteId SiteId
        +RequiredShipBy time.Time
        +AcknowledgeBy time.Time
        +LineCount int
        +At time.Time
    }
    class NetworkOrderSubmitted {
        <<DomainEvent>>
        +NetworkRef NetworkRef
        +SiteId SiteId
        +LocalOrderId LocalOrderId
        +ReceivedAt time.Time
        +At time.Time
    }
    class NetworkOrderAcknowledged {
        <<DomainEvent, wire v2>>
        +NetworkRef NetworkRef
        +SiteId SiteId
        +LocalOrderId LocalOrderId
        +ReceivedAt time.Time
        +At time.Time
    }
    class NetworkOrderRejected {
        <<DomainEvent>>
        +NetworkRef NetworkRef
        +SiteId SiteId
        +Reason RejectionReason
        +At time.Time
    }
    class NetworkOrderShipmentConfirmed {
        <<DomainEvent>>
        +NetworkRef NetworkRef
        +SiteId SiteId
        +LocalOrderId LocalOrderId
        +At time.Time
    }
    class AcknowledgementDeadlineAtRisk {
        <<DomainEvent>>
        +NetworkRef NetworkRef
        +SiteId SiteId
        +AcknowledgeBy time.Time
        +At time.Time
    }
    class RejectionReason {
        <<Enumeration>>
        UNTRANSLATABLE_SKU
        INFEASIBLE_DEADLINE
        ACKNOWLEDGEMENT_DEADLINE_MISSED
        SUBMISSION_FAILED
    }
    DomainEvent <|.. NetworkOrderReceived
    DomainEvent <|.. NetworkOrderSubmitted
    DomainEvent <|.. NetworkOrderAcknowledged
    DomainEvent <|.. NetworkOrderRejected
    DomainEvent <|.. NetworkOrderShipmentConfirmed
    DomainEvent <|.. AcknowledgementDeadlineAtRisk
    NetworkOrderRejected --> RejectionReason
```

Source: `internal/domain/shared/events.go`.

Omitted: JSON tags (camelCase field names on the wire, for example
`networkRef`, `lineCount`). Events are plain structs raised by use cases,
not by aggregate methods.

## 4. Hexagonal ports and adapters

```mermaid
flowchart LR
    subgraph IN["Inbound adapters"]
        HTTP["http.Server<br/>REST routes"]
        POLL["poller.Poller"]
        MCP["mcp Deps<br/>read-only tools"]
        AK["kafka.AnalyticsConsumer<br/>own analytics topic"]
        RPT["http.ReportsHandlers<br/>netfulfil-reports"]
    end
    subgraph APP["Application"]
        UC1["ReceiveNetworkDemand"]
        UC2["ReconcileSubmittedOrders"]
        UC3["SweepAcknowledgementDeadlines"]
        UC4["RejectOverdueOrders"]
        UC5["ConfirmNetworkOrderShipment"]
        UC6["RecomputeCapabilityOffers"]
    end
    subgraph DOM["Domain"]
        NO["networkorder.NetworkOrder"]
        CO["capabilityoffer.CapabilityOffer"]
    end
    subgraph PORTS["Outbound ports"]
        P1["NetworkOrderRepo"]
        P2["CapabilityOfferRepo"]
        P3["NetworkGateway"]
        P4["FulfillmentPlanner"]
        P5["ProductTranslation"]
        P6["ProcessPathCapability"]
        P7["PathCapacity"]
        P8["InventoryAvailability"]
        P9["EventPublisher"]
        P10["UnitOfWork"]
        P11["Clock"]
    end
    subgraph OUT["Outbound adapters"]
        A1["postgres.NetworkOrderRepo<br/>memory.NetworkOrderRepo"]
        A2["postgres.CapabilityOfferRepo<br/>memory.CapabilityOfferRepo"]
        A3["network.StubGateway"]
        A4["ordermanagement.Planner<br/>+ BreakerClient"]
        A5["memory.ProductTranslation"]
        A6["processpathcache.Consumer"]
        A7["pathcapacitycache.Consumer"]
        A8["inventoryclient.Client"]
        A9["kafka.Publisher + AnalyticsPublisher<br/>postgres.OutboxPublisher<br/>events.LogPublisher"]
        A10["postgres.UnitOfWork"]
    end
    POLL --> UC1
    HTTP --> UC5
    HTTP -. reads .-> P1
    HTTP -. reads .-> P2
    MCP -. reads .-> P1
    MCP -. reads .-> P2
    APP --> DOM
    APP --> PORTS
    P1 --- A1
    P2 --- A2
    P3 --- A3
    P4 --- A4
    P5 --- A5
    P6 --- A6
    P7 --- A7
    P8 --- A8
    P9 --- A9
    P10 --- A10
```

Source: `internal/application/ports/ports.go`, `cmd/netfulfil/main.go`
(the wiring), `cmd/mcp/main.go`, `cmd/netfulfil-projector/main.go`,
`cmd/netfulfil-reports/main.go`, `internal/architecture/architecture_test.go`
(the arch-go dependency rules).

Omitted: `Clock` is satisfied by a `systemClock` in each composition root,
so it has no adapter package. The tickers in `cmd/netfulfil` drive
UC2, UC3, UC4 and UC6, and are not drawn. The analytics consumer and the
reports handlers bypass `application/` and use `internal/analytics/report`
ports (`ProjectionStore`, `ReportStore`) implemented by
`internal/adapters/outbound/analyticsstore`. `postgres.OutboxRelay` drains
`outbox_events` to the Kafka publisher. The HTTP and MCP adapters read the
repositories directly, because every read is a pure projection (see the
`http.Server` doc comment).
