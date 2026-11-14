# Domain model: ubiquitous language, aggregates, events, use cases

Source of truth: `internal/domain/`, `internal/application/` and
`docs/adr/0001-network-fulfillment-bounded-context.md`. Keep this in sync
with the code — never describe a planned concept as if it exists.

## Ubiquitous Language (use these exact names)

- **NetworkOrder** — demand that arrived from the external network carrying
  a deadline we did not choose; answered exactly once, in full or not at all.
- **NetworkRef / NetworkLineRef / NetworkProductId** — the network's own
  identities (purchase order, line within it, product). Opaque strings we
  never parse. A `NetworkProductId` is **not** a `SKU`.
- **SKU** — our product identity; appears only as the OUTPUT of the
  Anti-Corruption Layer's translation.
- **LocalOrderId** — order-management's `OrderId` for the held order raised
  from a NetworkOrder. A persisted one-to-one mapping, never a string
  convention.
- **requiredShipBy** — the network's deadline, sent to order-management
  as-is; order-management decides feasibility.
- **acknowledgeBy** — `receivedAt + AcknowledgementWindow` (24h, a domain
  constant), fixed at receipt and persisted, never recomputed.
- **Held order** — an order-management order raised with
  `releaseOnAllocation: false`; released on acknowledgement, cancelled on
  rejection.
- **CapabilityOffer** (`internal/domain/capabilityoffer`, ADR 0001 §8) —
  advertised quantity per `(sku, siteId)` constrained by throughput, not just
  stock; recomputed on a schedule by `RecomputeCapabilityOffers`, always on.

## Aggregates

- **NetworkOrder** (`internal/domain/networkorder`) —
  `NEW -> SUBMITTED -> ACKNOWLEDGED -> CONFIRMED`, `NEW` or `SUBMITTED -> REJECTED`.
  - `Receive` requires a non-empty `NetworkRef` and at least one line;
    `ReceiveUntranslatable` records demand we must reject because a product
    has no SKU mapping.
  - `Submit` (`NEW -> SUBMITTED`: our answer is in flight, ADR 0001 §5);
    `ConfirmAcknowledgement` settles it to `ACKNOWLEDGED` (what
    `ReconcileSubmittedOrders` calls, `ErrNotSubmitted` otherwise).
    `Acknowledge` does both in one step and exists for tests and fixtures.
  - `Reject` only from `NEW` or `SUBMITTED` (`ErrAlreadyAnswered`).
  - `LinkLocalOrder` once `SUBMITTED` or `ACKNOWLEDGED` (`ErrNotAcknowledged`)
    and only once (`ErrLocalOrderAlreadyLinked`).
  - `ConfirmShipment` only from `ACKNOWLEDGED`
    (`ErrConfirmBeforeAcknowledge`); `ConfirmNetworkOrderShipment` calls it.
  - A shipment that arrives while `SUBMITTED` is kept on the order (`pending_shipment.go`, ADR 0014 D7):
    `NoteShipmentBeforeAcknowledge` only in `SUBMITTED` (else `ErrConfirmBeforeAcknowledge`), the first
    one wins; `ConfirmShipment` clears it, `DropPendingShipment` clears it without confirming.
  - The hold on the local order is tracked in `hold.go` (`RecordHold`,
    `SettleHold`, `HoldOrphaned(now, grace)`).
  - `AcknowledgementOverdue(now)` — still `NEW` and past `acknowledgeBy`.
- Value-object validation errors live in `internal/domain/shared`
  (`ErrEmptyNetworkRef`, `ErrEmptyNetworkLineRef`,
  `ErrEmptyNetworkProductId`, `ErrNonPositiveQuantity`, `ErrNoLines`,
  `ErrUnknownProduct`).

## Domain events

Seven, published through `ports.EventPublisher`: `NetworkOrderReceived`,
`NetworkOrderSubmitted`, `NetworkOrderAcknowledged`, `NetworkOrderRejected`,
`NetworkOrderShipmentConfirmed`, `AcknowledgementDeadlineAtRisk` and
`OrphanedHoldCancelled`. See `.claude/rules/integration-events.md`.

## Key use cases (`internal/application/usecases`)

- `ReceiveNetworkDemand` — idempotent on `NetworkRef`; translates each line
  (`ports.ProductTranslation`; one unknown product -> record + reject),
  raises a held order and asks for a verdict
  (`ports.FulfillmentPlanner.RaiseHeldOrder`), then either link + save +
  acknowledge to the network + release, or cancel + save + reject to the
  network. No promise arithmetic.
- `SweepAcknowledgementDeadlines` — finds orders whose acknowledgement window
  has closed with no answer and reports `AcknowledgementDeadlineAtRisk`; it
  never changes the aggregate (ADR 0001 §6).
- `RejectOverdueOrders` — the irreversible step the sweep leaves out: rejects
  the overdue order and cancels its held local order (ADR 0001 §6).
- `ReconcileSubmittedOrders` — settles `SUBMITTED` orders by asking the
  gateway's transaction-status query (ADR 0001 §5).
- `ConfirmNetworkOrderShipment` — tells the network an order shipped, closes
  it and raises `NetworkOrderShipmentConfirmed`. Driven by outbound-shipping's
  `ShipmentConfirmed` consumer (primary); the REST endpoint is the operator
  fallback (ADR 0014 addendum).
- `DeferShipmentConfirmation` — the consumer's fallback when the order is still `SUBMITTED`: records the
  shipment on the order instead of dead-lettering it (ADR 0014 D7).
- `ConfirmPendingShipments` — a `POLL_INTERVAL` ticker: confirms a recorded shipment once the order is
  `ACKNOWLEDGED`, drops it for a `CONFIRMED` or `REJECTED` order.
- `SweepOrphanedHolds` — frees order-management holds nobody decided (ADR 0024).
- `RecomputeCapabilityOffers` — recomputes and persists the `CapabilityOffer`s
  on a schedule (ADR 0001 §8); `FeedInventory` sends them to the network, one
  feed per site; `DeclareNodeProfiles` declares each site's cutoffs, handling
  time and throughput (ADR 0027).
- `IntakeInboundShipments` and `ReturnInboundReceipts` — the two halves of the
  inbound loop (ADR 0028).

Wiring lives in `cmd/netfulfil`: the poller (`internal/adapters/inbound/poller`)
feeds `ReceiveNetworkDemand` and the Kafka consumers in
`internal/adapters/inbound/kafka` drive the event-triggered use cases.

## Ports (`internal/application/ports`, interfaces only)

`NetworkOrderRepo` (postgres; memory is a test double), `NetworkGateway` (stub by default;
`NETWORK_MODE=live` selects the retail network's Vendor API gateway, ADR 0026;
`sandbox` is retired and parses as stub), `FulfillmentPlanner` (order-management
REST), `ProductTranslation` (file-backed map, `adapters/outbound/producttranslation`), `EventPublisher`,
`Clock`.
