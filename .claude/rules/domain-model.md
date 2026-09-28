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
- **CapabilityOffer** — *planned* (ADR 0001), not in code: advertised
  quantity per `(sku, siteId)` constrained by throughput, not just stock.

## Aggregates

- **NetworkOrder** (`internal/domain/networkorder`) —
  `NEW -> ACKNOWLEDGED -> CONFIRMED`, `NEW -> REJECTED`.
  - `Receive` requires a non-empty `NetworkRef` and at least one line;
    `ReceiveUntranslatable` records demand we must reject because a product
    has no SKU mapping.
  - `Acknowledge` / `Reject` only from `NEW` (`ErrAlreadyAnswered`).
  - `LinkLocalOrder` only when acknowledged (`ErrNotAcknowledged`) and only
    once (`ErrLocalOrderAlreadyLinked`).
  - `ConfirmShipment` only from `ACKNOWLEDGED`
    (`ErrConfirmBeforeAcknowledge`) — no use case calls it yet.
  - `AcknowledgementOverdue(now)` — still `NEW` and past `acknowledgeBy`.
- Value-object validation errors live in `internal/domain/shared`
  (`ErrEmptyNetworkRef`, `ErrEmptyNetworkLineRef`,
  `ErrEmptyNetworkProductId`, `ErrNonPositiveQuantity`, `ErrNoLines`,
  `ErrUnknownProduct`).

## Domain events

None typed yet. The `ports.EventPublisher` port exists and is wired to a
log-only publisher in `cmd/netfulfil`, but no use case calls it. See
`.claude/rules/integration-events.md`.

## Key use cases (`internal/application/usecases`)

- `ReceiveNetworkDemand` — idempotent on `NetworkRef`; translates each line
  (`ports.ProductTranslation`; one unknown product -> record + reject),
  raises a held order and asks for a verdict
  (`ports.FulfillmentPlanner.RaiseHeldOrder`), then either link + save +
  acknowledge to the network + release, or cancel + save + reject to the
  network. No promise arithmetic.
- `SweepAcknowledgementDeadlines` — rejects every overdue `NEW` order and
  cancels its held local order, if any.

Both are driven from `cmd/netfulfil`: the poller
(`internal/adapters/inbound/poller`) feeds `ReceiveNetworkDemand`; a ticker
runs the sweep.

## Ports (`internal/application/ports`, interfaces only)

`NetworkOrderRepo` (memory + postgres), `NetworkGateway` (stub only —
`sandbox`/`live` refuse to boot), `FulfillmentPlanner` (order-management
REST), `ProductTranslation` (file-backed in-memory map), `EventPublisher`,
`Clock`.
