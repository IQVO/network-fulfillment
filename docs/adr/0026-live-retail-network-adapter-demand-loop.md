---
id: 0026-live-retail-network-adapter-demand-loop
slug: /adr/0026-live-retail-network-adapter-demand-loop
title: "26. Live retail-network adapter: the demand loop"
sidebar_label: "26. Live adapter: demand loop"
sidebar_position: 26
description: "ADR 0026 (Accepted) builds the demand half of the live NetworkGateway against the retail-network Vendor API: node registration and a persisted site-to-node map, polling purchase orders, whole-order acknowledgements with a reason, reconciliation by a persisted transaction id, labels and shipment confirmation, all with deterministic idempotency keys, bounded retries, 429 backoff and a circuit breaker. The supply loop (capability and inventory feeds) is ADR 0027."
---

# 26. Live retail-network adapter: the demand loop

## Status

**Accepted** (2026-10-24). Implements the part of [ADR 0009](./0009-retail-network-not-amazon-counterpart.md)
(§2 package rename, §4 `NETWORK_MODE=live`) that answers the network's
purchase orders. Fulfils the "revisit in the SAME PR that adds its first real
client" clause of [ADR 0004](./0004-resilience-circuit-breakers-retry-dlq-shutdown.md)
("Deliberately deferred"). Amends the port of [ADR 0001](./0001-network-fulfillment-bounded-context.md)
(§Decision, the `NetworkGateway` shape) in the five places listed in Decision 2.

**ADR 0027 is reserved for the supply loop** (node operational profile,
`DeclareCapability`, `SubmitAvailability`, the inventory feed). Nothing here
builds it; the seams it needs are named under Decision 11.

## Context

`NETWORK_MODE=live` refused to boot ("not implemented yet") and only the stub
gateway existed. The counterpart is our own `retail-network`
([ADR 0009](./0009-retail-network-not-amazon-counterpart.md)); its Vendor API is
the `vendor-connectivity` service (Open Host Service, Published Language). This
ADR is written against that contract as frozen in
`internal/adapters/outbound/retailnetwork/testdata/vendor-connectivity-openapi.yaml`
(a byte copy, Decision 9). The facts the design rests on, each read from that
contract or from the service's code on its `develop`:

- Every write takes `vendorId` and an `Idempotency-Key`. The network keeps a key
  for 24 hours, scoped to method + path; the same key with the same body replays
  the first answer (for an asynchronous write, the same `transactionId`), the same
  key with a different body is `422 idempotency-key-reused`, a `5xx` is never
  cached.
- A write is `202 {transactionId}`; it takes effect only when
  `GET /vendor/transactions/{id}` reports `SUCCESS` (`PROCESSING | SUCCESS |
  FAILURE`). A failed acknowledgement returns the purchase order to `ISSUED`; a
  failed shipment confirmation returns it to `ACCEPTED`.
- A purchase order is acknowledged once, in full (every line, with its ordered
  quantity), before `ackBy = min(issuedAt + ackWindow, requiredShipBy -
  handlingTime)`. After `ackBy` the network itself moves the order to `EXPIRED`;
  a late write is `409 ack-window-closed`. A `REJECT` needs one of `OUT_OF_STOCK`,
  `UNKNOWN_LISTING`, `CANNOT_MEET_SHIP_BY`, `DISCONTINUED`: **there is no expiry
  reason** in that enum, expiry is a state the network records on its own.
- A label is the one synchronous write (`200`), only for an `ACCEPTED` order, and
  its request carries `packages[]` of `{packageNo}` and nothing else (no weight,
  no dimension, no address). A shipment confirmation needs `shippedAt` and
  `trackingNumbers` that are exactly the labelled ones.
- Polling is `GET /vendor/purchase-orders?vendorId=&createdAfter=&nextToken=`,
  oldest first, paged; a purchase order can be delivered more than once; `429`
  carries `Retry-After`.
- Registering a node (`POST /vendor/nodes`) needs a `vendorNodeCode`, a node type,
  a region, a timezone, a postal code and a country; it is asynchronous and the
  successful transaction carries the new `nodeId`. `GET /vendor/nodes` lists the
  vendor's nodes with their `vendorNodeCode`.

Today's port was written before any of this and loses information the network
needs: the acknowledgement carries no reason and the order's per-line quantities
live only in the adapter's world; `ackBy` is replaced by a local 24-hour window;
`SubmissionStatus(ref)` has no persisted transaction to ask about; a label request
and a shipment confirmation say nothing about packages or time.

## Decision

### 1. Scope: the demand loop

Built: node mapping (adopt or register), `PollDemand`, `SubmitAcknowledgement`
(accept, and reject with a reason), `SubmissionStatus`, `RequestLabel`,
`SubmitShipmentConfirmation`, and `NETWORK_MODE=live` boot. **Not built, and not
failing:** `DeclareCapability` and `SubmitAvailability` are explicit no-ops in the
live gateway that log once per call at DEBUG and name ADR 0027; the node profile
and the inventory feed do not exist yet. Vendor onboarding (`POST /vendors`) is a
network partner-manager operation, not part of the vendor-facing protocol: the
vendor account `NETWORK_VENDOR_ID` must already exist (the platform seeds it) and
the adapter never calls `/vendors`.

### 2. The port stays in our vocabulary; six changes

No `poNumber`, `listingId`, `nodeId`, `packageNo`, transaction id or network
reason code appears in `ports`, `contract`, `usecases` or the domain. The changes:

1. `contract.InboundDemand.AcknowledgeBy time.Time`: the network's `ackBy`. Zero
   keeps today's local 24-hour window; non-zero becomes the order's
   `acknowledgeBy` (a new `networkorder.ReceiveBy` constructor, `Receive` is
   unchanged), so `SweepAcknowledgementDeadlines`, `RejectOverdueOrders` and
   `SweepOrphanedHolds` all use the network's real deadline.
2. `SubmitAcknowledgement(ctx, ref, contract.Acknowledgement{Accepted, Reason})`.
   `Reason` is the existing `shared.RejectionReason`.
3. `RequestLabel(ctx, ref, packageCount int) ([]contract.LabelResult, error)`, one
   result per package. The Vendor API needs only package
   numbers, and this context holds no package data, so the count is the caller's:
   `ConfirmNetworkOrderShipment` passes exactly one package (a ship-complete order
   leaves as one package until a fleet fact says otherwise, fleet-changes row 8).
   No weight or dimension is needed, so none is invented.
4. `SubmitShipmentConfirmation(ctx, ref, shippedAt time.Time)`. The tracking
   numbers are the ones the adapter persisted from `RequestLabel`; a confirmation
   without a persisted label is an error (`ErrNoLabel`), mirroring the network's
   `confirm-without-label`.
5. `ConfirmNetworkOrderShipment` requests the label before it records
   `CONFIRMED`, so a label failure leaves the order `ACKNOWLEDGED` and retryable.

6. A new `contract.SubmissionNotSent` status: the gateway holds no record that the
   answer was ever sent (a crash after the order was saved `SUBMITTED` and before the
   call that tells the network). `ReconcileSubmittedOrders` then sends the accept
   again (`ReconcileResult.Resent`); the gateway keeps that call idempotent
   (Decision 4). A re-delivered purchase order is still answered once:
   `ReceiveNetworkDemand` sends nothing for an order that is already `SUBMITTED`.

### 3. Reasons are mapped inside the adapter

| Fleet reason | Wire |
| --- | --- |
| `INFEASIBLE_DEADLINE` | `REJECT` + `CANNOT_MEET_SHIP_BY` |
| `UNTRANSLATABLE_SKU` | `REJECT` + `UNKNOWN_LISTING` |
| `ACKNOWLEDGEMENT_DEADLINE_MISSED` | no write: the window is closed, the network expires the order itself (`EXPIRED`) and would refuse a late write with `ack-window-closed` |
| `SUBMISSION_FAILED` | never sent: it is the fleet's own verdict on a failed transaction and stays internal |

A reject without a reason, an accept with one, or `SUBMISSION_FAILED` are
programming errors and return an error before any I/O.

### 4. Identifiers are mapped and persisted (migration 0006)

Four tables, all in this service's Postgres and all written by the adapter:

- `network_nodes (site_code PK, node_id UNIQUE, ...)`: `siteCode <-> nodeId`.
- `network_purchase_orders (network_ref PK, po_number UNIQUE, vendor_id, node_id,
  ack_by, ...)` and `network_purchase_order_lines (network_ref, network_line_ref,
  line_no, listing_id, network_product_id, quantity)`: `NetworkRef <-> poNumber`
  and the lines' `listingId <-> NetworkProductId`. The `NetworkRef` is **minted**
  (an opaque random id) the first time a purchase order is seen and never derived
  from the `poNumber`; a re-delivery finds the same row. The `NetworkProductId`
  given to the ACL is the listing id, opaque; `ProductTranslation` stays the only
  place it becomes a SKU. The lines are kept because an acknowledgement must echo
  every line number with its quantity.
- `network_submissions (kind, subject, idempotency_key, request_body,
  transaction_id, status, ...)`, primary key `(kind, subject)`: the submission
  ledger. `kind` is `ACKNOWLEDGEMENT`, `SHIPMENT_CONFIRMATION` or
  `NODE_REGISTRATION`; `subject` is the `NetworkRef` (or the site code).
- `network_labels (network_ref, package_no, label_ref, tracking_number, carrier)`.

**Idempotency keys are deterministic per `(kind, subject)`** (`nf-ack-<ref>`,
`nf-ship-<ref>`, `nf-node-<site>`, `nf-label-<ref>`, 128 characters at most). The
ledger row, **including the exact request body**, is inserted before the first
HTTP call. A call is made only while the row has no `transaction_id`; the stored
body and key are what is (re)sent, so a retry after a crash never changes the body
(which would be `idempotency-key-reused`) and never creates a second write. The
`transaction_id` is stored as soon as the `202` is read.

### 5. Polling

`PollDemand(since)` pages with `nextToken` until it is absent (a repeated token is
an error, not a loop). It asks `createdAfter = since - 2 minutes`: the overlap
covers a purchase order committed on the network just after the previous poll
began. Re-delivery is therefore normal and harmless: receiving demand is
idempotent by `NetworkRef`, and the adapter de-duplicates a `poNumber` repeated
inside one poll. Only `ISSUED` purchase orders are demand; anything else is an
order this context already answered, or one the network ended (`EXPIRED`,
`CANCELLED`). A purchase order on a node the adapter has no map for triggers a
`GET /vendor/nodes` refresh (the node's `vendorNodeCode` is the site code it was
registered under); if the node is still unknown the poll fails and the next tick
retries, nothing is skipped.

### 6. Reconciliation

`SubmissionStatus(ref)` reads the acknowledgement ledger row. No row at all is
`NOT_SENT` (Decision 2.6). A row with no `transaction_id` yet (the call failed or
the process died mid-call) is (re)sent from the stored body and key first, and the
network replays the original transaction; then `GET /vendor/transactions/{id}` maps `PROCESSING` to
`PENDING`, `SUCCESS` to `SUCCESS`, `FAILURE` to `FAILURE`, and the terminal status
is stored. `409 already-acknowledged` on a re-send (the key expired after 24 hours)
adopts the transaction id from `GET /vendor/purchase-orders/{po}` when the order
shows its acknowledgement, and stays `PENDING` otherwise. Only acknowledgements are
reconciled by a use case; a shipment confirmation's transaction id and status are
recorded in the ledger for operators.

### 7. Nodes

At each `PollDemand` the adapter converges its sites one step at a time and never
blocks: it lists the vendor's nodes and adopts those whose `vendorNodeCode`
matches a configured site; for a configured site with no node it registers one
(`POST /vendor/nodes`, `vendorNodeCode` = the site code) from `NETWORK_NODES_FILE`
(JSON, strict like the product dictionary: site code, node type, optional subtype,
region, timezone, postal code, country) and reconciles that transaction on later
ticks, storing the returned `nodeId`. A site with no data and no node is logged, not
fatal.

### 8. HTTP behaviour

One `http.Client` with a bulkhead of its own. Each attempt gets a 10 second
timeout bounded by the caller's remaining deadline (`resilience.CallTimeout`).
Network errors, timeouts and `5xx` are retried with jittered exponential backoff
(100 ms to 2 s, at most 5 attempts); `429` waits for `Retry-After` (capped at
30 s and at the caller's deadline) before the next attempt; any other `4xx` is
never retried and is mapped to a typed error from the `application/problem+json`
type. One circuit breaker guards the dependency (the tuning of ADR 0004 §1,
`circuit_breaker_state{dependency="retail-network"}`); transport errors and `5xx`
count as failures, an answer (`2xx`, `4xx`, `429`) does not; an open breaker returns
`ErrCircuitOpen` and the call is not attempted. ADR 0004 §4 refused blind retry of
mutations for lack of an idempotency-key contract; here every write has one
(Decision 4), so retrying them is safe.

### 9. Contract test

`testdata/vendor-connectivity-openapi.yaml` is a byte copy of
`vendor-connectivity` `apis/openapi.yaml` (blob
`7393aeb57374eb7e38d98272a6851a5ee4ba36a1`, last changed by commit
`26c54625e72343d5e1e7ede651dcfe03ed149706`, copied from its `develop` at
`802cfe28096ac25288522063db3ab92aba60de06` on 2026-10-24). The repository already
carries a JSON Schema validator (`google/jsonschema-go`, through the MCP SDK), so
the test loads the spec, resolves its component schemas and validates every request
body the adapter builds (types, required fields, enums, no property the schema does
not list, because the server refuses unknown fields), checks recorded responses
against the schemas and decodes them through the adapter's own DTOs, and drives each
client operation against a recording server to prove the route exists with its
`Idempotency-Key` or `vendorId` parameter. **Drift note:** the copy is not refreshed
automatically; when `vendor-connectivity` changes its contract, copy the new file
over it, record the new commit here and fix what the test reports.

### 10. Configuration

`NETWORK_MODE=live` requires `NETWORK_BASE_URL` and `NETWORK_VENDOR_ID`; a missing
one stops the boot with an error naming it. `NETWORK_NODES_FILE` is optional.
`stub` stays the default everywhere. There is no credential: the fleet has none
and neither has the Vendor API. `/readyz` does **not** depend on the network: it
keeps checking Postgres and Kafka only, because a network outage must not take the
read surface, the sweeps and the reconciliation of already-sent answers out of
rotation; the breaker gauge and the poller's error logs are the signals.

### 11. Seams left for ADR 0027

`DeclareCapability` and `SubmitAvailability` stay on the port. The node map
(`network_nodes`) gives the supply loop its `siteCode -> nodeId`; the ledger
already has the `kind` column an `INVENTORY` or `NODE_PROFILE` submission needs; the
client has the retry, 429 and breaker behaviour; `listing_id <-> network_product_id`
rows exist for the lines this context has seen, but the supply loop needs the
listing of every SKU the dictionary knows, which the demand loop cannot provide.

## Consequences

- The fleet can answer real purchase orders: acknowledged, rejected with the right
  reason, reconciled, labelled and confirmed, and a kill between any two of those
  steps ends with one order here and one write on the network.
- Acknowledgement deadlines are the network's, which for a late-evening order is far
  shorter than 24 hours.
- Known residuals, none introduced here: a shipment confirmation whose HTTP call
  fails after the order was saved `CONFIRMED` is not retried by the use case
  ([ADR 0018](./0018-godog-bdd-acceptance-tests.md) finding 3); the network's
  `FAILURE` of a shipment confirmation is recorded but no use case reconciles it; a
  single package per order until a fleet fact carries the real count.
- One new migration (0006), four tables, one optional config file, two new env
  vars (`NETWORK_VENDOR_ID`, `NETWORK_NODES_FILE`), no new dependency module (the
  contract test promotes `google/jsonschema-go` and `yaml.v3`, already in
  `go.sum`).
- The package is renamed `outbound/network/` to `outbound/retailnetwork/`
  ([ADR 0009](./0009-retail-network-not-amazon-counterpart.md) §2) in the same
  change; the stub, its seed loader and `NETWORK_SEED_FILE` move with it unchanged.

## Alternatives considered

- **Use the `poNumber` as the `NetworkRef`.** Simpler and self-healing after a lost
  map, but it makes the network's identifier the fleet's key by convention, which is
  what hard rule 4 forbids; a minted ref plus a persisted map is the rule applied.
- **A shipment `packages[]` of invented weights.** Not needed: the API takes package
  numbers only. A real count arrives with the order-level manifested fact.
- **Registering nodes at boot.** The registration is asynchronous; blocking the boot
  on it would turn a slow network into a crash loop. Converging on each poll is
  restart-safe and needs no extra worker.
- **Mapping `ACKNOWLEDGEMENT_DEADLINE_MISSED` to `CANNOT_MEET_SHIP_BY`.** The write
  would be refused (`ack-window-closed`) and would claim a reason that is not true.
- **Making `/readyz` depend on the network.** Rejected, Decision 10.
