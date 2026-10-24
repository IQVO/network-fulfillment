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
live gateway that log at Info on the first call of each method and at Debug after
that, and name ADR 0027 (amended, see As built 2); the node profile
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

Five tables (`network_purchase_orders` and its lines are two), all in this
service's Postgres and all written by the adapter:

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
retries, nothing is skipped. **Amended, see As built 1 and 4:** the node refresh is
not triggered by an unknown node but runs at the start of every poll, and a
purchase order whose node is still unmapped is skipped with a warning, not failed.

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
  reason, reconciled, labelled and confirmed. The ledger row is written before the
  call, so a process death between two of those steps is designed to end with one
  order here and one write on the network; what is tested is a graceful stop (see
  As built 8), not a `SIGKILL`.
- Acknowledgement deadlines are the network's, which for a late-evening order is far
  shorter than 24 hours.
- Known residuals, none introduced here: a shipment confirmation whose HTTP call
  fails after the order was saved `CONFIRMED` is not retried by the use case
  ([ADR 0018](./0018-godog-bdd-acceptance-tests.md) finding 3); the network's
  `FAILURE` of a shipment confirmation is recorded but no use case reconciles it; a
  single package per order until a fleet fact carries the real count.
- One new migration (0006), five tables, one optional config file, two new env
  vars (`NETWORK_VENDOR_ID`, `NETWORK_NODES_FILE`), no new dependency module (the
  contract test promotes `google/jsonschema-go` v0.4.3 and `gopkg.in/yaml.v3`
  v3.0.1 from indirect to direct in `go.mod`; neither version changed and both
  were already in `go.sum`).
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

## As built

Where the code differs from, or fixes, what the Decisions above say. The Decisions
stay as written; this section wins where they disagree. Status is unchanged
(Accepted).

1. **A purchase order on an unmapped node is skipped, not failed (amends Decision
   5).** `PollDemand` converges the nodes at the start of every poll (list, adopt,
   register), then maps each purchase order's `nodeId` to a site. A node that is
   still unmapped skips that purchase order with a `WARN`
   (`purchase order skipped, its node is not mapped to any of our sites`, fields
   `nodeId` and `poNumber`) and the pass goes on, so one stray node cannot stop the
   other sites' demand. There is no metric for the skip: the log line is the only
   signal. **Residual risk, recorded on purpose:** the poller's watermark lives in
   the process and starts at zero. A pass that skipped an order still counts as
   clean, so the watermark moves past it and the next ticks (which look back only
   `pollOverlap`, 2 minutes) do not ask for it again. It is read again on the next
   boot, when the watermark is zero again and the network returns everything it
   still lists as `ISSUED`; if the node is mapped by then it is received, and if
   not, the network expires it at its `ackBy`. A skipped order can therefore be
   lost to expiry, and the `WARN` is what tells an operator to map the node.
2. **Supply-loop no-ops are quiet (amends Decision 1).** `DeclareCapability` and
   `SubmitAvailability` log at `Info` the first time each is called in a process
   and at `Debug` after that, so the recompute ticker does not fill the log.
3. **Which refusals are final (refines Decisions 4 and 6).** Only a definitive
   refusal becomes a verdict: `409` `ack-window-closed`, `illegal-transition`,
   `confirm-without-label` and `label-before-acceptance`, and `422`
   `validation-failed`. The ledger row is stored as `FAILURE` and every later call
   returns `ErrWriteRefused` without a request; `SubmissionStatus` turns it into the
   `FAILURE` that `ReconcileSubmittedOrders` acts on. Everything else stays an
   error and is retried on the next tick: a `409` `concurrent-update`, `403`,
   `404`, `422` `idempotency-key-reused`, `429` after its retries, `5xx` and
   transport errors. A network `FAILURE` of a **shipment confirmation** is logged at
   `ERROR` (`the network failed a shipment confirmation; the order stays accepted
   there until an operator acts`) and not returned, because no use case reconciles a
   confirmation; the ledger row keeps the errors.
4. **Identifier shapes (refines Decision 4).** `NetworkLineRef` is `line-<lineNo>`
   and `NetworkProductId` is the listing id, both derived once when the purchase
   order is first stored. Idempotency keys are `nf-ack-<ref>`, `nf-ship-<ref>`,
   `nf-node-<site>` and `nf-label-<ref>`. The label request is the one synchronous
   write and has **no ledger row**: its deterministic key makes a retry of the same
   body a replay on the network. Labels are stored only after the answer checks out
   (one label per package, each with a tracking number); a response that does not
   persists nothing and returns an error. Once labels are stored `RequestLabel`
   returns them whatever count is asked, so the **first package count wins**
   (the same key with another body would be `idempotency-key-reused`).
5. **Configuration (refines Decisions 7 and 10).** `LiveConfig` takes `Sites` and
   `Nodes`. The sites converged onto network nodes are `SITE_ID` (default
   `site-1`) plus every site of the node definitions. Node definitions come from
   `NETWORK_NODES_FILE` (strict JSON, `retailnetwork.LoadNodesFile`; a typo'd key, a
   missing field, half a coordinate or a duplicate site is a boot error). A site
   whose `vendorNodeCode` is already a node on the network is **adopted** (its
   `nodeId` is stored) with or without a definition, and a mapped site is never
   registered again. A site with neither a node nor a definition is a `WARN` at
   every poll, not a boot error.
   The environment, exactly: `NETWORK_MODE` (default `stub`; anything but `live`
   is `stub`); `NETWORK_BASE_URL` and `NETWORK_VENDOR_ID`, read only in live mode,
   with one boot error naming every one that is missing; `NETWORK_NODES_FILE`,
   optional and live only; `NETWORK_SEED_FILE`, stub only and refused against the
   live gateway; `DATABASE_URL` and `KAFKA_BROKERS`, mandatory in every mode. There
   is no timeout, retry or backoff variable: Decision 8's numbers are fixed in
   code.
6. **Boot order.** The gateway is built after the Postgres pool (the live one
   keeps its maps and ledger there), so a live boot without a reachable database
   never gets as far as the network. The node file is read before the database is
   dialled, like the product dictionary.
7. **One breaker gauge.** order-management's breaker and the network's report into
   one `telemetry.CircuitBreakerMetrics`, so
   `circuit_breaker_state{dependency="retail-network"}` is on `GET /metrics` next to
   `dependency="order-management"`.
8. **What the restart test proves.** `TestLiveDemand_RestartAfterAcknowledgementSubmitted`
   stops the service with a context cancel (a graceful shutdown), not a `SIGKILL`,
   between the acknowledgement being sent and its reconciliation, and starts it on
   the same database. It shows one order in order-management and one write on the
   network. A hard kill is covered only by the design (ledger row and body before
   the call, transaction id stored as soon as the `202` is read, an unsent row
   resent from the stored body), not by a test that kills the process.
9. **The vendored contract's source and how to refresh it (amends Decision 9).**
   `testdata/vendor-connectivity-openapi.yaml` is `apis/openapi.yaml` at blob
   `7393aeb57374eb7e38d98272a6851a5ee4ba36a1`, last changed on that service's
   `develop` by commit `26c54625e72343d5e1e7ede651dcfe03ed149706`
   (`git log -1 --format=%H origin/develop -- apis/openapi.yaml`); `develop` was at
   `802cfe28096ac25288522063db3ab92aba60de06` when it was copied. To refresh it:
   `git show origin/develop:apis/openapi.yaml` from a current checkout of that
   service, written byte for byte over the vendored file (no re-formatting);
   `git hash-object` of the result must equal `git rev-parse
   origin/develop:apis/openapi.yaml`; run
   `go test ./internal/adapters/outbound/retailnetwork/...` (the contract test) and fix
   what it reports; then update the three hashes above. Nothing compares the copy
   with the service automatically, so a drift stays invisible until someone does
   this.
