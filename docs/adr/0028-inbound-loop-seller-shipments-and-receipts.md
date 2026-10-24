---
id: 0028-inbound-loop-seller-shipments-and-receipts
slug: /adr/0028-inbound-loop-seller-shipments-and-receipts
title: "28. The inbound loop: seller shipments in, receipts out"
sidebar_label: "28. Inbound loop"
sidebar_position: 28
description: "ADR 0028 (Accepted) builds the inbound half of the fleet-to-network seam: poll the Vendor API for seller shipments (SSCC-labelled cartons) and register them as inbound-receiving ASNs, return what the dock received (with carton discrepancies) through POST /vendor/inbound-receipts exactly once per shipment, and declare catalogue packaging in product-master. ADR 0027 is the supply loop."
---

# 28. The inbound loop: seller shipments in, receipts out

## Status

**Accepted** (2026-10-24). Builds on [ADR 0026](./0026-live-retail-network-adapter-demand-loop.md)
(the live Vendor API client, the submission ledger, the node map, `NETWORK_MODE=live`)
and is independent of ADR 0027 (the supply loop: node profile and inventory feed).
Implements P5 of the mastercase/GTIN contracts (section 10) and section 5 (the
fleet-to-network seam). The ASN carton model, the discrepancy kinds and the
eaches-only rule are inbound-receiving's; the Vendor API side is vendor-connectivity
ADR 0005.

## Context

Sellers now ship SSCC-labelled cartons (and pallets) to our sites. The network knows
the shipment; the fleet must learn about it before the truck is at the dock, and must
tell the network what the dock actually received, carton by carton, discrepancies
included. Both directions cross the one seam the fleet has: this context, the ACL, and
the Vendor API. Read from the merged contracts:

- `GET /vendor/inbound-shipments?vendorId=&nodeId=&createdAfter=&nextToken=&limit=`
  returns only `SHIPPED` shipments bound for the vendor's nodes, oldest first, paged,
  `createdAfter` comparing with `shippedAt`. A shipment leaves the poll once the network
  records its receipt. The tree is `{shipmentId, sellerId, nodeId, shippedAt, status,
  pallets[{sscc, cartons[]}], cartons[]}`; a carton is `{sscc, parentSscc?, kind,
  contents[{gtin, qtyEaches, lot?, expiry?}], grossWeightG?, dimsMm?}`. Quantities are
  eaches, GTINs 14 digits, SSCCs 18 digits.
- `POST /vendor/inbound-receipts` is asynchronous (`202 {transactionId}`, kind
  `INBOUND_RECEIPT`), takes an `Idempotency-Key`, and the body is `{vendorId, shipmentId,
  nodeId, receivedAt, cartons[{sscc, status(RECEIVED|MISSING|UNEXPECTED|DAMAGED),
  items[{gtin, qtyEaches}], complianceCodes[]}]}`. More or fewer eaches than the manifest
  is a discrepancy to report, never a refusal; a carton not on the manifest is refused
  unless it is `UNEXPECTED`; a `MISSING` carton has no items.
- inbound-receiving: `POST /asns` (Idempotency-Key required) accepts `ownerRef` and
  hierarchical `cartons[]` next to the per-SKU `lines[]` (in eaches), and rejects a
  carton set whose per-line sum differs from the line (`carton-contents-mismatch`) and a
  duplicate SSCC. **Drift found:** the code accepts `ownerRef` and `cartons` on
  `POST /asns` (its end-to-end test registers one), but `RegisterAsnRequest` in its
  `apis/openapi.yaml` does not list them (`additionalProperties: false`). We build to the
  accepted shape and report the gap; the client test pins the body we send.
- inbound-receiving publishes `ReceiptCartonReceived` (identity and contents of every
  carton accepted, per carton, eaches and condition), `CartonDiscrepancyRaised` (the
  carton findings, and **only** there) and `ReceiptClosed` (the three v1 line kinds) on
  `warehouse.inbound-receiving.events`, keyed by ASN number. It carries no site.
- product-master `GET /gtins/{gtin}` answers `{sku, level, unitsPerPack, active}` and
  says itself that no service should call it on a hot path; every consumer is meant to
  hold a projection of the pack-level events. This loop is a background job, not a
  request path, and it needs a GTIN once.

## Decision

### 1. A port of its own, our vocabulary

`ports.InboundGateway` has `PollInboundShipments(ctx, site, since)` and
`SubmitInboundReceipt(ctx, receipt)`. It is **not** two more methods of `NetworkGateway`:
that interface has six implementers (the stub, the live gateway and four test doubles)
and ADR 0027 edits it in parallel. The live and the stub gateway implement both. The
types (`contract.InboundShipment`, `InboundCarton`, `InboundReceipt`, ...) carry a
`SiteId`, never a node id; the shipment reference is the network's opaque identifier and
the key of everything this context records about it. Quantities are eaches.

### 2. The live client reuses ADR 0026's machinery

`apiClient` (per-attempt timeout bounded by the caller's deadline, jittered backoff for
transport errors and 5xx, `429` waits for `Retry-After`, one circuit breaker) serves the
two new operations unchanged. `PollInboundShipments` asks `createdAfter = since - 2
minutes` (ADR 0026 Decision 5), pages by `nextToken` (a repeated token is an error), keeps
`SHIPPED` only, drops a shipment repeated inside the poll, and flattens pallets: a carton
on a pallet carries the pallet SSCC as `parentSscc`. A site with no node yet returns an
empty answer without a request (nodes converge on the demand poll).
`SubmitInboundReceipt` writes a ledger row (`kind INBOUND_RECEIPT`, subject = shipment
reference, key `nf-rcpt-<shipment>`, the exact body stored before the first call), sends
once, stores the `transactionId`, reconciles it, and returns `ErrWriteRefused` carrying the
network's errors on a `FAILURE` or a definitive refusal. Exactly once per shipment.

### 3. Intake: poll, record, register

`IntakeInboundShipments` runs on a ticker (`INBOUND_INTAKE_INTERVAL`, default 30 s, live
mode only) for every owned site (`SITE_ID` plus the node-file sites). Per site it polls
from the **persisted cursor** (`inbound_poll_cursors`, the newest `shippedAt` recorded;
never moved back), records each shipment as `PENDING` in `inbound_shipments` (shipment
reference, deterministic ASN number, the tree as polled), and only then advances the
cursor, so a failure to record leaves the cursor and the shipment comes back. It then
attempts every due `PENDING` shipment, oldest first, at most 100 per pass.

- **ASN number**: `NF-<shipmentRef>` when the reference is made of `[A-Za-z0-9._-]` (60
  characters at most), else `NF-` plus 40 hex digits of its SHA-256 (a sanitised copy could
  collide). A replay of the poll is the same ASN.
- **Mapping**: cartons keep SSCC, parent SSCC, kind, weight, dimensions and contents (lot,
  expiry); each GTIN becomes a SKU through product-master; the `lines[]` are the per-SKU
  roll-up in eaches, numbered 1..n in order of first appearance, and every carton content
  carries its `lineNo`, so inbound-receiving's per-line sum rule holds by construction;
  `ownerRef` and `supplierRef` are the seller (`supplierRef` is `NETWORK` for a seller-less
  shipment). The fleet interprets `ownerRef` nowhere.
- **GTIN to SKU**: `GET /gtins/{gtin}`, cached in Postgres (`gtin_sku_cache`; a GTIN
  identifies one `(sku, level)` for good, so only the activity flag can change). Only an
  answer is cached: a `404` is `ErrGtinNotFound` and is never stored. A GTIN that is not an
  `EACH` GTIN is a verdict about the data (the manifest counts eaches of an each GTIN).
- **Failures are recorded, never guessed.** An unknown GTIN, a transport failure, an SKU
  inbound-receiving has not caught up with (`unknown-sku`), a `409 concurrent-update`: the
  shipment stays `PENDING` with `last_error`, `attempts + 1` and `next_attempt_at` after a
  backoff of one minute doubling to one hour (`IntakeBackoff`), logged at `WARN`. A verdict
  about the data (any other `400`/`422`, no cartons, a non-each GTIN) parks the shipment as
  `REJECTED` (logged at `ERROR`, never retried; an operator re-arms it with SQL). `409
  asn-already-exists` is success: the ASN number can only be ours, so this is a replay.
- **Calls**: `POST /asns` with `Idempotency-Key: nf-asn-<asn number>`, through a small
  client of its own (`fleetrest`: timeout, retry for 5xx and transport errors, no retry of a
  `4xx`, a breaker per dependency, `circuit_breaker_state{dependency="inbound-receiving"}`
  and `"product-master"`), because those calls are idempotent by construction.

### 4. Receipt return

- A consumer group of our own (`INBOUND_RECEIPT_CONSUMER_GROUP`, mandatory in live mode, the
  chart renders it and refuses to render without it) reads inbound-receiving's
  `warehouse.inbound-receiving.events` (kafka-go reader, `MaxWait` 500 ms, start at the earliest
  offset for a new group, commit after handling). It is a **durable** group, unlike the replay
  caches' per-process groups: a receipt closed while the pod was down is read when it returns.
  It dispatches on the full CloudEvents `type`, ignores every other type, retries a failing
  handler three times and then dead-letters to `<topic>.dlq` (the analytics consumer's rule),
  and projects `ReceiptCartonReceived`, `CartonDiscrepancyRaised` and `ReceiptClosed` into local
  tables (migration 0009) keyed by **ASN number**, only for ASNs in `inbound_shipments`; the id of
  every applied CloudEvent is kept in the same transaction, so a redelivery changes nothing. The
  ASN number is the Kafka key (inbound-receiving's rule); `CartonDiscrepancyRaised` carries none in
  its data, so the key is the source. Same key and topic means the carton and discrepancy events of
  a receipt arrive before its `ReceiptClosed`; the receipt is built at report time from the
  projection, not at close time.
- A consumer that stops on its own (a real fetch error, not a cancel) makes the process shut down
  with exit code 1, like the capability caches: a service that silently stopped hearing about
  receipts is worse than one that restarts.
- `ReturnInboundReceipts` (a ticker, the same `INBOUND_INTAKE_INTERVAL`) reports every closed,
  unreported receipt: per carton **RECEIVED** (every counted unit Good), **DAMAGED** (any
  `Damaged`), **MISSING** (a manifest carton never accepted: the close raises `MissingCarton` for it
  and, a closed receipt being the fact, the report does not depend on that event), **UNEXPECTED** (a
  carton off the manifest that the dock accepted or flagged `UnexpectedCarton`; items are what was
  counted, empty when the scan was refused); `items[{gtin, qtyEaches}]` are the counted eaches per
  GTIN summed over conditions, in GTIN order; `complianceCodes` are the carton's other discrepancy
  kinds in UPPER_SNAKE (`CaseQtyMismatch` becomes `CASE_QTY_MISMATCH`, `CartonOverLimit` becomes
  `CARTON_OVER_LIMIT`), sorted, without duplicates, at most 20 (the network's limit). `receivedAt` is
  the close; the node comes from the shipment record (`ReceiptClosed` has none).
- **What cannot be reported**: a finding with no SSCC (`UnknownGtin`, `UnlabelledCarton`) and a
  carton the dock accepted without a readable SSCC (it has only an internal LPN). The receipt has
  no place for either (a carton is identified by its SSCC, and one not on the manifest is only
  accepted as `UNEXPECTED`); both are logged at `WARN` and stay in the projection.
- **Exactly once**: `SubmitInboundReceipt` is idempotent (Decision 2: ledger row, stored body,
  deterministic key). The loop marks the receipt `SENT` after the call returns; a crash in between
  replays the same stored request. A failure is retried with the same 1 minute to 1 hour backoff as
  the intake and logged at `WARN`; a refusal that can never succeed (`contract.ErrReceiptRefused`,
  from a definitive 4xx or from the transaction ending in `FAILURE`) parks the receipt as
  `REJECTED` and is logged at `ERROR` with the network's errors. The transaction id is in the ledger
  (`network_submissions`, kind `INBOUND_RECEIPT`, subject = the shipment reference), next to its
  status.

### 5. Catalogue packaging to product-master (step 4)

Decided in the commit that builds it, only if the contracts allow it without guessing.

## Consequences

- A seller shipment is an ASN before it arrives, once, whatever the poll replays, and the
  record of it survives a restart.
- Three new boot variables in live mode (`INBOUND_RECEIVING_URL`, `PRODUCT_MASTER_URL`,
  `INBOUND_RECEIPT_CONSUMER_GROUP`), each a boot error naming it and a chart
  render error; `INBOUND_INTAKE_INTERVAL` is optional. Stub mode reads none and runs no
  loop.
- Migrations `0008` (three tables and the widened kind check) and `0009` (the receipt projection and queue). It admits `NODE_PROFILE` too,
  so it is correct whichever of this and ADR 0027's migration `0007` lands first.
- The vendored Vendor API contract is refreshed (blob `fa5545b7b66b8f51afeed0b1e5a4581f46a9f890`,
  last changed on vendor-connectivity's `develop` by commit
  `122d73cef75835a9d54b695855085a5808480536`, `develop` at
  `c31dc13ddd576d1803650683b8a1652808578394`, 2026-10-24) and the contract test now checks the
  inbound request body, a recorded shipment page and the two routes.
- Residual risks, none new: a hard kill between a successful `POST /asns` and the status
  update is repaired by the `409 asn-already-exists` rule; the receipt is repaired by the
  ledger row.

## Alternatives considered

- **Two more methods on `NetworkGateway`.** Rejected, Decision 1.
- **A synchronous GTIN lookup per carton with no cache.** product-master asks callers not
  to; the cache turns a shipment of 5,000 cartons into one call per distinct GTIN, once.
- **A local projection of product-master's pack-level events instead of REST.** The fleet
  rule (contracts section 7 R8) for the services on a hot path. This loop is not on one and
  the brief for this work names REST; the cache keeps the call count at the number of
  distinct GTINs. A projection can replace `ProductMaster` behind `GtinResolver` later.
- **Parking a shipment on its first unknown GTIN.** The product may be declared minutes
  later; a verdict is only for data that can never register.
- **Sanitising the shipment reference into the ASN number.** Two references could collide.

## As built

Filled in as each step lands (see the pull request for the state of steps 3 and 4).

1. **Steps 1 and 2** are built as Decisions 1 to 3 say. The live integration test
   (`demand_inbound_live_integration_test.go`) runs the real `run()` against a real
   Postgres and Kafka with the Vendor API, inbound-receiving and product-master as
   `httptest` fakes: one ASN for a pallet-and-mixed-carton shipment, GTINs resolved once,
   the cursor persisted, a restart that registers nothing twice, an unknown GTIN that waits
   and then registers, and a verdict that parks the shipment.
2. **Step 3** is built as Decision 4 says. `demand_inbound_receipt_integration_test.go` produces
   the events inbound-receiving would publish onto a real Kafka topic and checks the whole
   report: the statuses, items and compliance codes of the vendor receipt, a foreign ASN ignored,
   a redelivered close counted once, the transaction in the ledger, nothing sent twice across a
   restart, and a receipt the network fails parked as `REJECTED`.
