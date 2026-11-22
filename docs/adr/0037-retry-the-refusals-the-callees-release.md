---
id: 0037-retry-the-refusals-the-callees-release
slug: /adr/0037-retry-the-refusals-the-callees-release
title: "37. Retry, under the same Idempotency-Key, the refusals the callees release (replay class RETRYABLE)"
sidebar_label: "37. Retry released refusals"
sidebar_position: 37
description: "ADR 0037: consumer half of the fleet idempotency rule of 2026-11-18. Each outbound adapter keeps one visible table from the callee's problem slug to retryable or terminal (inbound-receiving ADR 0027 for POST /asns, order-management ADR 0054 for POST /orders and POST /orders/{id}/releases). A refusal the callee releases keeps the step pending under the same key, bounded and ending in the terminal state it always ended in; FINAL, unknown and no problem document are terminal. Fixes the concurrent-update/concurrent-modification slug mismatch."
---

# 37. Retry, under the same `Idempotency-Key`, the refusals the callees release (replay class `RETRYABLE`)

## Status

**Accepted** (2026-11-19). Consumer half of the fleet idempotency rule of 2026-11-18
(`.claude/rules/fleet/idempotency-and-outbox.md`). It answers the Consequences of
inbound-receiving ADR 0027 and order-management ADR 0054, which name this service as a caller
with deterministic keys, and it refines [ADR 0028](./0028-inbound-loop-seller-shipments-and-receipts.md)
(the ASN registration) and [ADR 0024](./0024-orphaned-order-management-hold-sweep.md) /
[ADR 0032](./0032-adopt-the-fleet-rest-api-guideline.md) (the held-order calls) without rewriting
them. The outbound Vendor API (`nf-<kind>-<subject>` keys, vendor-connectivity) is an external
system, not a fleet store: it is not touched. No schema, wire contract or `apis/openapi.yaml`
change.

## Context

Two calls of this service carry a deterministic key, so a retry is a replay:

| Call | Route | Key | Caller |
| --- | --- | --- | --- |
| register an ASN | inbound-receiving `POST /asns` | `nf-asn-<asnNumber>` | `IntakeInboundShipments` (PENDING shipment, backoff) |
| raise the hold | order-management `POST /orders` | `netful-raise-<networkRef>` | `ReceiveNetworkDemand` (the order stays `NEW`, the poll re-delivers it) |
| release the hold | order-management `POST /orders/{id}/releases` | `netful-release-<localOrderId>` | `ReconcileSubmittedOrders.confirm` |

Before the rule a callee stored every answer below 500 for 24 h, so retrying any refusal was
pointless. Now each callee declares a replay class per problem type: a `RETRYABLE` refusal
committed nothing, is NOT stored and releases the key, so the same request under the SAME key
executes again and may pass; a `FINAL` or `OUTCOME` one is stored and replayed. This service
treated the refusals by guesses that did not follow those classes:

- ASN: it retried `unknown-sku` and the slug `concurrent-update`, but inbound-receiving answers
  `concurrent-modification` (an unknown slug here, so a generic error) and `duplicate-sscc` is
  `RETRYABLE` there although it was read as a verdict; a `409`/`404` was never a refusal at all
  (a `409 asn-terminal` looped forever as a generic error); `unknown-sku` only starts to work now
  that inbound-receiving releases it.
- order-management: the client read no problem document: every non-2xx was one opaque
  `status <n>`.

## Decision

1. **The class is the callee's knowledge and lives in the anti-corruption layer.** Each outbound
   adapter keeps ONE visible table from the callee's problem slug (the last path element of
   the problem document's `type`) to retryable or terminal, derived from the callee's class
   table: `inboundreceiving/refusal_classes.go` (`asnRefusals`) and
   `ordermanagement/refusal_classes.go` (`heldOrderRefusals`), both built with
   `outbound/replayclass` (`NewTable` refuses a slug listed twice). The use cases and the domain
   see no slug: only `AsnRefusedError.Retryable` and `ordermanagement.RefusalError.Released`.
2. **Unknown is terminal.** A slug not in the table, a 4xx with no problem document and a body
   that is not a problem document are terminal, as is a `FINAL` or `OUTCOME` slug. The table is
   read for `400`, `404`, `409` and `422` of the keyed routes only: a `401`, `403`, `429` and any
   `5xx` were never verdicts and stay plain errors the next pass retries. `DELETE /orders/{id}`
   is not keyed: the cancel keeps its answers (ADR 0024 section 4).
3. **ASN registration: a released refusal keeps the shipment PENDING.** The shipment is
   retried with the existing backoff (one minute doubling to one hour), the same ASN number and
   so the same key and the same body. Because this loop had no bound for a retryable refusal
   (it was only ever retried pointlessly), one is added with no schema change: `rec.Attempts`
   (the failed attempts, already stored) is compared with
   `IntakeInboundShipments.MaxReleasedRefusalAttempts` (default
   `DefaultReleasedRefusalAttempts` = 30, about 24 h of backoff, the time the callee keeps a
   stored answer). The attempt that reaches it is recorded `REJECTED` with the refusal and "giving
   up", the state a final refusal has always had (`last_error`, ERROR log, the SQL re-arm of the
   runbook). Only a released refusal is bounded: a transport failure or a 5xx keeps retrying as
   before. `Attempts` counts every failure, so a long outage near the end can shorten the window;
   that is accepted (no counter column).
4. **order-management raise: the existing pending mechanism already is the bound.** A refused
   raise leaves the order `NEW`; the poll re-delivers it and the raise runs again with the same
   key; once `acknowledgeBy` passes the re-delivery leaves it to the sweep and
   `RejectOverdueOrders` rejects it `ACKNOWLEDGEMENT_DEADLINE_MISSED`. That is the same for every
   class, so nothing in the use case changes: the released refusals (`unknown-process-path`,
   `line-ineligible-for-resolved-path`, `concurrent-modification`) now simply pass when retried.
   What changes is the error: `RefusalError{Status, Problem, Detail, Released}` wraps the
   status error (its text keeps `status <n>`) and says "released, a retry under the same key may
   pass" or "final, a retry under the same key is answered the same", which the poller logs.
5. **order-management release: classified, not retried.** The release runs after the order is
   saved `ACKNOWLEDGED`, and nothing records "release confirmed", so no pass retries a refused
   release (residual P7 of ADR 0024, unchanged). The classification is returned all the same;
   retrying would need a schema or an ordering change, see Open points.
6. **The tables are held against the callees' contracts.**
   - inbound-receiving: `refusal_spec_test.go` reads the byte-pinned copy of its OpenAPI
     (`contracts/pinned.json`, refreshed in this change): the `Idempotency-Key` text lists the
     released slugs, the `400`/`409`/`422` responses of `POST /asns` name the rest. The test fails
     when a slug the spec names is unclassified, a released slug is not retryable, a retryable
     slug is not released, or an exception below is stale; a second test proves the audit catches
     each drift.
   - order-management: its ADR 0054 (PR #305, merged) put the replayed and the released slugs
     in the `Idempotency-Key` text of its OpenAPI, so the pin (refreshed in this change) holds
     both lists: `refusal_classes_test.go` fails when a slug the pin replays is not terminal
     here, a slug it releases is not retryable here, or a problem type named on the two keyed
     routes is unclassified. The pin does not say the class of every problem type, so the
     table is also held equal to a COPY of the class table of ADR 0054
     (`testdata/order-management-adr-0054-classes.tsv`, 32 rows, 18 `RETRYABLE`): no slug is
     missing and none is invented. Refresh the copy when ADR 0054 changes.

### The tables

inbound-receiving, `POST /asns` (ADR 0027). 72 problem types, 44 `FINAL`, 28 `RETRYABLE`; the
table lists the 26 `RETRYABLE` that are not a 5xx and the `FINAL` ones this route can name:

| Slug(s) | Status | Callee class | Here | Why |
| --- | --- | --- | --- | --- |
| `unknown-sku` | 422 | RETRYABLE | **retry** | the product projection has not caught up with product-master |
| `concurrent-modification` | 409 | RETRYABLE | **retry** | a write lost a race; replaces the slug `concurrent-update`, which inbound-receiving never answers |
| `duplicate-sscc` | 422 | RETRYABLE | **retry** | the SSCC sits on another ASN; cancelling it frees it. Was a verdict (REJECTED at once) |
| `idempotency-key-reused` | 409 | RETRYABLE | **retry** | never stored; the key row of an earlier answer expires after 24 h, then `asn-already-exists` is success. The OpenAPI text does not list it (exception in the test) |
| `unknown-asn`, `unknown-appointment`, `asn-not-on-appointment`, `line-not-on-asn`, `asn-not-found`, `appointment-not-found`, `receiving-policy-not-found`, `carton-not-found`, `asn-in-progress`, `asn-not-receivable`, `receipt-already-open`, `tolerance-override-required`, `tolerance-blocked`, `sscc-prefix-not-configured`, `sscc-serial-exhausted`, `counted-contents-not-mixed`, `counted-sku-unknown`, `scan-kind-mismatch`, `unsupported-pack-level`, `unknown-gtin`, `version-mismatch` | 404/409/412/422 | RETRYABLE | retry | not answered by `POST /asns` today; classified as the callee classes them |
| `malformed-request` | 400 | RETRYABLE | **terminal** | mixed slug at the callee (its ADR 0027 judgement 1: invalid JSON or an unparsable `If-Match`); this client sends its own DTO bytes and no `If-Match`, the same on every attempt, so a retry cannot pass. Recorded in the test as `releasedButTerminalHere` |
| `asn-terminal`, `appointment-cancelled`, `receipt-closed`, `receipt-not-found`, `homogeneous-carton-mixed`, `carton-contents-mismatch`, `unsupported-gtin`, `duplicate-sku`, `duplicate-asn-number`, the `invalid-*` and `*-requires-*` validations, `invalid-limit`, `invalid-cursor`, `invalid-query`, `unknown-query-parameter` | 400/404/409/422 | FINAL | terminal | stored and replayed; a retry only repeats it |
| `idempotency-key-required`, `idempotency-key-invalid` | 400 | FINAL | terminal | this client sends a valid key |
| `asn-already-exists` | 409 | FINAL | **success** | the number is derived from the shipment, so the ASN is ours; read before the table |
| `unknown-dock-door` | 422 | not in its table | terminal | named by the OpenAPI text, not answered by this route |
| any other slug, no problem document | 400/404/409/422 | unknown | terminal | an unknown refusal never loops |

order-management, `POST /orders` and `POST /orders/{id}/releases` (ADR 0054). 32 problem types,
14 `FINAL`, 18 `RETRYABLE`, no `OUTCOME`:

| Slug(s) | Status | Callee class | Here | Why |
| --- | --- | --- | --- | --- |
| `unknown-process-path` | 400 | RETRYABLE | **retry** | the process-path catalogue is fed by events |
| `line-ineligible-for-resolved-path` | 422 | RETRYABLE | **retry** | product classification and catalogue are fed by events |
| `concurrent-modification` | 409 | RETRYABLE | **retry** | lost an optimistic-concurrency race |
| `order-not-held` | 409 | RETRYABLE | retry | release only; callee's tie-break |
| `no-backordered-lines`, `no-allocated-lines`, `promise-date-not-set`, `order-already-released`, `order-line-already-allocated`, `order-line-not-allocated`, `order-line-not-backordered`, `order-line-not-found`, `order-line-not-pending` | 400/409 | RETRYABLE | retry | not answered by these two calls; classified as the callee classes them |
| `insufficient-stock`, `downstream-not-configured`, `downstream-unavailable`, `internal-error`, `report-store-error` | 5xx | RETRYABLE | retry | a 5xx is never read as a refusal, so never consults the table |
| `malformed-request-body`, `order-without-lines`, `empty-sku`, `empty-order-id`, `empty-path-id`, `invalid-fulfillment-site-id`, `non-positive-quantity`, `held-order-must-be-ship-complete`, `invalid-query-parameter`, `invalid-report-query` | 400/422 | FINAL | terminal | a function of the request |
| `order-not-found` | 404 | FINAL | terminal | ids are minted there and never deleted |
| `idempotency-key-required`, `idempotency-key-invalid`, `idempotency-key-reused` | 400/409 | FINAL | terminal | the keys are never empty, one per network order and per held order |
| any other slug, no problem document | 400/404/409/422 | unknown | terminal | an unknown refusal never loops |

## Consequences

- A shipment whose SKU inbound-receiving does not know yet (or whose registration lost a race, or
  whose SSCC is on an ASN that is later cancelled) is registered by a later pass under the same
  key instead of waiting on a stored refusal. Past about 24 h it is `REJECTED` with the refusal,
  as a final refusal is. `unknown-sku` used to be retried without any end; it now ends.
- Re-arming a `REJECTED` shipment by SQL must also reset `attempts` (runbook), or the first
  released refusal would end it again at once.
- A `409 asn-terminal`, `receipt-closed` or any other FINAL refusal of `POST /asns` is a verdict
  (`REJECTED`) instead of an error retried forever; a `404` or `409` with an unknown slug too.
- `duplicate-sscc` is retried instead of parked at once; the cost is up to 30 requests per such
  shipment over a day. Flip it to terminal (one word and one test row) if the owner prefers the
  immediate verdict.
- order-management raise behaviour is unchanged (retried every poll inside the 24 h window, then
  rejected overdue); what is new is that a retry can now pass, and the refusal and its class are
  in the log line. Its circuit breaker still counts a 4xx as a failure, as before.
- Each table copies knowledge the callee owns: a change of the callee's class table is a change
  of this table. The pinned OpenAPI (inbound-receiving) and the ADR copy (order-management) are
  the early warning; pins are refreshed by copying.

## Open points (not decided here)

- **A refused release is lost.** `confirm` saves `ACKNOWLEDGED` and publishes before it releases,
  and `ListSubmitted` no longer lists the order. A `concurrent-modification` or `order-not-held`
  on the release (now released by order-management) therefore cannot be retried. A fix needs a
  release marker on `network_orders` (a migration) or releasing before the state change (which
  reopens ADR 0016's record-before-release choice). Residual P7 of ADR 0024 stands.
- **A FINAL refusal of the raise is retried until the deadline.** Rejecting such an order at
  once needs a rejection reason that does not exist (`RejectionReason` is a published event
  value): a contract change, not made here.
- **The pending reason is only in `last_error`/the log.** No new column or event field.
- The breaker counts a refusal as a dependency failure; classifying answers as non-failures is
  its own decision.
