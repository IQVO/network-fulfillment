---
id: 0015-docs-audit-contract-corrections
slug: /adr/0015-docs-audit-contract-corrections
title: "15. Contract corrections from the 2026-10-05 docs audit: 409 for confirm-before-acknowledge, CORS POST, SUBMISSION_FAILED report counter"
sidebar_label: "15. Docs-audit contract corrections"
sidebar_position: 15
description: "ADR 0015 records four additive REST/report contract changes found by the 2026-10-05 docs audit: ErrConfirmBeforeAcknowledge maps to 409 problem type confirm-before-acknowledge, CORS allows POST for the shipment-confirmation endpoint, the acknowledgement report gains ordersRejectedSubmissionFailed (additive column in the analytics migration 0002), and the OpenAPI/AsyncAPI documents now describe routes and events the code already had. It also records two design notes deliberately NOT decided here."
---

# 15. Contract corrections from the 2026-10-05 docs audit

## Status

Accepted (2026-10-06). Additive to [ADR 0001](./0001-network-fulfillment-bounded-context.md)
and [ADR 0014](./0014-explicit-shipment-confirmation-endpoint.md); amends
neither. No existing field, status code, event `type` or `dataschema` is
removed or changes meaning.

## Context

The 2026-10-05 DDD documentation refresh compared the published contracts
and the code and found drift in both directions:

- `apis/openapi.yaml` did not describe `GET /capability-offers` or
  `POST /network-orders/{networkRef}/shipment-confirmation`, both
  registered in `internal/adapters/inbound/http/server.go`.
- `POST .../shipment-confirmation` on an order that is not `ACKNOWLEDGED`
  returned `500 internal-error`: `statusFor`/`problemFor` had no case for
  `networkorder.ErrConfirmBeforeAcknowledge`, so a caller-fixable business
  rule was reported as a server bug (the same defect class the existing
  `errors_test.go` guard exists to prevent).
- CORS allowed only `GET`/`OPTIONS`, so a browser could not call that `POST`
  cross-origin.
- `PostgresProjection.ApplyNetworkOrderRejected` had no counter for reason
  `SUBMISSION_FAILED` (a submitted acknowledgement that reconciliation found
  the network refused). The event was claimed and then contributed to no
  column, so these rejections vanished from the acknowledgement report.
- `apis/asyncapi.yaml` omitted `AcknowledgementDeadlineAtRisk` from the
  analytics channel although the composition root's fan-out publishes every
  event to both topics, and described four events where the code has five.

## Decision

1. **`ErrConfirmBeforeAcknowledge` maps to `409 Conflict`**, problem type
   `https://errors.network-fulfillment.warehouse-systems.dev/confirm-before-acknowledge`,
   in both `statusFor` and `problemFor`. 409 because the request is well
   formed and the order exists, but its current lifecycle state conflicts
   with it. Callers that previously saw a 500 for this case now see a 409;
   no caller can reasonably depend on the former 500.
2. **CORS allows `POST`** (`GET, POST, OPTIONS`) so the one ADR-0014 write
   endpoint is callable from the browser console. No new write endpoint is
   added; Hard rule 9 stands.
3. **The acknowledgement report row gains `ordersRejectedSubmissionFailed`**
   (REST `GET /reports/acknowledgement`, the MCP report view, and
   `report.Row.OrdersRejectedSubmissionFailed`). It is backed by a new
   additive analytics migration (`migrations/analytics/0002_...`:
   `ADD COLUMN orders_rejected_submission_failed BIGINT NOT NULL DEFAULT 0`).
   Existing columns, JSON fields and their meaning are unchanged; consumers
   that ignore unknown fields keep working. History is not back-filled:
   rows aggregated before this change read 0, and the claimed event ids in
   `analytics_processed_events` are not replayed. Rebuilding by replaying the
   analytics topic into an empty read model would populate it.
4. **The OpenAPI and AsyncAPI documents are corrected to match the code**:
   the two missing routes (with `CapabilityOffer` schema, 204/404/409/422/500
   for the POST), the analytics-channel `AcknowledgementDeadlineAtRisk`
   message, the five-event description, the IQVO contact URL, and the real
   timing of `NetworkOrderAcknowledged` (below). The AsyncAPI changes are
   documentation only; no event `type`, `dataschema` or payload changes, so
   the CloudEvents type catalogue (ADR 0008) is unaffected.

## Not decided here (design notes the audit raised)

These were reported to the product owner rather than changed, because neither
ADR 0001 nor ADR 0009 specifies the behaviour:

- **`NetworkOrderAcknowledged` timing.** The event is published when the
  order becomes `SUBMITTED`, before `ReconcileSubmittedOrders` settles it, and
  `SUBMITTED -> ACKNOWLEDGED` raises no event. ADR 0001 §5 requires the
  submitted-but-unreconciled state to be *visible* and AGENTS.md rule 8
  requires work to be released only after reconciliation; neither says when
  the integration event fires. The AsyncAPI now documents the true timing so
  consumers are not misled. Moving the event, or adding `NetworkOrderSubmitted`
  / a settled event, is an event-contract change that needs its own decision
  (see the audit report for the options).
- **`contract.EligiblePath.CycleTimeP95`** is cached but unused by
  `RecomputeCapabilityOffers.throughputFeasible`. ADR 0001 lists
  `cycleTimeP95 + CPTSchedule` as inputs to `throughputFeasibleBefore` but
  does not specify how cycle time combines with remaining capacity, so no rule
  is invented here.

## Consequences

**Easier:** the published REST and event contracts describe what the service
actually does; confirm-before-acknowledge is a diagnosable 409; operators see
network-refused submissions in the acknowledgement report; the browser
console can call the shipment-confirmation endpoint.

**Harder / to watch:** the new report counter starts at 0 for history that
predates the migration. The two design notes remain open and the AsyncAPI
text now states them plainly, so changing either later is visibly a contract
change.
