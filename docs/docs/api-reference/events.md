---
id: events
title: Event catalogue
sidebar_label: Event catalogue
sidebar_position: 2
---

# Event catalogue

Every Kafka message this service produces or consumes is a CloudEvents 1.0 event in
**structured mode**: the Kafka value is the whole JSON envelope and the header
`content-type` is `application/cloudevents+json; charset=UTF-8`. Envelopes are built
and decoded in `internal/adapters/kafka/cloudevents`
([ADR 0008](/docs/adr/0008-cloudevents-mandatory-event-envelope)). The contract is
`apis/asyncapi.yaml` (AsyncAPI 2.6.0, version 2.0.0); field-level detail is on
[Domain events](/docs/ddd/domain-events).

:::info Postgres and Kafka are the only runtime
Events are written to `outbox_events` in the same transaction as the state change and
sent by the relay in `cmd/netfulfil` ([ADR 0003](/docs/adr/0003-transactional-outbox),
[ADR 0025](/docs/adr/0025-postgres-and-kafka-are-the-only-runtime)). The tables below are
the pinned contract, not observed traffic. Whether the other side of each edge exists
is on [Integration](/docs/ecosystem/integration).
:::

## Published

Source `/warehouse/network-fulfillment`; `subject` and the Kafka key are the
`networkRef`; `dataschema`
`urn:warehouse:network-fulfillment:<events|analytics>:<EventName>:v<N>` (v1, except
`NetworkOrderAcknowledged`, which is v2, [ADR 0016](/docs/adr/0016-cloudevents-submitted-and-settle-time-acknowledged)).
Each occurrence goes to both `warehouse.network-fulfillment.events` (integration)
and `warehouse.network-fulfillment.analytics` (this context's own projector), with a
distinct `id` per topic row. All types carry the prefix
`com.warehouse.wes.network-fulfillment.networkorder.`.

| Type (after the prefix) | Raised when | `data` |
| --- | --- | --- |
| `NetworkOrderReceived` | demand is turned into an order, also for untranslatable demand (`lineCount: 0`) | `networkRef`, `siteId`, `requiredShipBy`, `acknowledgeBy`, `lineCount`, `at` |
| `NetworkOrderSubmitted` | the order goes `SUBMITTED` (acknowledgement told to the network) | `networkRef`, `siteId`, `localOrderId`, `receivedAt`, `at` |
| `NetworkOrderAcknowledged.v2` | the network confirms the submission (`ACKNOWLEDGED`) | `networkRef`, `siteId`, `localOrderId`, `receivedAt`, `at` |
| `NetworkOrderRejected` | untranslatable SKU, infeasible deadline, deadline missed, or the network refused the submission | `networkRef`, `siteId`, `reason` (`UNTRANSLATABLE_SKU`, `INFEASIBLE_DEADLINE`, `ACKNOWLEDGEMENT_DEADLINE_MISSED`, `SUBMISSION_FAILED`), `at` |
| `NetworkOrderShipmentConfirmed` | `POST /network-orders/{networkRef}/shipment-confirmation` | `networkRef`, `siteId`, `localOrderId`, `at` |
| `AcknowledgementDeadlineAtRisk` | every sweep pass while a `NEW` order stays overdue (level-triggered: expect repeats) | `networkRef`, `siteId`, `acknowledgeBy`, `at` |
| `OrphanedHoldCancelled` | the orphaned-hold sweep freed an order-management hold nobody decided ([ADR 0024](/docs/adr/0024-orphaned-order-management-hold-sweep)) | `networkRef`, `siteId`, `localOrderId`, `acknowledgeBy`, `answer` (`CANCELLED` or `NOT_FOUND`), `at` |

The type string of `NetworkOrderAcknowledged` v2 is
`...networkorder.NetworkOrderAcknowledged.v2`. The unsuffixed v1 type was published
at submission before ADR 0016 and still sits on the analytics topic; the projector
counts it so a replay keeps its numbers. No consumer of the integration topic was
found in the local fleet checkouts (the asyncapi marks the facts of the
external-network channel as having no consumer by decision).

Optional Kafka headers: a W3C `traceparent` (and `tracestate` / `baggage`) when the
event was written inside a span ([ADR 0022](/docs/adr/0022-trace-context-crosses-the-outbox)).
Dedupe on the CloudEvent `id`; per-key order holds except for a row that hit
`OUTBOX_MAX_ATTEMPTS` ([ADR 0021](/docs/adr/0021-outbox-relay-attempt-cap)).

## Consumed

| Type | Topic | Used for |
| --- | --- | --- |
| `com.warehouse.wes.process-path-management.processpath.ProcessPathCreated` | `warehouse.process-path-management.events` | capability cache: the path's `cycle_time_p95` |
| `com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated` | `warehouse.process-path-management.events` | replaces it |
| `com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated` | `warehouse.process-path-management.events` | forgets the path |
| `com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged` | `warehouse.process-path-management.events` | the site's cutoffs and timezone |
| `com.warehouse.wes.work-planning.workpool.PathCapacityChanged` | `warehouse.work-planning.events` | remaining admission capacity per `(path_id, cutoff_at)` |
| `com.warehouse.wms.inbound-receiving.receipt.ReceiptCartonReceived` | `warehouse.inbound-receiving.events` | inbound loop: a carton of a receipt was received ([ADR 0028](/docs/adr/0028-inbound-loop-seller-shipments-and-receipts)) |
| `com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised` | `warehouse.inbound-receiving.events` | inbound loop: an unexpected or missing carton |
| `com.warehouse.wms.inbound-receiving.receipt.ReceiptClosed` | `warehouse.inbound-receiving.events` | inbound loop: the receipt is complete and is reported to the network |

The two capability caches read from the earliest offset with a group id that is
unique per process; the receipt consumer uses the stable, durable group named by
`INBOUND_RECEIPT_CONSUMER_GROUP` (live mode only). Every other type on those topics is
ignored. A message that is not a valid CloudEvent is logged and skipped by the two
caches (no DLQ), and sent to `warehouse.inbound-receiving.events.dlq` by the receipt
consumer. The analytics consumer in `netfulfil-projector` reads
`warehouse.network-fulfillment.analytics`, handles `NetworkOrderReceived`,
`NetworkOrderSubmitted` (claimed, no report effect), `NetworkOrderAcknowledged` (v1
and v2) and `NetworkOrderRejected`, ignores the other three types, retries a failing
message 3 times and then dead-letters it to
`warehouse.network-fulfillment.analytics.dlq`. See the
[Runbook](/docs/operations/runbook#kafka) for topics, groups and the DLQ procedure.
