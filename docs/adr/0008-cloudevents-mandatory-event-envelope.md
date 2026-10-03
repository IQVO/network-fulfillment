---
id: 0008-cloudevents-mandatory-event-envelope
slug: /adr/0008-cloudevents-mandatory-event-envelope
title: "8. CloudEvents 1.0 as the mandatory event envelope"
sidebar_label: "8. CloudEvents mandatory envelope"
sidebar_position: 8
description: "ADR 0008 — every Kafka message network-fulfillment produces or consumes (integration topic warehouse.network-fulfillment.events AND analytics topic warehouse.network-fulfillment.analytics) is a CloudEvents 1.0 event in structured content mode, built and validated with sdk-go v2's event package. The flat envelope (event_id/event_type/occurred_at/source/data) and the analytics Envelope v1 (schema_version) are removed with no coexistence, as part of the fleet-wide cutover."
---

# 8. CloudEvents 1.0 as the mandatory event envelope

## Status

Accepted (2026-09-30) — implemented in the same change that introduces this
record. Fleet-wide standard; this is network-fulfillment's half of the
cutover. Supersedes the envelope description in ADR 0003 (the `Envelope` /
`AnalyticsEnvelope` shapes the outbox rows carried).

## Context

Until this change the service published two hand-rolled, "CloudEvents-like"
flat envelopes:

- integration topic: `{event_id, event_type, occurred_at, source, data}`,
  `event_type` being the short name (`NetworkOrderReceived`) and `source`
  the bare string `network-fulfillment`;
- analytics topic: the same plus `schema_version: 1` ("Envelope v1").

Its own analytics projector decoded that flat shape and dispatched on the
short `event_type`. The rest of the fleet had drifted the same way, with
some services on dual-write / dual-read migrations behind an
`EVENT_ENVELOPE_MODE` toggle. Short names collide across contexts, the flat
shape is not interoperable with any CloudEvents tooling, and toggles mean
two wire formats in flight forever.

## Decision

EVERY message this service writes to or reads from Kafka is a CloudEvents 1.0
event. No flat envelope, no dual-write, no dual-read, no envelope toggle.

### Encoding — Kafka protocol binding, structured content mode

- The Kafka message VALUE is the CloudEvents JSON event format
  (`application/cloudevents+json`).
- Every produced message carries the header
  `content-type: application/cloudevents+json; charset=UTF-8`.
- The Kafka KEY is unchanged: the network order reference (`networkRef`),
  with the `kafkago.Hash{}` balancer (ADR 0005).
- W3C trace context, if ever propagated, stays in Kafka headers — never in
  CloudEvents extension attributes.
- Events are built, validated and (un)marshalled with the official
  `github.com/cloudevents/sdk-go/v2/event` package (v2.16.2) — `event.New()`,
  setters, `Validate()`, `json.Marshal` / `json.Unmarshal`, `DataAs`. The
  sdk-go protocol/client packages are NOT used; transport stays
  `segmentio/kafka-go`.
- All helpers live in ONE package,
  `internal/adapters/kafka/cloudevents/` (`New`, `Decode`,
  `ContentTypeHeader`, `Type`, `DataSchema`). Publishers and consumers both
  go through it; nothing else hand-builds an envelope.

### Context attributes (all required)

| attribute         | value |
|-------------------|-------|
| `specversion`     | `1.0` |
| `id`              | UUID v4, minted ONCE per (occurrence, stream) at encode time and persisted inside the outbox row's value, so a relay redelivery carries the same id. `(source, id)` is the consumer idempotency key. |
| `source`          | `/warehouse/network-fulfillment` |
| `type`            | `com.warehouse.wes.network-fulfillment.networkorder.<EventName>` |
| `subject`         | the network order reference (`networkRef`) — never empty |
| `time`            | the domain event's occurred-at, UTC, RFC 3339 |
| `datacontenttype` | `application/json` |
| `dataschema`      | `urn:warehouse:network-fulfillment:<events\|analytics>:<EventName>:v1` |

`data` is byte-for-byte the payload shape published before this change
(the domain event's own JSON). The analytics `schema_version` field is
removed; `dataschema` replaces it. No extension attributes.

### Types

The general convention is
`com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`. The
same `type` names the occurrence on both topics; `dataschema` distinguishes
the payload contract. Published by this service:

    com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived
    com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderAcknowledged
    com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderRejected
    com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderShipmentConfirmed

Consumed (own analytics projector, `warehouse.network-fulfillment.analytics`):
`NetworkOrderReceived`, `NetworkOrderAcknowledged`, `NetworkOrderRejected`
under the full types above. This service consumes no other context's events.

A breaking payload change requires a new `.v2` type AND a new dataschema
version, published as a new event — never a mutation of an existing one.

For reference, the fleet's cross-service contract (exact strings, consumed
by another repo) is:

    com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered       -> inventory-storage
    com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned   -> inventory-storage
    com.warehouse.wms.facility-layout.zone.ZoneRegistered                       -> inventory-storage
    com.warehouse.wms.inventory-storage.reservation.StockReserved               -> wes-work-planning
    com.warehouse.wms.inventory-storage.reservation.ReservationRevoked          -> wes-work-planning
    com.warehouse.wes.order-management.order.OrderAllocated                     -> wes-work-planning
    com.warehouse.wes.order-management.order.OrderPartiallyAllocated            -> wes-work-planning
    com.warehouse.wes.order-management.order.OrderRepromised                    -> (published contract)
    com.warehouse.wes.process-path-management.processpath.ProcessPathCreated    -> fulfillment-execution, wes-work-planning, workforce-management, order-management
    com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated    -> same four
    com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated-> same four
    com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged    -> order-management
    com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded     -> workforce-management, labor-performance
    com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted         -> wes-work-planning
    com.warehouse.wes.fulfillment-execution.task.TaskCompleted                  -> wes-work-planning, labor-performance
    com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed                  -> order-management
    com.warehouse.wes.fulfillment-execution.package.PackageManifested           -> order-management
    com.warehouse.wes.work-planning.workunit.WorkReleased                       -> fulfillment-execution
    com.warehouse.wes.work-planning.workpool.PathCapacityChanged                -> order-management

### Consumer rules

1. Decode with `cloudevents.Decode` (SDK unmarshal + `Validate()` +
   `specversion == 1.0`). A message that is not a valid CloudEvent —
   including the retired flat envelope — is a deterministic poison
   message: the analytics consumer dead-letters it to
   `<topic>.dlq` immediately (no retry) and commits past it. It never
   crashes, never blocks the partition, never falls back to a flat parse.
2. Dispatch on the FULL `type`. Unknown types are committed past silently.
3. Dedupe on the CloudEvents `id` (the `analytics_consumed_events.event_id`
   / `analytics_processed_events.event_id` columns keep their names and are
   now populated from `id`).
4. Read `time` / `subject` from the attributes, the payload via `DataAs`.

### Outbox

`outbox_events.value` stores the full structured-mode CloudEvent bytes and
`outbox_events.headers` the content-type header; `outbox_events.event_type`
now stores the full CloudEvents `type`. Because the id is minted at encode
time and lives inside the persisted bytes, the relay republishes the
identical event on redelivery.

## Consequences

- Wire-breaking: this service must deploy together with the rest of the
  fleet cutover. Before deploy, drain `outbox_events` (rows encoded before
  this change hold flat bytes), then delete and recreate
  `warehouse.network-fulfillment.events` and
  `warehouse.network-fulfillment.analytics` so the analytics projector's
  FirstOffset replay finds no flat message (if one remains, it is
  dead-lettered, not mis-parsed). See warehouse-infra
  `docs/cloudevents-cutover.md`.
- Golden exact-JSON tests pin every published type on both streams,
  including all attributes and the content-type header; consumer tests
  prove a legacy flat message is rejected/dead-lettered.
- `apis/asyncapi.yaml` documents both channels with
  `defaultContentType: application/cloudevents+json`, one required-attribute
  CloudEvent schema, and every message's exact `type` and `dataschema`.
