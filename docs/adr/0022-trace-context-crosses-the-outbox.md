---
id: 0022-trace-context-crosses-the-outbox
slug: /adr/0022-trace-context-crosses-the-outbox
title: 22. The W3C trace context crosses the outbox to Kafka
sidebar_label: 22. Trace context across the outbox
description: "ADR 0022 — the outbox publisher stores the writing span's W3C traceparent (and tracestate/baggage) in outbox_events.headers; the relay sends it as Kafka headers, so a consumer can continue the producer's trace."
---

# 22. The W3C trace context crosses the outbox to Kafka

## Status

> Status pointer: ADR 0025 removed the "direct mode" (no database) described in this record; the outbox is the only path.

**Accepted** (2026-10-13, ecosystem gap fix D27). Additive: no migration (the
existing `outbox_events.headers jsonb` column carries it), optional Kafka
headers only. Payloads and the CloudEvents envelope
([ADR 0008](./0008-cloudevents-mandatory-event-envelope.md)) are unchanged.

## Context

Every write of this context that raises events (the poller's
`ReceiveNetworkDemand`, the sweeps, reconciliation, the REST shipment
confirmation) leaves through the transactional outbox
([ADR 0003](./0003-transactional-outbox.md)) when `DATABASE_URL` is set and
`EVENT_PUBLISHER=kafka`: `postgres.OutboxPublisher.Publish` inserts the
encoded events into `outbox_events` in the use case's transaction, and the
relay later publishes them from a background goroutine with no span of its
own. The outbox stored only the CloudEvents `content-type` header, so the
trace stopped at the database: a consumer of
`warehouse.network-fulfillment.events` or of the analytics topic
(`netfulfil-projector`) could never link its work to the request or poll pass
that produced the event, although `telemetry.Setup`
([ADR 0019](./0019-otlp-telemetry-export.md)) already installs the W3C
propagator and otelchi opens a server span for every REST request
([ADR 0020](./0020-adr-0013-standard-metrics-convention-adoption.md)).

The fleet decision for this gap (D27) is trace continuity across the outbox,
using the storage the fleet's reference implementation (order-management)
already uses: the trace context lives in the outbox row's `headers jsonb`
column, injected with `otel.GetTextMapPropagator().Inject` at publish time,
and the relay sets the stored headers as Kafka message headers.

## Decision

1. **Capture at publish.** `postgres.OutboxPublisher.Publish` (it receives the
   use case's context, which carries the active span) injects the trace
   context with the global propagator into a map carrier and appends
   `traceparent`, and `tracestate` / `baggage` when present, to each row's
   headers, after the encoder's `content-type`, in a stable key order. A key
   the encoder already set is never overwritten. Without an active span the
   propagator injects nothing and the row keeps only `content-type`.
2. **No schema change.** `outbox_events.headers` already exists
   (`JSONB NOT NULL DEFAULT '[]'`, migration `0002_outbox`) and the relay
   already copies every stored header onto the Kafka message
   (`kafka.Publisher.Send`). Rows written before this change relay exactly as
   before.
3. **Both topics.** The integration and the analytics row of one event are
   inserted by the same `Publish` call with the same context, so both carry
   the same `traceparent`.
4. **Consumers.** This service's consumers (the analytics projector and the
   two capability-offer caches) open no spans, so they do not extract; when
   one starts a span it must call `Extract` on the message headers first.
   The contract (`apis/asyncapi.yaml`) documents the headers as optional:
   consumers may use them and must not require them.
5. **Direct mode.** Without a database (`EVENT_PUBLISHER=kafka`, no
   `DATABASE_URL`, local runs only) the publishers write directly and no
   trace header is added; that mode has no outbox to carry it.

## Consequences

- A consumer that extracts the headers parents its span on the producing
  span: one trace from the REST call (today `POST
  /network-orders/{networkRef}/shipment-confirmation`, whose otelchi server
  span is active when `NetworkOrderShipmentConfirmed` is enqueued) to the
  consumer's handling of the event.
- The poller and the background tickers (`ReceiveNetworkDemand`, the
  acknowledgement sweep, overdue rejection, reconciliation) open no span of
  their own (observability page, "Traces"), so their rows carry no
  `traceparent` yet. When one of them starts a span around its pass, its
  events inherit trace continuity with no further change here.
- Each outbox row grows by one ~55-byte header (more with `tracestate` or
  baggage).
- Baggage set by a caller is forwarded to Kafka consumers; this service sets
  none and the HTTP edge has no auth, so nothing sensitive is expected there.
- Tests (real Postgres and Kafka via testcontainers, in
  `internal/adapters/outbound/postgres/outbox_trace_integration_test.go`):
  `TestOutboxTrace_TraceparentCrossesTheOutboxToKafka` (every row written by
  `ReceiveNetworkDemand` in a span stores that span's `traceparent`, a relay
  pass on a background context sends it to both topics next to
  `content-type`, and it extracts to the producing trace and span) and
  `TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged` (a row without
  stored headers relays with its key and value unchanged and no headers).
  Unit tests in `outbox_trace_headers_test.go` pin the injection and the
  encoder-key-wins merge.
