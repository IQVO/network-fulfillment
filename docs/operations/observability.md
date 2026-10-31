---
id: observability
title: Observability
sidebar_label: Observability
---

# Observability

What each process emits, where it goes, and what to watch. Read from
`internal/adapters/outbound/telemetry/`, `internal/adapters/inbound/http/`,
the four `main.go` files and the OTel modules pinned in `go.mod`
(`otelchi` v0.12.3, `contrib/instrumentation/runtime` v0.72.0, `otel`
v1.47.0). Dashboards are in `warehouse-infra`, see below.

## Signals per process

| Process | Traces | Metrics | Logs |
| --- | --- | --- | --- |
| `netfulfil` | OTLP/gRPC push (ADR 0019, 0020) | OTLP/gRPC push every 30 s, plus a Prometheus `GET /metrics` | JSON to stdout, one line per request, level from `LOG_LEVEL` |
| `mcp` | OTLP/gRPC push (ADR 0020) | OTLP/gRPC push every 30 s | JSON to stdout, level from `LOG_LEVEL` |
| `netfulfil-projector` | none | OTLP/gRPC push every 30 s (Go-runtime only, ADR 0020) | JSON to stdout, level from `LOG_LEVEL` |
| `netfulfil-reports` | OTLP/gRPC push (ADR 0020) | OTLP/gRPC push every 30 s | JSON to stdout, one line per request, level from `LOG_LEVEL` |

All four processes now call `telemetry.Setup` (ADR 0019 scoped this to
`netfulfil` only; ADR 0020 closes that gap for the other three).
`netfulfil-projector`'s admin HTTP surface stays unwired to otelchi — it
serves nothing besides `GET /healthz` — so it reports Go-runtime metrics
but no HTTP RED metrics; see ADR 0020 for why.

## OpenTelemetry export (all four processes)

`telemetry.Setup` (`internal/adapters/outbound/telemetry/telemetry.go`) runs
first in `main`, before the HTTP handler is built:

- One `TracerProvider` (batching) and one `MeterProvider` (periodic reader,
  30 s), both exporting OTLP/gRPC, **insecure**, to
  `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`; chart default
  `otel-collector.observability.svc.cluster.local:4317`).
- The exporters dial lazily. A missing Collector drops telemetry; it never
  delays or fails boot.
- Propagators: W3C `traceparent` plus baggage.
- Resource: the SDK defaults, then `service.name` (`OTEL_SERVICE_NAME`,
  default `network-fulfillment`), `service.version` (`SERVICE_VERSION`,
  default `dev`) and `deployment.environment.name` (`ENVIRONMENT`, default
  `local`).
- Shutdown flushes both providers with a 5 s budget.

### Traces

`otelchi.Middleware` wraps the chi router (`Server.Routes` in `netfulfil`;
`NewReportsRouter` in `netfulfil-reports`; `newRouter` in `mcp`), with
`otelchi.WithChiRoutes` so the span/metric route label is the chi pattern
up front rather than once per concrete path. Every request becomes one
server span named `<METHOD> <route pattern>`, for example
`GET /network-orders/{networkRef}` or
`POST /network-orders/{networkRef}/shipment-confirmation`. An incoming
`traceparent` header is honoured.

There are no other spans. The order-management and inventory-storage HTTP
clients use a plain `http.Client`, so outbound calls are not traced and no
trace context is sent to order-management. The poller, the tickers, the
outbox relay and the Kafka publishers open no spans.

**Trace context across the outbox** ([ADR 0022](../adr/0022-trace-context-crosses-the-outbox.md)).
In outbox mode `OutboxPublisher.Publish` injects the active span's W3C
`traceparent` (and `tracestate` / `baggage` when present) into each
`outbox_events.headers` row, after the CloudEvents `content-type`; the relay
sends the stored headers with the Kafka message, so a consumer that extracts
them continues the producer's trace. Today that covers events raised inside a
REST request span (`NetworkOrderShipmentConfirmed` from the
shipment-confirmation endpoint). Events raised by the poller and the tickers
carry no `traceparent`, because those passes open no span. Direct mode (no
`DATABASE_URL`) adds no trace header. This service's own consumers open no
spans, so they do not extract.

### Metric instruments

From `otelchi/metric` v0.12.3 (ADR-0013 Tier 1 item 2; server side, one set
per request, on `netfulfil`, `netfulfil-reports` and `mcp`):

| Instrument | Type | Unit | Attributes |
| --- | --- | --- | --- |
| `http.server.request.duration` | histogram | `s` | `http.request.method`, `http.route`, `http.response.status_code`, and other OTel HTTP semantic-convention attributes |
| `http.server.active_requests` | up-down counter | `{request}` | `http.request.method` |

From `contrib/instrumentation/runtime` v0.72.0 (`runtime.Start`; all are
asynchronous (observable) instruments, read on each 30 s collection):

| Instrument | Type | Unit | Notes |
| --- | --- | --- | --- |
| `go.memory.used` | up-down counter | `By` | attribute `go.memory.type` = `stack` or `other` |
| `go.memory.limit` | up-down counter | `By` | only when a `GOMEMLIMIT` is set |
| `go.memory.allocated` | counter | `By` | |
| `go.memory.allocations` | counter | `{allocation}` | |
| `go.memory.gc.goal` | up-down counter | `By` | |
| `go.goroutine.count` | up-down counter | `{goroutine}` | |
| `go.processor.limit` | up-down counter | `{thread}` | |
| `go.config.gogc` | up-down counter | `%` | |

No business metric is defined for orders received, acknowledged or rejected:
those numbers come from the analytics report and from `GET /inbound-status`
(below). The one business counter is the orphaned-hold sweep's.

### Node profile metrics

`internal/adapters/outbound/telemetry/node_profile_metrics.go` (ADR 0027 §8, same
convention and transport as the sweep metric below: the global `MeterProvider`,
OTLP, not on `GET /metrics`). Live mode only; a stub deployment emits nothing.

| Instrument | Type | Unit | Attributes |
| --- | --- | --- | --- |
| `network_fulfillment.node_profile.declarations` | counter | `{declaration}` | `outcome` = `declared`, `confirmed`, `pending`, `unchanged`, `failed`, `skipped_no_node`, `skipped_no_schedule` or `skipped_incomplete` |
| `network_fulfillment.node_profile.revision` | gauge | `{revision}` | `site`; the revision last declared or settled, only recorded when an attempt concerned one |

One data point per site per pass (`DeclareNodeProfiles` through
`ports.ProfileMetrics`). A healthy steady state is `unchanged` on every pass; a
stuck `skipped_*` or `failed` is the signal (see
[the node profile](./runbook.md#the-node-profile)).

### Orphaned-hold sweep metric

`internal/adapters/outbound/telemetry/orphan_hold_metrics.go` (ADR 0024 §7,
following the fleet's Tier 2 business-counter convention that ADR 0020 adopts),
built on the global `MeterProvider` that `telemetry.Setup` installs and pushed
over OTLP like the instruments above. It is **not** on `GET /metrics`, which
serves only the breaker gauge.

| Instrument | Type | Unit | Attributes |
| --- | --- | --- | --- |
| `network_fulfillment.orphaned_holds.swept` | counter | `{hold}` | `outcome` = `cancelled`, `already_released` or `failed` |

One data point per hold per pass, added by `SweepOrphanedHolds` through
`ports.OrphanHoldMetrics`: `cancelled` = order-management cancelled the hold or
answered `404`; `already_released` = it answered `409` or `422`; `failed` = the
cancel or the save failed and the hold stays open for the next pass, so a hold
that keeps failing adds one `failed` per pass. A healthy fleet emits nothing at
all, so there is no series until the first hold is swept. Before `Setup` runs
(or in a test) the instrument is OTel's no-op. Behind the Collector the
Prometheus name has not been checked here; the usual OTel rules would give
`network_fulfillment_orphaned_holds_swept_total` with an `outcome` label.

In Prometheus, behind the fleet Collector, these names become
`http_server_request_duration_seconds_*`, `go_goroutine_count`,
`go_memory_used_bytes` and so on, with `service_name` as a label.

## Prometheus `GET /metrics` (`netfulfil`)

`internal/adapters/outbound/telemetry/circuit_breaker_metrics.go` registers
one gauge on its own registry (no Go or process collectors):

| Metric | Type | Labels | Values |
| --- | --- | --- | --- |
| `circuit_breaker_state` | gauge | `dependency="order-management"` | `0` closed, `1` half-open, `2` open |

The series appears after the breaker's first state change; before that the
endpoint returns no sample for it. ADR 0019 records that nothing in the
cluster scrapes this endpoint, so read it by hand:

```bash
kubectl -n <apps namespace> port-forward deploy/network-fulfillment 8080:8080
curl -s localhost:8080/metrics
```

## Inbound status

`GET /inbound-status` (`internal/adapters/inbound/http/dto.go`,
`inboundStatusResponse`) is the operator's view of the inbound leg. The
poller's failure mode is silence, so this is the endpoint to alert on.

| Field | Meaning |
| --- | --- |
| `networkMode` | the gateway mode actually in force (`stub` or `live`) |
| `polls` | poll passes started since boot |
| `received` | demand units the use case handled without error (a rejection counts as handled) |
| `failed` | demand units whose `ReceiveNetworkDemand` returned an error; a failed `PollDemand` call does not count here, it is only logged |
| `since` | the watermark the next poll uses; absent until one pass has fully succeeded |
| `unanswered` | orders in `NEW` |
| `overdue` | `NEW` orders past their 24 h `acknowledgeBy` |

The counters live in process memory and reset on restart; each replica has
its own.

## Logs

Every process uses `log/slog` with a JSON handler on stdout, at Info and
above by default. All four honour `LOG_LEVEL` (`debug`, `info`,
`warn`/`warning`, `error`; anything else is Info). `netfulfil` now logs one `http request` line per request
(`RequestLogger`, same shape `netfulfil-reports` already used), stamped
with the chi request id from `middleware.RequestID`; there is still no
OTel trace id in log lines (the slog handler is plain, not
trace-correlated).

Messages worth knowing, with their fields (all from the code):

| Message | Level | Fields | Process | Meaning |
| --- | --- | --- | --- | --- |
| `network gateway wired` | INFO | `mode`, `baseURL` | netfulfil | mode in force at boot |
| `no PRODUCT_TRANSLATION_FILE set; every network order will be rejected as untranslatable` | WARN | | netfulfil | empty ACL dictionary |
| `product translation loaded` / `stub demand seeded` | INFO | `file`, `products` / `demands` | netfulfil | curated files loaded |
| `order repository wired` | INFO | `backend` (`memory` or `postgres`) | netfulfil, mcp | persistence mode |
| `retrying` / `succeeded after retry` | WARN / INFO | `op`, `attempt`, `in`, `err` | netfulfil | boot retry |
| `event publisher configured` | INFO | `publisher`, `mode` (`direct`/`outbox`), `integration_topic`, `analytics_topic`, `brokers` | netfulfil | Kafka wiring |
| `poll demand failed` | ERROR | `err`, `since` | netfulfil | the gateway call failed; retried next tick |
| `receive network demand failed` | ERROR | `networkRef`, `err` | netfulfil | one unit failed; the watermark will not advance |
| `poll pass had failures; watermark not advanced` | WARN | `failures`, `of`, `since` | netfulfil | |
| `acknowledgement deadlines at risk` | WARN | `examined`, `atRisk` | netfulfil | sweep found overdue `NEW` orders |
| `overdue orders rejected` | WARN | `examined`, `rejected` | netfulfil | |
| `orphaned holds swept` | WARN | `examined`, `cancelled`, `alreadyReleased`, `failed` | netfulfil | the orphaned-hold sweep acted on at least one hold; not logged on a pass that found nothing to do ([runbook](./runbook.md#a-held-order-is-stuck-in-order-management)) |
| `orphaned-hold sweep failed` | ERROR | `err` | netfulfil | the sweep could not list the open holds; retried next tick |
| `orphaned-hold sweep disabled by ORPHAN_HOLD_SWEEP_ENABLED: a hold nobody decides stays reserved in order-management` | WARN | | netfulfil | once at boot, when the sweep is switched off |
| `invalid ORPHAN_HOLD_SWEEP_ENABLED, keeping the orphaned-hold sweep on` / `invalid ORPHAN_HOLD_GRACE, using the default` | WARN | `value`, `error` (and `default` for the grace) | netfulfil | at boot, for an unparseable or (grace) negative value |
| `submitted orders reconciled` | INFO | `examined`, `confirmed`, `failed`, `pending` | netfulfil | |
| `capability offer recompute completed` | INFO | `examined`, `throughput_constrained` | netfulfil | |
| `capability offer: usable inventory lookup failed` | ERROR | `sku`, `err` | netfulfil | inventory-storage call failed for one SKU |
| `outbox relay pass failed` | ERROR | `error` | netfulfil | a database failure of a relay pass (begin, claim, mark published); rows stay unpublished and are retried. Send failures are logged by the two lines below instead |
| `outbox relay: send failed, stopping this pass at the failed row` | WARN | `outbox_id`, `topic`, `event_type`, `attempts`, `error` | netfulfil | the broker rejected a row below the attempt cap; the next pass retries it |
| `outbox relay: row reached the attempt cap and is excluded from future claims; re-arm it with attempts = 0 after fixing the cause` | ERROR | `outbox_id`, `topic`, `event_type`, `key`, `attempts`, `max_attempts`, `error` | netfulfil | once per row, when it reaches `OUTBOX_MAX_ATTEMPTS` ([ADR 0021](../adr/0021-outbox-relay-attempt-cap.md)); consumers never got that event ([runbook](./runbook.md#an-outbox-row-hit-the-attempt-cap)) |
| `outbox relay running` | INFO | `topic`, `analytics_topic`, `max_attempts` | netfulfil | outbox mode is on, with the attempt cap in force |
| `stub network acknowledgement` | INFO | `networkRef`, `accepted` | netfulfil | what the stub gateway was told |
| `analytics: message is not a valid CloudEvent, sending to dead-letter topic` | WARN | `topic`, `partition`, `offset`, `error` | projector | |
| `analytics: exhausted retries, sending to dead-letter topic` | WARN | `topic`, `dlq_topic`, `phase`, `ce_id`, `ce_type`, `attempts`, `error` | projector | |
| `analytics consumer stopped` | ERROR | `error` | projector | the consumer loop exited; the process keeps serving `/healthz` |
| `http request` | INFO, ERROR for 5xx | `method`, `path`, `status`, `duration_ms`, `bytes`, `request_id` | netfulfil, reports | one line per request |

## Dashboards

`warehouse-infra` provisions one Grafana dashboard for this context,
`terraform/dashboards/contexts/network-fulfillment.json` (uid
`warehouse-network-fulfillment`), generated by
`scripts/gen-context-dashboards.py`. As read on `warehouse-infra` `develop`
it has:

- Kong edge panels on `kong_http_requests_total`, `kong_request_latency_ms_bucket`
  and `kong_upstream_latency_ms_bucket` for the
  `httproute.warehouse-systems.network-fulfillment.*` services: rate by code,
  5xx ratio, p95 latency.
- RED panels on `http_server_request_duration_seconds_*` filtered to
  `service_name=~"(network-fulfillment|netfulfil).*"`: rate by route, 5xx
  rate, p95.
- `go_goroutine_count` and `go_memory_used_bytes`.
- Loki panels on `{app="network-fulfillment"}`: the log stream, volume by
  level, and ERROR/WARN lines.

Only `netfulfil` exports a Prometheus `GET /metrics` endpoint, so that
panel group stays `netfulfil`-only. The RED panels on
`http_server_request_duration_seconds_*`, filtered to
`service_name=~"(network-fulfillment|netfulfil).*"`, now have data from
`netfulfil`, `netfulfil-reports` and `mcp` (ADR 0020); the projector
contributes only the Go-runtime panels.

## Suggested alerts

None of these exist yet; they follow from the code above.

| Alert | Expression idea | Why |
| --- | --- | --- |
| Overdue acknowledgements | `/inbound-status` `overdue > 0`, or WARN `acknowledgement deadlines at risk` in Loki | the 24 h SLA is being missed |
| Poller stalled | `polls` not growing for 3 x `POLL_INTERVAL`, or `since` absent 5 min after start | the poller's failure mode is silence |
| Demand failing | `failed` growing, or ERROR `receive network demand failed` | orders stay `NEW` until rejected as overdue |
| Breaker open | `circuit_breaker_state{dependency="order-management"} == 2` (needs a scrape job first) | every hold, release and cancel is refused |
| Everything rejected | report `ordersRejectedUntranslatableSku` close to `ordersReceived` | empty or wrong product dictionary |
| Analytics stale | `GET /reports/acknowledgement/freshness` `lagSeconds` high while orders are arriving | projector stopped; note the lag also grows when no event is published at all |
| 5xx on the API | `http_server_request_duration_seconds_count{http_response_status_code=~"5.."}` | |
| DLQ growth | message count on `warehouse.network-fulfillment.analytics.dlq` | poison or failing projection |
| Outbox row excluded | any ERROR `outbox relay: row reached the attempt cap ...` (log alert), or `SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND attempts >= 20` > 0 | an event consumers never received; re-arm it after fixing the cause (ADR 0021) |
| Outbox backlog | `SELECT count(*), min(created_at) FROM outbox_events WHERE published_at IS NULL` growing for > 5 min | the relay is stuck on a failing row below the cap, or the broker is down |
| Orphaned holds failing | `network_fulfillment.orphaned_holds.swept{outcome="failed"}` increasing, or WARN `orphaned holds swept` with `failed > 0` on consecutive passes, or a row of the runbook's open-holds query older than `ORPHAN_HOLD_GRACE + SWEEP_INTERVAL` | a hold nobody decided cannot be freed (order-management down, breaker open, an unexpected status); stock stays reserved ([runbook](./runbook.md#a-held-order-is-stuck-in-order-management)) |
| A hold had leaked | any `OrphanedHoldCancelled` event, or `network_fulfillment.orphaned_holds.swept{outcome="cancelled"}` increasing | the normal cancel path failed first; worth finding out why |
| Sweep switched off | WARN `orphaned-hold sweep disabled by ORPHAN_HOLD_SWEEP_ENABLED` at boot | nothing frees a hold that `RejectOverdueOrders` missed |
| Node profile not declared (live) | `network_fulfillment.node_profile.declarations{outcome=~"skipped_.*\|failed"}` on every pass, or ERROR `the network failed a node profile declaration`, or WARN `node profile declaration disabled by NETWORK_PROFILE_INTERVAL=0` at boot | the network keeps an old (or no) handling time and cutoffs, and judges our acknowledgement deadlines by them ([runbook](./runbook.md#the-node-profile)) |
| Inventory not fed (live) | ERROR `the network failed an inventory feed`, `refused an inventory feed` or `refused a listing as not on the node`, WARN `inventory feed disabled by NETWORK_INVENTORY_INTERVAL=0` at boot, or no `inventory fed` line while `capability_offers` change (the feed has no metric: logs and the `INVENTORY` rows of `network_submissions` are the signal) | the network keeps selling against the stock it was last told ([runbook](./runbook.md#the-inventory-feed)) |
