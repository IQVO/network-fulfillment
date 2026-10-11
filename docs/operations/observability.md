---
id: observability
title: Observability
sidebar_label: Observability
---

# Observability

What each process emits, where it goes, and what to watch. Read from
`internal/adapters/outbound/telemetry/`, `internal/adapters/inbound/http/`,
the four `main.go` files and the OTel modules pinned in `go.mod`
(`otelhttp` v0.70.0, `contrib/instrumentation/runtime` v0.72.0, `otel`
v1.47.0). Dashboards are in `warehouse-infra`, see below.

## Signals per process

| Process | Traces | Metrics | Logs |
| --- | --- | --- | --- |
| `netfulfil` | OTLP/gRPC push (ADR 0019) | OTLP/gRPC push every 30 s, plus a Prometheus `GET /metrics` | JSON to stdout |
| `mcp` | none | none | JSON to stdout |
| `netfulfil-projector` | none | none | JSON to stdout, level from `LOG_LEVEL` |
| `netfulfil-reports` | none | none | JSON to stdout, one line per request, level from `LOG_LEVEL` |

The chart sets `OTEL_*` variables on all four pods, but only `netfulfil`
calls `telemetry.Setup`; ADR 0019 scopes the other three out.

## OpenTelemetry export (`netfulfil`)

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

`otelhttp.NewHandler` wraps the REST mux (`Server.Routes`), inside the CORS
middleware. Every request becomes one server span named
`<METHOD> <route pattern>`, for example `GET /network-orders/{networkRef}` or
`POST /network-orders/{networkRef}/shipment-confirmation` (otelhttp's
`SpanName`, using the `ServeMux` pattern as `http.route`). An incoming
`traceparent` header is honoured.

There are no other spans. The order-management and inventory-storage HTTP
clients use a plain `http.Client`, so outbound calls are not traced and no
trace context is sent to order-management. The poller, the tickers, the
outbox relay and the Kafka publishers open no spans and add no trace headers
to messages.

### Metric instruments

From `otelhttp` v0.70.0 (server side, one set per request):

| Instrument | Type | Unit | Attributes |
| --- | --- | --- | --- |
| `http.server.request.duration` | histogram | `s` | `http.request.method`, `url.scheme`, `server.address`, `server.port`, `network.protocol.name`, `network.protocol.version`, `http.response.status_code`, `http.route` |
| `http.server.request.body.size` | histogram | `By` | same |
| `http.server.response.body.size` | histogram | `By` | same |

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

No business metric is defined in this repo: there is no counter for orders
received, acknowledged or rejected. Those numbers come from the analytics
report and from `GET /inbound-status` (below).

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

Every process uses `log/slog` with a JSON handler on stdout. `netfulfil` and
`mcp` log at Info and above (no level knob); the projector and reports honour
`LOG_LEVEL`. There is no request log on the `netfulfil` REST server and no
trace id in log lines.

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
| `submitted orders reconciled` | INFO | `examined`, `confirmed`, `failed`, `pending` | netfulfil | |
| `capability offer recompute completed` | INFO | `examined`, `throughput_constrained` | netfulfil | |
| `capability offer: usable inventory lookup failed` | ERROR | `sku`, `err` | netfulfil | inventory-storage call failed for one SKU |
| `outbox relay pass failed` | ERROR | `error` | netfulfil | rows stay unpublished and are retried |
| `stub network acknowledgement` | INFO | `networkRef`, `accepted` | netfulfil | what the stub gateway was told |
| `analytics: message is not a valid CloudEvent, sending to dead-letter topic` | WARN | `topic`, `partition`, `offset`, `error` | projector | |
| `analytics: exhausted retries, sending to dead-letter topic` | WARN | `topic`, `dlq_topic`, `phase`, `ce_id`, `ce_type`, `attempts`, `error` | projector | |
| `analytics consumer stopped` | ERROR | `error` | projector | the consumer loop exited; the process keeps serving `/healthz` |
| `http request` | INFO, ERROR for 5xx | `method`, `path`, `status`, `duration_ms`, `bytes`, `request_id` | reports | one line per request |

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

Only the API pod exports OTel metrics, so the RED and runtime panels show
`netfulfil` alone.

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
