# ADR-0019: OTLP trace and metric export for the API process

Status: Accepted (2026-10-08)

## Context

network-fulfillment had no OpenTelemetry SDK. Its only metric,
`circuit_breaker_state{dependency}` (ADR 0004), lives on a private Prometheus
registry served at `GET /metrics`, and nothing in the cluster scrapes it. The
Helm chart already passed `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`,
`SERVICE_VERSION` and `ENVIRONMENT` (and says "traces and metrics are PUSHED
over OTLP/gRPC"), but no code read them, so the chart described a behaviour the
binary did not have.

The consequence was visible once the per-context Grafana dashboard was added
(warehouse-infra `scripts/gen-context-dashboards.py`): Prometheus held no
`service_name` series for this context, so every HTTP rate / error / latency
and Go-runtime panel was empty.

Every other HTTP context in the fleet exports OTLP through the OTel Collector
with the same small adapter (`internal/adapters/outbound/telemetry`, first
written in warehouse-planning, copied into product-master).

## Decision

1. Add `internal/adapters/outbound/telemetry/telemetry.go` (fleet convention)
   next to the existing circuit-breaker registry. `Setup` installs a
   `TracerProvider` and a `MeterProvider` exporting over OTLP/gRPC, the W3C
   trace-context propagator and Go runtime metrics. It never blocks on the
   Collector: with none reachable the service starts and serves as before and
   telemetry is dropped.
2. `cmd/netfulfil` calls `Setup` before anything is wired, with
   `service.name` taken from `OTEL_SERVICE_NAME` (the chart sets it to
   `network-fulfillment`) and falling back to `inboundhttp.DefaultServiceName`.
   Setup failure refuses to boot, like a bad database URL. It must run before
   `Routes()` is built.
3. `Routes()` wraps the `ServeMux` in `otelhttp`, inside the CORS middleware.
   This records `http.server.request.duration` labelled with the ServeMux
   pattern as `http.route`; a test pins that, because the dashboards depend on
   it.
4. The existing `circuit_breaker_state` registry and `GET /metrics` are left
   as they are. Whether to move that gauge onto the OTel pipeline (and drop
   the unscraped endpoint) is a separate decision.

## Out of scope

The `mcp`, `netfulfil-projector` and `netfulfil-reports` binaries are not
instrumented here. The dashboard's `(network-fulfillment|netfulfil).*` service
regex already covers them once they call the same `Setup`.

No business-metric counters are added: there is no agreed metric to export
yet, and the dashboard declares none.

## Consequences

- The request-rate / error-rate / p95 and Go-runtime panels of the
  network-fulfillment dashboard fill in for the API process once the new
  image is deployed.
- Adds the OTel SDK, OTLP gRPC exporters and gRPC to `go.mod`.
