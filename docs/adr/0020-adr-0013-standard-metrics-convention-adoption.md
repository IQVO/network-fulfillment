---
id: 0020-adr-0013-standard-metrics-convention-adoption
title: Adopt ADR-0013 (fleet "Standard metrics convention"): otelchi HTTP RED metrics everywhere
status: Accepted (2026-10-12)
---

# ADR-0020: Adopt ADR-0013 (fleet "Standard metrics convention"): otelchi HTTP RED metrics everywhere

## Status

Accepted (2026-10-12)

## Context

wes-work-planning's ADR-0013 ("Standard metrics convention across the
fleet") defines a Tier 1 baseline that is mandatory and identical across
every service: `telemetry.Setup` installs the MeterProvider and starts
Go-runtime metrics, every HTTP-serving process wires `otelchi.Middleware`
plus `otelchimetric.NewServerRequestDuration` (ahead of the request
logger, in a fixed order), and `GET /healthz` returns
`{"status":"ok"}`. ADR-0013's own text is explicit that "This is the ONLY
sanctioned way to get request rate/error/duration — no service hand-rolls
its own HTTP counter."

A fleet-wide audit for ADR-0013 found this repo not instrumented with
`otelchi` anywhere. Looking closer:

- `cmd/netfulfil` (the OLTP API) called `telemetry.Setup` (ADR 0019) and
  wrapped its REST mux in `otelhttp.NewHandler`, which does emit
  `http.server.request.duration` — but via `otelhttp`, not `otelchi`, and
  on a stdlib `http.ServeMux` rather than `chi.Router`. ADR-0019 made that
  choice deliberately, before ADR-0013 existed.
- `cmd/netfulfil-reports` and `cmd/mcp` both already used `chi.Router` for
  their own reasons (route params, `chi.Mount`) but wired no middleware at
  all — no tracing, no metrics, not even `telemetry.Setup`. ADR-0019
  explicitly scoped these two (and the projector) OUT: "The mcp,
  netfulfil-projector and netfulfil-reports binaries are not instrumented
  here."
- `cmd/netfulfil-projector` is an internal admin port that serves only
  `GET /healthz` for a Kafka consumer process; it called no `telemetry.Setup`
  either.

`docs/operations/observability.md` and the per-context Grafana dashboard
(`warehouse-infra` `scripts/gen-context-dashboards.py`) both record the
consequence: only the `netfulfil` pod exports anything, so the RED and
Go-runtime panels show that one process alone.

## Decision

Bring every HTTP-serving process in this repo onto ADR-0013's Tier 1
baseline, verbatim, copying the exact `otelchi`/`otelchimetric` usage
`inventory-storage` and `wes-work-planning` already follow:

```go
r.Use(middleware.RequestID)
r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
metricCfg := otelchimetric.NewBaseConfig(serviceName)
r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
r.Use(otelchimetric.NewServerActiveRequests(metricCfg))
r.Use(RequestLogger(logger))
r.Use(middleware.Recoverer)
```

1. **`cmd/netfulfil` (`internal/adapters/inbound/http/server.go`).**
   `Server.Routes()` now builds a `chi.Router` instead of a stdlib
   `http.ServeMux`, with the middleware chain above (CORS is layered on
   top as an additional `r.Use`). `otelhttp` is removed — otelchi now owns
   both tracing and the duration/active-requests metrics, so running both
   would double-instrument every request. Path params move from
   `r.PathValue` to `chi.URLParam`. `Server` gains `Logger` and
   `ServiceName` fields (nil/empty falls back to `slog.Default()` and
   `DefaultServiceName`, matching every pre-existing caller). This
   **supersedes ADR-0019 §3's otelhttp choice**; ADR-0019's other
   decisions (telemetry.Setup, the OTLP pipeline itself, the Go-runtime
   metrics) stand unchanged.
2. **`cmd/netfulfil-reports`.** Gains `telemetry.Setup` (service name
   `network-fulfillment-reports` by default) and `NewReportsRouter` gains
   a `serviceName` parameter wiring the same otelchi/otelchimetric chain
   ahead of its existing `RequestLogger`/`Recoverer`.
3. **`cmd/mcp`.** Gains `telemetry.Setup` (service name
   `network-fulfillment-mcp` by default) and its `newRouter` wires the
   same chain (minus a request logger, which this router never had)
   ahead of `middleware.Recoverer`.
4. **`cmd/netfulfil-projector`.** Gains `telemetry.Setup` (service name
   `network-fulfillment-projector` by default) for Go-runtime metrics
   only. Its admin HTTP surface stays a plain stdlib `http.ServeMux` with
   a single `/healthz` route — the same shape
   `inventory-storage`'s `inventory-projector` and
   `wes-work-planning`'s `wes-projector` use for their own admin ports.
   otelchi RED metrics measure a real request surface's rate/error/
   duration; a liveness probe on an internal admin port is not that
   surface, so it is intentionally left off `chi`/`otelchi`.

This explicitly **closes the gap ADR-0019 left open** ("The mcp,
netfulfil-projector and netfulfil-reports binaries are not instrumented
here... once they call the same Setup").

## Consequences

- All four processes now export Go-runtime metrics; `netfulfil`,
  `netfulfil-reports` and `mcp` additionally export
  `http.server.request.duration` and `http.server.active_requests` with
  `http.route` from the chi-resolved pattern, via the identical
  `otelchi`/`otelchimetric` mechanism the rest of the fleet uses — so the
  Grafana per-context dashboard's RED panels, filtered on
  `service_name=~"(network-fulfillment|netfulfil).*"`, now have data from
  every HTTP-serving pod, not just the API one.
- No business metric is added or renamed (Tier 2 is explicitly out of
  scope here); no existing route, status code, or response body changes.
- `docs/operations/observability.md` and `.claude/rules/rest-api.md` are
  updated to describe the new instrumentation.
- Exporters are non-blocking exactly as ADR-0019 already established: a
  missing Collector degrades every one of the four processes to
  "telemetry dropped," never to "process won't start."
