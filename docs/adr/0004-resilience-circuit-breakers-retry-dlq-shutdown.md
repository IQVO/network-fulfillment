---
id: 0004-resilience-circuit-breakers-retry-dlq-shutdown
slug: /adr/0004-resilience-circuit-breakers-retry-dlq-shutdown
title: "4. Circuit breaker, Kafka DLQ, and graceful shutdown hardening"
sidebar_label: "4. Circuit breaker, DLQ, shutdown"
description: "ADR 0004 — Phase 2 resilience for network-fulfillment, ported from order-management's ADR 0025: a sony/gobreaker/v2 circuit breaker around the one real sync cross-context HTTP client (ordermanagement.Planner), a dead-letter topic for the analytics Kafka consumer, and a readiness-flip-first graceful shutdown sequence. The external network gateway (NETWORK_MODE=stub/sandbox/live) is deliberately deferred: it has no implemented sandbox/live client yet, so there is nothing real to wrap."
---

# 4. Circuit breaker, Kafka DLQ, and graceful shutdown hardening

## Status

Accepted — implemented in the same change that introduced this record.
This is network-fulfillment's Phase 2 (resilience) fan-out of the fleet
production-readiness plan, porting order-management's ADR 0025
verbatim wherever this repo's own shape matches it, and documenting the
two places it deliberately does not: no permissive/fail-open fallback
exists on `Planner` today, and the external network gateway has no
implemented sandbox/live client yet.

## Context

Before this change, network-fulfillment had exactly ONE real sync
cross-context HTTP client: `ordermanagement.Planner`
(`internal/adapters/outbound/ordermanagement/planner.go`), a plain
`*http.Client` with a fixed 10s timeout, calling order-management's
`RaiseHeldOrder`/`ReleaseHeldOrder`/`CancelHeldOrder` routes. Unlike
order-management's own outbound clients (which each ship a `permissive`
mode switch), `Planner` has no soft-fallback path at all: reading
`planner.go`, `client.go` and its test suite confirms every failure
(non-2xx status, transport error, JSON decode error) is wrapped and
returned to the caller unchanged — there is no existing "degrade
gracefully" behavior to preserve or reuse for a breaker's OPEN state.
There was also no circuit breaker, no bounded retry, and no
context-deadline propagation: a slow or hung order-management would
block every caller for the full fixed timeout, repeatedly, with nothing
to stop the pressure.

`internal/adapters/outbound/network/gateway.go` is this repo's OTHER
outbound HTTP-shaped dependency, selected by `NETWORK_MODE`
(`stub`/`sandbox`/`live`). Reading it confirms `stub` (the default)
talks to nobody, and `sandbox`/`live` both return a plain "not yet
implemented" error — there is no real client behind either mode today.
Wrapping a breaker around a gateway with no live implementation would
protect nothing; per the plan's own scoping and this repo's verified
state, this gateway is explicitly OUT OF SCOPE here (see
"Deliberately deferred" below) and is revisited once a real
sandbox/live client exists.

On the inbound side, `internal/adapters/inbound/kafka/` has exactly one
consumer, `AnalyticsConsumer` — it reads THIS SERVICE'S OWN analytics
topic and replays its own past-tense events into the analytics read
model (mirroring facility-layout's ADR-0010 / process-path-management's
ADR-0007 pattern). Before this change it had no dead-letter handling:
a message whose mark-processed or projection-apply phase always errors
(a malformed future-producer payload, a permanently broken
analyticsstore) would be retried on every redelivery indefinitely,
blocking every other event behind it on the same partition — the same
gap order-management's `RepromiseConsumer` had before ADR-0025, and
confirmed fleet-wide as a pre-Phase-2 gap with no DLQ anywhere.

`internal/adapters/inbound/poller/poller.go` polls the (stubbed)
network gateway for demand on a ticker and, per successful item,
invokes `ReceiveNetworkDemand`, which itself calls `Planner`. Reading
`poller.go` shows its OWN failure mode is already structurally
different from an HTTP client's: a failed `PollDemand` call is logged
and simply retried on the NEXT ticker interval (bounded backpressure by
construction — the ticker interval itself throttles retry rate, it is
not a blind tight retry loop), and its per-item failures inside one
pass do not abort the pass or advance the watermark past the failed
item. There is therefore no separate bounded-retry-with-backoff
mechanism to bolt onto the poller itself; what actually protects the
poller's own downstream pressure is the `Planner` breaker this ADR adds
underneath `ReceiveNetworkDemand`/`SweepAcknowledgementDeadlines` (see
"Deliberately deferred" below for the reasoning in full).

Finally, graceful shutdown in `cmd/netfulfil/main.go` already existed
(`signal.NotifyContext` + `httpServer.Shutdown`) but had no readiness
flip, no bounded wait for the poller/outbox-relay to actually finish
in-flight work, and no dedicated `GET /readyz` distinct from the
existing liveness-only `GET /healthz`.

## Decision

### 1. One circuit breaker around `Planner`, sharing one instance across all three methods

`internal/resilience` (a new, tiny, dependency-free top-level package,
ported verbatim from order-management's ADR-0025 package of the same
name — same `ReadyToTrip`, same `DefaultMaxRequests`/`DefaultInterval`/
`DefaultTimeout` tuning, same `CallTimeout` deadline-propagation helper,
same `RecordStateChange`/`StateRecorder` adapter) holds the shared
policy:

```go
const (
    DefaultMaxRequests = 1
    DefaultInterval    = 30 * time.Second
    DefaultTimeout     = 30 * time.Second
)

func ReadyToTrip(counts gobreaker.Counts) bool {
    if counts.ConsecutiveFailures >= 5 {
        return true
    }
    if counts.Requests < 10 {
        return false
    }
    return float64(counts.TotalFailures)/float64(counts.Requests) > 0.5
}
```

`ordermanagement.NewBreakerClient` wraps `Planner` (via a new
`FulfillmentPlanner` interface `Planner` itself already satisfies) with
ONE `*gobreaker.CircuitBreaker[...]` instance guarding
`RaiseHeldOrder`/`ReleaseHeldOrder`/`CancelHeldOrder` together — one
breaker per DOWNSTREAM DEPENDENCY, not per HTTP verb, exactly matching
the reference. `Planner` already owns its own dedicated `*http.Client`
(bulkhead — confirmed, not newly built, same as order-management's
Phase 2 finding).

### 2. No permissive fallback to reuse — the OPEN-state behavior is a distinctly-labelled error

This is the one place this port deliberately diverges from the
reference's mechanism (not its intent): order-management's breakers
route their OPEN-state calls to each client's PRE-EXISTING
permissive/fail-open fallback. `Planner` has no such fallback to route
to — every one of its own real failures already propagates as a plain
wrapped error. Inventing a NEW soft-fallback behavior here (e.g.
silently treating an unreachable order-management as
`Feasible=false`) would be exactly the kind of new semantics ADR-0025
itself warns against manufacturing at the breaker layer — see its
"Alternatives considered". Instead, `BreakerClient`'s OPEN-state path
returns `ErrCircuitOpen` (wrapping gobreaker's own rejection), which is
just as loud and just as easy for a caller to recognize as `Planner`'s
own existing errors — the caller-observable behavior (a failed
`RaiseHeldOrder`/`ReleaseHeldOrder`/`CancelHeldOrder`) is unchanged
either way; only the class of error differs from "the underlying HTTP
call failed" to "the breaker refused to even attempt it".

`errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err,
gobreaker.ErrTooManyRequests)` distinguishes gobreaker refusing to
ATTEMPT the call from a real error a call gobreaker DID let through —
only the former is wrapped as `ErrCircuitOpen`; a real error from an
attempted call propagates unchanged, exactly as `Planner` already
behaved before this breaker existed.

### 3. Context deadline propagation: `resilience.CallTimeout`, reused verbatim

Every `BreakerClient` method derives its outbound deadline from the
caller's own remaining `ctx.Deadline()`, capped at
`resilience.DefaultTimeout` (30s) when the caller's own budget is
larger or absent — replacing `Planner`'s previous fixed-10s-per-call
shape with one that can never outlast (or needlessly shorten) the
caller's own patience.

### 4. No retry added to `Planner`'s calls

All three of `Planner`'s methods are MUTATING (raise/release/cancel a
held order against order-management) — none are read-only/idempotent
GETs in the sense ADR-0025 §5 requires before adding blind retry (that
ADR retried `productclassification`'s read-only `GetClassification`
specifically, and explicitly declined to retry `inventorystorage`'s
mutating POST/DELETE for the same reason). Consistent with that
precedent, no `cenkalti/backoff/v4` retry wraps any `BreakerClient`
method — retrying a `RaiseHeldOrder` blindly risks a duplicate hold
order-management has no idempotency-key contract with this caller to
de-duplicate.

### 5. Circuit breaker state as a Prometheus gauge

Unlike order-management (which re-exports an OTel `Int64Gauge` through
its OTel Collector pipeline), network-fulfillment has no OTel/otelchi
wiring at all today — confirmed by grepping the module for
`go.opentelemetry.io` and finding no import anywhere. Rather than pull
in an OTel dependency for one gauge, `telemetry.CircuitBreakerMetrics`
(`internal/adapters/outbound/telemetry/circuit_breaker_metrics.go`)
goes straight to `github.com/prometheus/client_golang` against its
OWN dedicated `*prometheus.Registry` (not the global
`DefaultRegisterer`), registering the SAME `circuit_breaker_state`
gauge name and numbering convention the reference established
(`{dependency="order-management"}`, 0=closed/1=half-open/2=open,
gobreaker's own numbering verbatim, no translation table). This
registry is served at a new `GET /metrics` route
(`internal/adapters/inbound/http/server.go`), wired only when a
`*prometheus.Registry` is set on `Server.MetricsRegistry` — a `nil`
registry (every pre-existing test/caller) means `/metrics` is simply
not registered, not a route that panics.

### 6. Dead-letter queue for `AnalyticsConsumer`

`AnalyticsConsumer.handleMessage` now retries its two INDEPENDENT
phases — `Processed.MarkProcessed` and the projection-apply step —
SEPARATELY, each with jittered exponential backoff
(`cenkalti/backoff/v4`, 100ms-2s) up to `maxHandlerAttempts` (3) total
attempts per phase.

This is the one structural place this port diverges from
`RepromiseConsumer`'s exact retry shape, for a reason specific to this
consumer's own design: order-management's dedupe gate commits
ATOMICALLY with the state change it guards (both inside one
`RepromiseOrder.Execute` call), so retrying the WHOLE handler as one
unit is safe. `AnalyticsConsumer`'s `ProcessedEvents.MarkProcessed` is
a plain, SEPARATE call from the projection apply that follows it: if
`MarkProcessed` succeeds and the SUBSEQUENT apply then fails, retrying
the whole handler from the top would call `MarkProcessed` again, see
the event already marked processed, and skip `Apply` entirely —
silently losing that projection update forever. Retrying only the
phase that actually failed avoids that trap while preserving the
reference's exact retry BUDGET and backoff shape per phase.

Whichever phase exhausts its retries dead-letters the message: the raw,
byte-identical original payload plus `x-dlq-source-topic`/
`x-dlq-error`/`x-dlq-failed-at` headers are published to
`<source-topic>.dlq` (derived from the consumer's own topic, never a
fixed constant — an isolated test topic gets an isolated DLQ topic for
free, exactly like the reference), and the offset is committed anyway:
one poison message must never permanently block every other event
behind it on the same partition. This is logged at WARN level with
full context (topic, dlq_topic, phase, event id, event type, attempts,
error) — since ADR-0008 the id/type are the CloudEvents `id`/`type`
(`ce_id`/`ce_type` log keys), and a message that is not a valid
CloudEvent is dead-lettered immediately without retry.

Proven end to end with a REAL testcontainers Kafka
(`analytics_dlq_integration_test.go`,
`TestAnalyticsConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition`):
an event whose `MarkProcessed` is made to always fail (for one specific
`event_id` only) lands on `.dlq` after exactly 3 attempts with the raw
payload and error-context headers intact, and a well-formed event
published right after it on the SAME topic is processed without delay
— proving the partition was never blocked.

### 7. The poller gets NO new bounded-retry/backoff wrapper of its own — its structure already provides backpressure, and its real cross-context risk is covered by #1

`poller.Run`'s existing shape — one `PollDemand` attempt per ticker
tick, log-and-continue on failure, no in-pass abort — is already
bounded, ticker-throttled backpressure, not the "blind retry forever"
failure mode ADR-0025/this plan's Phase 2 intent targets (a tight loop
retrying as fast as it can, with no pacing at all). Wrapping it in a
`cenkalti/backoff/v4` policy on top of an already-paced ticker would
add complexity without addressing a real gap: the interval itself
already caps retry rate, and `PollDemand` today talks only to the
STUBBED network gateway (§ "Deliberately deferred"), which cannot
actually fail against a real dependency yet.

The poller's genuine cross-context risk is calling `Planner` (via
`ReceiveNetworkDemand`/`SweepAcknowledgementDeadlines`) for every item
it processes — and that risk is exactly what wrapping `Planner` itself
in the breaker (#1) already covers, transparently, without the poller
needing any breaker-awareness of its own. This mirrors the reference's
own principle that a breaker protects a DEPENDENCY, not a caller —
every caller of `Planner` (the poller's use cases, and any future
caller) gets the same protection for free.

### 8. Graceful shutdown hardening

`cmd/netfulfil/main.go`'s `signal.NotifyContext` + `httpServer.Shutdown`
sequence is extended, not rewritten, into the same order the reference
established:

1. **Flip readiness to not-ready FIRST**
   (`inboundhttp.Readiness.SetNotReady`, backing a new `GET /readyz`,
   distinct from the pre-existing `GET /healthz` which stays a pure
   liveness signal never flipped by shutdown).
2. **Stop accepting new HTTP connections and drain in-flight requests**
   — `httpServer.Shutdown(shutdownCtx)`, unchanged.
3. **Stop the outbox relay AND the poller cleanly** — cancel each
   one's own context and WAIT, bounded by the same `shutdownCtx`, for
   each goroutine to actually finish (`relayDone`/`pollerDone`
   channels), rather than firing the cancel and moving on.
4. **Close the pgx pool and event publisher LAST** — the existing
   `defer`red `closeOrders`/`closeEventPublisher` calls run after this
   point by `defer`'s LIFO order, once every consumer/relay/poller
   goroutine has already stopped touching the pool.

`Readiness`'s zero value (and a `nil *Readiness`) is always ready —
every existing test and caller behaves exactly as before this type was
introduced.

## Deliberately deferred

- **`internal/adapters/outbound/network/gateway.go`
  (`NETWORK_MODE=stub|sandbox|live`) gets NO breaker.** `stub` (the
  default) talks to nobody; `sandbox`/`live` both return a plain
  `errors.New("not yet implemented")` today — there is no real
  transport to protect. Wrapping a breaker around a method that always
  either no-ops or immediately errors "not implemented" would protect
  nothing and would need to be re-verified (and likely reshaped) once
  a genuine sandbox/live HTTP client exists. Revisit this gateway in
  the SAME PR that adds its first real client implementation, not
  before.
- **No dedicated retry/backoff wrapper on the poller itself** — see
  Decision §7. If a future change gives `PollDemand` a real external
  transport (once the network gateway above is implemented), THAT
  change should re-evaluate whether the poller needs its own
  `cenkalti/backoff/v4` policy independent of the ticker's own pacing,
  since a real external dependency's failure mode may no longer be
  adequately covered by ticker-interval pacing alone.

## Consequences

- Every `Planner` call now derives its timeout from the caller's
  remaining budget rather than a fixed fresh 10s — a caller with a
  short deadline gets a short-lived outbound call.
- A failing order-management deployment now trips ONE breaker after 5
  consecutive failures (or a sustained >50% error rate with enough
  volume) and stops sending real traffic to it for `DefaultTimeout`
  (30s) before probing again — bounded load instead of unbounded
  retry-forever pressure across every caller of `Planner`
  (`ReceiveNetworkDemand`, `SweepAcknowledgementDeadlines`, and any
  future caller).
- Callers of `Planner` now see a NEW error class, `ErrCircuitOpen`,
  distinct from `Planner`'s own transport/decode errors — any existing
  error-message-based assertions in calling code should match on
  `errors.Is`, not string content (already true throughout this
  codebase's existing test suite).
- `AnalyticsConsumer` can no longer be permanently wedged by one
  poison event; every other event on the partition keeps flowing. The
  `.dlq` topic is a new operational surface needing monitoring (out of
  scope here — the WARN-level log line is the interim signal) and a
  manual replay tool (also out of scope).
- `GET /readyz` is a new endpoint; a future Helm chart update pointing
  this service's `readinessProbe` at it (mirroring
  `charts/order-management/values.yaml`'s own ADR-0025-driven change)
  is a natural follow-up, not done in this change since
  `charts/network-fulfillment/values.yaml` has no `readinessProbe`
  override to update today (it inherits the harness template default).
- `sony/gobreaker/v2` (new direct dependency, same v2.4.0 order-
  management uses) and `github.com/prometheus/client_golang` (new
  direct dependency — this repo had no Prometheus/OTel telemetry
  package before this change) are added.
  `github.com/cenkalti/backoff/v4` was already an indirect dependency
  and is now promoted to direct.
  `github.com/testcontainers/testcontainers-go/modules/kafka` (same
  v0.44.0 the rest of this module's testcontainers stack uses) is a
  new direct test-only dependency, for the DLQ integration test.
- The external network gateway and the poller's own retry shape are
  explicitly untouched — see "Deliberately deferred" above.

## Alternatives considered

- **Inventing a soft fallback for `Planner`'s OPEN state** (e.g.
  treating an unreachable order-management as an automatic
  `Feasible=false`): rejected — there is no existing soft-fallback
  contract for this dependency to preserve, and manufacturing a new one
  at the breaker layer would silently change `Planner`'s
  caller-observable failure semantics in a way ADR-0025 itself warns
  against (a breaker should decide WHEN to fall back, never WHAT the
  fallback is, when no such behavior already exists to reuse).
- **Adding blind retry to `Planner`'s mutating calls**: rejected, same
  reasoning as ADR-0025 §5's `inventorystorage` DELETE/POST decision —
  no idempotency-key contract exists between network-fulfillment and
  order-management for these calls today.
- **Wrapping the stub network gateway in a breaker for consistency**:
  rejected — there is no real client behind `sandbox`/`live` yet, so
  a breaker there would protect nothing and would need reshaping the
  moment a real client is added; better to defer it to that change.
- **Giving the poller its own `cenkalti/backoff/v4` retry loop on top
  of its existing ticker**: rejected for now — the ticker interval
  already provides bounded, paced retry, and the poller's real
  downstream risk (`Planner`) is already covered by the breaker this
  ADR adds underneath it. Revisit once the poller's own transport
  (`PollDemand`) is a real implementation rather than a stub.

## References

- Reference PR: order-management PR #107 (commit `d65072f`), ADR-0025
  — the ADR this port copies verbatim wherever this repo's own shape
  matches it, and deviates from explicitly (and only) where documented
  above.
- `docs/adr/0001-network-fulfillment-bounded-context.md` — this
  service's own bounded-context ADR, establishing `ordermanagement`
  and `network` as this repo's two outbound adapter packages.
- `docs/adr/0003-transactional-outbox.md` — the outbox/relay this ADR's
  graceful shutdown sequence now also bounds the wait for, alongside
  the poller.
