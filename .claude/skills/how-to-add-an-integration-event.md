
# How to add an integration event (publish and consume)

> **In network-fulfillment:** this service does **not** use Kafka yet —
> no publisher, consumer, topic or `apis/asyncapi.yaml` exist (see
> `.claude/rules/integration-events.md`). The file paths cited below
> (`internal/adapters/outbound/kafka/publisher.go`,
> `internal/adapters/outbound/facilitycache/`, the Postgres outbox,
> `docs/` AsyncAPI regeneration) are from sibling repos and are examples
> of the pattern, not files in this tree. The consumer-group rules and the
> testcontainers rule apply as written, and the fitness tests that enforce
> them already exist in `internal/architecture/fitness_test.go`.

Use when asked to publish a new cross-context integration event, or
consume one from a sibling bounded context. This fleet's Kafka is ONE
broker platform-wide — every design decision below exists because that
shared-broker reality has already caused a real incident once.

## Publishing a new integration event

### 1. Is it actually cross-service?

Not every domain event this service raises belongs on the wire. Check
`internal/adapters/outbound/kafka/publisher.go`'s doc comment — this repo
forwards only `StockReserved`/`ReservationRevoked`; everything else is a
local concern published only to the Postgres outbox
(`internal/adapters/outbound/postgres/event_publisher.go`) for audit, not
broadcast. Before adding a new event to the Kafka publisher, confirm a
sibling context genuinely needs to react to it — check
`docs/docs/ddd/context-map.md` or the equivalent ubiquitous-language doc
for who's actually downstream.

### 2. Envelope: CloudEvents 1.0, structured mode

Every message is a CloudEvents 1.0 JSON document
(`application/cloudevents+json`). The context attributes carry routing:

```json
{
  "event_id": "<uuid>",
  "event_type": "com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>",
  "occurred_at": "<RFC3339>",
  "source": "<bounded-context-slug>",
  "data": { /* the actual payload, business types only */ }
}
```

The `type`/`event_type` follows the platform-wide reverse-DNS convention:
`com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`, all
lowercase except the final PascalCase event name — e.g.
`com.warehouse.wms.inventory-storage.reservation.ReservationRevoked`. Get
the subdomain (`wms`/`wes`/`wcs`/etc.) from this repo's own
`apis/asyncapi.yaml` intro section; don't guess it.

### 3. Implementation

Add the event struct to `internal/domain/<aggregate>/` (it should already
exist as a domain event the aggregate raises — publishing wires an
EXISTING domain event onto Kafka, it doesn't invent a new payload shape at
the adapter layer). In the Kafka publisher adapter:

- Add the event's marshal-to-envelope case
- Give the message a partition key that keeps ordering where it matters
  (usually the aggregate id)
- Use `Topic` — this service's own topic constant
  (`warehouse.<context>.events`), never a sibling's

### 4. Contract + docs

- Add the message to `apis/asyncapi.yaml` under this service's channel,
  matching the entity-grouping convention already there (group by
  aggregate, not chronologically)
- Regenerate the AsyncAPI HTML reference:
  ```bash
  cd docs && npm run gen-async-docs:all   # or gen-async-docs, check package.json
  ```
  This repo's `docs-api-drift` CI job fails the PR if the generated
  `static/asyncapi/<ctx>/` output doesn't match a fresh regen — a nullable
  field change here has bitten before.

### 5. Test

Unit test the marshal shape against a fake `Writer` (see
`publisher_test.go` — never a real broker in a unit test). If this event
now needs a `_integration_test.go` asserting real delivery, it MUST use
testcontainers (see the fitness test `TestKafkaIntegrationTestsUseTestcontainers`
in `internal/architecture/` — a skip-gated `KAFKA_BROKERS` test or a
hardcoded `localhost:9092` fails CI).

## Consuming an integration event from a sibling context

### 1. Never import the sibling's Go packages

This service knows a sibling's topic name and payload shape ONLY — never
its Go types. See `internal/adapters/outbound/facilitycache/consumer.go`'s
own doc comment: "This service has no business knowing anything else
about that context beyond this topic name and the envelope/payload shapes
below." Hand-mirror the payload struct locally; do not add a Go module
dependency on the sibling repo (an architecture fitness test in most
repos in this fleet would catch that anyway for the stricter contexts —
check this repo's own `internal/architecture/` for a
`TestNoSiblingContextOutboundCalls`-style guard before assuming it's
allowed).

### 2. Choose the right consumer-group pattern — this is the part that bites

Two DIFFERENT correct patterns exist. Picking the wrong one for your use
case is THE most common integration-event mistake in this fleet, and it
was learned from a real incident (wes-work-planning#67).

**Pattern A — long-lived, single-instance consumer group (a named
constant).** Use when exactly ONE instance of this consumer ever runs at
a time (e.g. this service's own analytics projector). The group id is a
plain named constant (`AnalyticsConsumerGroup`), reused across restarts —
that's correct because Kafka's committed-offset resume semantics are
EXACTLY what you want: pick up where the single instance left off.

**Pattern B — per-process-unique consumer group (a generated id).** Use
when this consumer rebuilds a complete read model from a topic's FULL
history on every start (an event-sourced local cache, not a work queue) —
see `facilitycache/consumer.go`'s `consumerGroupPrefix` +
`uniqueConsumerGroup()`. The group id MUST be unique per process instance
(hostname+PID+timestamp), NEVER a fixed shared string. Consumer group
offsets are shared infrastructure state: a brand-new process joining a
group an EARLIER instance already consumed resumes from that instance's
committed offset, so the new process gets marked "ready" with an empty
local cache having replayed nothing — a silent correctness bug, not a
crash.

**Never do this** (the actual incident): a fixed shared consumer group id
on a consumer meant to run as exactly one instance per environment. When
a local dev/test harness process joins the SAME broker's SAME group as a
live in-cluster Deployment, Kafka's rebalance protocol hands the
partition to only ONE of the two group members — the other silently
starves. Fix: make the group id env-configurable
(`KAFKA_CONSUMER_GROUP`/`<SERVICE>_CONSUMER_GROUP`), never hardcode it as
a literal string. This fleet's `internal/architecture/`
`TestKafkaConsumerGroupNeverHardcodedInline` fitness test (where present)
enforces this statically — an inline `GroupID: "literal"` fails CI.

### 3. Readiness gate, if this consumer backs a local cache

If the consumer replays a topic's full history to build a cache other
code depends on, expose a `Ready()` gate the health check consults, and
block readiness (not process startup — a transient Kafka outage
shouldn't be fatal) until the initial replay finishes. A readiness check
that only re-evaluates on a NEW message arriving deadlocks forever on an
ordinary restart where a shared/already-caught-up group never gets a new
message to trigger it — use `OffsetFetch` against the group's committed
offset, or (simpler and less bug-prone) just use the per-process-unique
group pattern above, which sidesteps the whole class of bug.

## Verify before opening the PR

```bash
make check-all    # includes arch-test — will catch a sibling-package import
```

Prove any new fitness-test-adjacent behavior actually matters by running
the specific scenario against a real broker if this repo has
testcontainers-based integration tests for the consumer/publisher touched.
