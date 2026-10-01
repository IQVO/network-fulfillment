
# How to add an integration event (publish and consume)

> **In network-fulfillment:** the Kafka publishers live in
> `internal/adapters/outbound/kafka/` (`publisher.go` → integration topic
> `warehouse.network-fulfillment.events`, `analytics_publisher.go` →
> `warehouse.network-fulfillment.analytics`), feeding the Postgres outbox
> (`internal/adapters/outbound/postgres/outbox_*.go`, ADR 0003) when
> `DATABASE_URL` is set. The only consumer is this service's own analytics
> projector (`internal/adapters/inbound/kafka/analytics_consumer.go`).
> `facilitycache/` paths cited in the consumer section are from sibling
> repos — examples of the pattern, not files in this tree.

Use when asked to publish a new cross-context integration event, or
consume one from a sibling bounded context. This fleet's Kafka is ONE
broker platform-wide — every design decision below exists because that
shared-broker reality has already caused a real incident once.

## Publishing a new integration event

### 1. Is it actually cross-service?

Every domain event the `NetworkOrder` aggregate raises is published on
both topics today (`aggregateKey` in `publisher.go` lists them). Before
adding a new one, confirm a sibling context (or the analytics projector)
genuinely needs to react to it, and that nothing network-shaped (PO
numbers, ASINs, network status codes, PII) leaks into its payload.

### 2. Envelope: CloudEvents 1.0, structured mode — MANDATORY (ADR 0008)

Every message is a CloudEvents 1.0 JSON event
(`application/cloudevents+json`), with the Kafka header
`content-type: application/cloudevents+json; charset=UTF-8`. There is no
other envelope — no `event_id`/`event_type`/`occurred_at`, no
`schema_version`, no dual mode.

```json
{
  "specversion": "1.0",
  "id": "<uuid, minted once at encode time>",
  "source": "/warehouse/network-fulfillment",
  "type": "com.warehouse.wes.network-fulfillment.networkorder.<EventName>",
  "subject": "<networkRef — the aggregate id, never empty>",
  "time": "<occurred-at, UTC RFC 3339>",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:network-fulfillment:<events|analytics>:<EventName>:v1",
  "data": { /* the domain event's own JSON, business types only */ }
}
```

`type` follows `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`,
all lowercase except the PascalCase event name. Here: subdomain `wes`,
context `network-fulfillment`, entity = the raising aggregate
(`networkorder`). Same `type` on both topics; `dataschema` names the
stream/shape. A breaking payload change => new `.v2` type + new dataschema
version, published as a new event.

### 3. Implementation

Add the event struct to `internal/domain/shared/events.go` implementing
`shared.DomainEvent` (`EventName()`, `OccurredAt()`) — publishing wires an
EXISTING domain event, it never invents a payload at the adapter layer.
Then in `internal/adapters/outbound/kafka/publisher.go`:

- Add a case to `aggregateKey` returning the aggregate id (it becomes BOTH
  the Kafka key and the CloudEvents `subject`).
- If the new event comes from a different aggregate, give it its own
  entity segment (do not reuse `Entity`).
- Do nothing else: `encodeAll` builds the CloudEvent through
  `internal/adapters/kafka/cloudevents.New` for both publishers and stamps
  the content-type header. Never marshal an envelope by hand.

### 4. Contract + docs


- Add the message to `apis/asyncapi.yaml` on BOTH channels (integration
  and `...Analytics`), with its exact `type` const, its `dataschema`
  const, and a `<EventName>Data` payload schema composed onto the shared
  `CloudEvent` schema. This repo has no Docusaurus site, so there is no
  HTML regeneration step.

### 5. Test

Add the event to `goldenCases` in `publisher_test.go`: that gives it an
exact-JSON golden test on both streams (every attribute, the full `type`,
`dataschema`, the key and the content-type header) against a fake
`Writer` — never a real broker in a unit test. If this event
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

### 2. Decode CloudEvents only, dispatch on the full type

Decode with `internal/adapters/kafka/cloudevents.Decode`, switch on the
FULL `type` string from the fleet catalogue (ADR 0008 lists the exact
cross-service strings — never a short name or suffix match), ignore
unknown types, dedupe on the CloudEvents `id`, read `time`/`subject` from
attributes and the payload with `DataAs`. A message that fails CloudEvents
validation goes to the consumer's DLQ (or WARN + commit past if it has
none) — never retried, never parsed as a legacy flat shape. Add a test
proving a legacy flat message is rejected.

### 3. Choose the right consumer-group pattern — this is the part that bites

Two DIFFERENT correct patterns exist. Picking the wrong one for your use
case is THE most common integration-event mistake in this fleet, and it
was learned from a real incident (wes-work-planning#67).

**Pattern A — long-lived, single-instance consumer group (a named
constant).** Use when exactly ONE instance of this consumer ever runs at
a time. The group id is a plain named constant, reused across restarts —
that's correct because Kafka's committed-offset resume semantics are
EXACTLY what you want: pick up where the single instance left off.

**Pattern B — per-process-unique consumer group (a generated id).** Use
when this consumer rebuilds a complete read model from a topic's FULL
history on every start (an event-sourced local cache or a replayable read
model — this repo's analytics projector uses
`NewUniqueConsumerGroup(AnalyticsConsumerGroupPrefix)`) — see a sibling's
`facilitycache/consumer.go`'s `consumerGroupPrefix` +
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

### 4. Readiness gate, if this consumer backs a local cache

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
go test -tags=integration ./internal/adapters/...   # testcontainers Kafka + Postgres
```

Prove any new fitness-test-adjacent behavior actually matters by running
the specific scenario against a real broker if this repo has
testcontainers-based integration tests for the consumer/publisher touched.
