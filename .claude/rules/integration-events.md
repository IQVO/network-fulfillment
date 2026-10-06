---
paths:
  - "internal/adapters/**/kafka/**"
  - "internal/adapters/outbound/events/**"
  - "apis/asyncapi*"
---

# Cross-service integration events (Kafka)

**Envelope: CloudEvents 1.0 is MANDATORY (ADR 0008).** Every Kafka message
this service produces or consumes is a CloudEvents 1.0 event in structured
content mode. No flat envelope (`event_id`/`event_type`/`occurred_at`), no
`schema_version`, no dual-write/dual-read, no envelope toggle.

## What exists today

- **Publishers** (`internal/adapters/outbound/kafka/`), active when
  `EVENT_PUBLISHER=kafka` (otherwise `cmd/netfulfil` uses a log-only
  publisher):
  - `publisher.go` — integration topic `warehouse.network-fulfillment.events`.
  - `analytics_publisher.go` — analytics topic
    `warehouse.network-fulfillment.analytics`.
  - Both implement `Encoder`; with `DATABASE_URL` set they feed the
    transactional outbox (`internal/adapters/outbound/postgres/outbox_*.go`,
    ADR 0003), otherwise they publish directly.
- **Consumer** (`internal/adapters/inbound/kafka/analytics_consumer.go`,
  run by `cmd/netfulfil-projector`) — consumes this service's OWN analytics
  topic into the acknowledgement report. This service consumes no other
  context's events.
- Cross-context *commands* are synchronous REST to order-management
  (`internal/adapters/outbound/ordermanagement`), plus polling the
  external network through `ports.NetworkGateway`.

## The wire format

Built ONLY via `internal/adapters/kafka/cloudevents` (`New`, `Decode`,
`ContentTypeHeader`), which wraps `github.com/cloudevents/sdk-go/v2/event`.
Transport stays `segmentio/kafka-go`; never use sdk-go's protocol/client
packages and never hand-roll an envelope struct.

```json
{
  "specversion": "1.0",
  "id": "<uuid, minted once, persisted in the outbox row>",
  "source": "/warehouse/network-fulfillment",
  "type": "com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived",
  "subject": "<networkRef>",
  "time": "<occurred-at, UTC RFC 3339>",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:network-fulfillment:events:NetworkOrderReceived:v1",
  "data": { "networkRef": "po-1", "siteId": "site-1", "lineCount": 2, "at": "..." }
}
```

- Kafka header on every message:
  `content-type: application/cloudevents+json; charset=UTF-8`.
- Kafka key = `networkRef` (= `subject`), `kafkago.Hash{}` balancer (ADR 0005).
- Same `type` on both topics; the `dataschema` stream segment is `events`
  or `analytics`. Breaking payload change => new `.v2` type + new
  dataschema version, never mutate an existing one.
- Published types: `com.warehouse.wes.network-fulfillment.networkorder.`
  + `NetworkOrderReceived` | `NetworkOrderAcknowledged` |
  `NetworkOrderRejected` | `NetworkOrderShipmentConfirmed`, and
  `networkorder.AcknowledgementDeadlineAtRisk` — five events, all on BOTH
  topics (ADR 0015). `NetworkOrderAcknowledged` fires at SUBMITTED, before
  reconciliation; the analytics projector handles only Received/Acknowledged/
  Rejected and ignores the other two.

## Consumer rules

- Decode with `cloudevents.Decode`; a message that fails CloudEvents
  validation (incl. a legacy flat message) is dead-lettered to
  `<topic>.dlq` immediately and committed past — never retried, never
  parsed as a legacy shape, never blocks the partition.
- Dispatch on the FULL `type` string; ignore unknown types.
- Dedupe on the CloudEvents `id`; read `time`/`subject` from attributes and
  the payload via `DataAs`.
- Consumer group ids must be env-configurable or generated, never inline
  literals (`TestKafkaConsumerGroupNeverHardcodedInline`). A consumer that
  replays from FirstOffset must use a group id unique per process instance
  (hostname+PID+timestamp) — `NewUniqueConsumerGroup`.

## Contract rules

- Nothing network-shaped crosses into a published event — no purchase-order
  numbers as fleet identities, no ASINs, no network status codes, no PII.
- Every published message is documented in `apis/asyncapi.yaml` with its
  exact `type` and `dataschema`; update it in the same PR.
- Each published type has a golden exact-JSON test (all attributes + the
  content-type header) in `internal/adapters/outbound/kafka/*_test.go`.
- Kafka integration tests start their own broker via testcontainers
  (`TestKafkaIntegrationTestsUseTestcontainers`) — never skip-gated, never
  `localhost:9092`.
