# Cross-service integration events (Kafka)

**Current state: this service neither publishes to nor consumes from Kafka.**
There is no Kafka client, topic or consumer group in the code.

- `ports.EventPublisher` exists; `cmd/netfulfil` wires it to a
  `logPublisher` that only writes a structured log line. No use case calls
  `Publish` yet.
- Cross-context interaction today is synchronous REST to order-management
  (`internal/adapters/outbound/ordermanagement`: `POST /orders` for a held
  order carrying `requiredShipBy`, `POST /orders/{id}/release`,
  `DELETE /orders/{id}`), plus polling the external network through
  `ports.NetworkGateway`.

## When Kafka arrives

ADR 0001's `CapabilityOffer` is planned to consume fleet facts (inventory
availability, the process-path CPT schedule, wes-work-planning path
capacity), and publishing comes later. When that lands:

- One broker for the whole fleet; topic naming `warehouse.<context>.events`;
  CloudEvents-style envelopes with
  `com.warehouse.<subdomain>.network-fulfillment.<entity>.<EventName>` types.
- Nothing network-shaped crosses into a published event — no purchase-order
  numbers as fleet identities, no ASINs, no network status codes, and no
  customer PII.
- Consumer group ids must be env-configurable, never inline literals
  (`TestKafkaConsumerGroupNeverHardcodedInline`). A consumer that replays
  from FirstOffset to build an in-memory cache must use a group id unique
  per process instance (hostname+PID+timestamp).
- Kafka integration tests must start their own broker via testcontainers
  (`TestKafkaIntegrationTestsUseTestcontainers`) — CI provides no broker.
- Add `apis/asyncapi.yaml` in the same PR.
