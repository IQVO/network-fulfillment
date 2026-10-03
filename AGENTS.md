# Network Fulfillment

harness-template: v3

Supporting bounded context: the anti-corruption layer between the fleet and
an external retail fulfillment network. **Conformist** upstream, **ACL**
downstream. Owns **NetworkOrder** (built) and **CapabilityOffer** (planned).
Study project, not production (see README.md banner).

Read `docs/adr/0001-network-fulfillment-bounded-context.md` (companion of
`order-management` ADR 0020) AND `docs/adr/0002-retail-network-not-amazon-counterpart.md`
(Accepted, amends 0001: the counterpart is the fleet's own `retail-network`,
not a real retailer's SP-API) before writing network-facing code. Note
`docs/adr/0002-mcp-and-analytics-data-product.md` shares number 0002.

## Current state

Built: `NetworkOrder` aggregate, ACL boundary, use cases
`ReceiveNetworkDemand` / `SweepAcknowledgementDeadlines`, in-memory and
Postgres repos (`DATABASE_URL` set means Postgres or REFUSE to boot, never
a silent fallback), stub gateway, poller (inbound leg), READ-ONLY REST,
CloudEvents Kafka publishers + outbox, analytics projector, MCP adapter,
`web/` remote, Helm chart. Not built: `CapabilityOffer` and its Kafka caches,
a live `retail-network` gateway, shipment confirmation, transaction-status
reconciliation; no live network call exists.
Operations, CI, branch protection, gremlins thresholds, related fleet
ADRs: `docs/operations-notes.md`.

Core concept: **throughput-constrained advertised availability**
= `min(physicalAvailable, throughputFeasibleBefore(nextCutoff))` (ADR 0001).
`.claude/rules/*.md` describe the code as it is; never write planned
concepts into them as if they exist.

## Architecture (NON-NEGOTIABLE)

Hexagonal, enforced by `arch-go` tests in `internal/architecture/`:
**domain depends on nothing; application depends on domain; adapters
depend on application/domain.**

```
cmd/netfulfil (composition root; also cmd/mcp, cmd/netfulfil-projector, cmd/netfulfil-reports)
internal/domain/{networkorder,shared}  internal/application/{contract,ports,usecases}
internal/adapters/inbound/{http,poller,kafka,mcp}
internal/adapters/outbound/{network,ordermanagement,postgres,memory,kafka,events,...}
```

## Hard rules

1. **The network's vocabulary stops at `adapters/outbound/network/`**
   (becomes `outbound/retailnetwork/` per ADR 0002). `purchaseOrderNumber`,
   `itemSequenceNumber`, `buyerProductIdentifier` (ASIN) and other
   real-retailer field names must never appear in `internal/domain` or
   anything published to the fleet; nor may `retail-network`'s (`poNumber`,
   `listingId`, `nodeId`, reason codes) leak past that package. `arch-go`
   cannot catch a vocabulary leak: that check is human.
2. **No ship-to PII reaches this context** (ADR 0002 amending 0001): only
   a `poNumber`, lines, quantities and `requiredShipBy` come in; a label
   request returns only `{labelRef, trackingNumber, carrier}`. Hence no
   auth for PII reasons; unauthenticated like every fleet context.
3. **Never recompute promise math here.** Ask `order-management`'s
   `PromisePolicy.FeasibleBy` (its ADR 0020); a second copy drifts.
4. **Correlation is a persisted mapping, never a string convention.**
   `NetworkOrder.localOrderId` is stored explicitly (OM ADR 0018).
5. **Network demand is ship-complete.** The network confirms or rejects a
   purchase order in full; partial acknowledgements are rejected. OM ADR
   0017's per-shipment-group promising must not apply.
6. **`NETWORK_MODE=live|stub`, default `stub`** (ADR 0002 removed
   `sandbox`). The kind cluster, `e2e-tests` and CI must never need a
   credential. Log the chosen mode at startup.
7. **A FirstOffset-replay Kafka cache needs a consumer group id unique per
   process instance** (hostname+PID+timestamp); a shared group resumes from
   an old offset and is marked ready with an empty cache.
8. **Submissions are asynchronous.** `submitAcknowledgement` /
   `submitShipmentConfirmations` return accepted-for-processing; reconcile
   via `getTransactionStatus`. A 200 is not a commitment: model
   submitted-but-unreconciled as its own state; release work only after
   the acknowledgement reconciled to `SUCCESS` (ADR 0002 D5).
9. **REST is read-only.** Demand arrives by polling only (ADR 0001 section
   5); no write endpoint without a new ADR.

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message produced or consumed (`warehouse.network-fulfillment.events`
and `.analytics`) is a CloudEvents 1.0 structured-mode event: no flat
envelope, no dual-write/read, no toggle. Use `github.com/cloudevents/sdk-go/v2/event`
via `internal/adapters/kafka/cloudevents/`; header `content-type:
application/cloudevents+json; charset=UTF-8`. Required: `specversion=1.0`,
`id` (UUID, stable across outbox redelivery), `source=/warehouse/network-fulfillment`,
`type=com.warehouse.wes.network-fulfillment.<entity>.<EventName>`, `subject`
(aggregate id), `time` (UTC), `datacontenttype=application/json`,
`dataschema=urn:warehouse:network-fulfillment:<events|analytics>:<EventName>:v<N>`.
Breaking change => new `.v2` type and dataschema. Consumers dispatch on the
FULL `type`, ignore unknown types, dedupe on `id`, DLQ/skip anything that
fails validation (ADR 0008: `docs/adr/0008-cloudevents-mandatory-event-envelope.md`).

## Commands (HARNESS.md describes each sensor)

```bash
make check-fast   # fmt-check + vet + arch-test: run before saying "done"
make check-all    # check + coverage (90% gate) + arch-test + bdd (no features/ yet)
make integration  # Postgres via testcontainers, never skip-gated
make mutation     # gremlins on ./internal/domain/networkorder (CI: mutation-fast)
make guide-lint   # these agent guides: references resolve, context budget
```

Git: GitFlow, `feature/*` off `develop`, PR into `develop`, `develop` promotes
to `main`. Do not merge your own PR; leave it open for independent review.

<!-- harness:scoped-rules:start (generated by tools/migrate_v3.py in warehouse-harness-template; do not hand-edit) -->
## Scoped rules and harness

Claude Code loads each rule below automatically when you touch the matching paths. OpenCode and Codex do NOT: read the rule BEFORE editing matching files.

| When touching | Read |
|---|---|
| `internal/adapters/**/kafka/**`, `internal/adapters/outbound/events/**`, `apis/asyncapi*` | `.claude/rules/integration-events.md` |
| `internal/adapters/inbound/http/**`, `apis/openapi*.yaml`, `apis/openapi/**` | `.claude/rules/rest-api.md` |

Hooks (`scripts/harness/hook.py`, wired for Claude Code, Codex and OpenCode) block pushes to develop/main, `--no-verify`, bare `rm -rf`, and edits to generated files, and feed gofmt/vet findings back after each edit. Before saying "done" run `make check-fast`; the full gate is `make check-all`. `HARNESS_OFF=1` disables the hooks when debugging the harness itself.
<!-- harness:scoped-rules:end -->
