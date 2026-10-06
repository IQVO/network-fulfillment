# DDD artifacts (ddd-crew)

The strategic and tactical design of **network-fulfillment**, drawn with
the [ddd-crew](https://github.com/ddd-crew) tools plus UML/ER diagrams.
Each page below follows one tool; every diagram is Mermaid and is derived
from code, not from intent.

| Artifact | ddd-crew tool / notation | Page |
| --- | --- | --- |
| Where this context sits on differentiation vs complexity | [Core Domain Charts](https://github.com/ddd-crew/core-domain-charts) | [core-domain-chart.md](core-domain-chart.md) |
| Purpose, classification, every inbound/outbound message | [Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas) | [bounded-context-canvas.md](bounded-context-canvas.md) |
| Upstream/downstream relationships and their patterns | [Context Mapping](https://github.com/ddd-crew/context-mapping) | [context-map.md](context-map.md) |
| `NetworkOrder` and `CapabilityOffer` design | [Aggregate Design Canvas v1.1](https://github.com/ddd-crew/aggregate-design-canvas) | [aggregate-design-canvas.md](aggregate-design-canvas.md) |
| Key business scenarios across contexts | [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling) | [domain-message-flow.md](domain-message-flow.md) |
| Commands, events, policies, read models, hotspots | [EventStorming glossary & cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet) | [eventstorming.md](eventstorming.md) |
| Glossary mapped to code identifiers | Ubiquitous Language | [ubiquitous-language.md](ubiquitous-language.md) |
| Domain types and ports/adapters | UML class diagram | [class-diagram.md](class-diagram.md) |
| Postgres schema (OLTP + analytics) | ER diagram | [entity-relationship.md](entity-relationship.md) |
| Each use case, step by step | UML sequence diagram | [sequence-diagrams.md](sequence-diagrams.md) |
| Every event published and consumed | Event catalogue | [domain-events.md](domain-events.md) |

## Sources of truth

When a page and the code disagree, the code wins. The pages were derived
from these paths on `develop`:

- Domain: `internal/domain/networkorder/`, `internal/domain/capabilityoffer/`,
  `internal/domain/shared/` (value objects, errors, domain events).
- Application: `internal/application/usecases/`, `internal/application/ports/ports.go`,
  `internal/application/contract/contract.go`.
- Adapters: `internal/adapters/inbound/{http,poller,mcp,kafka}/`,
  `internal/adapters/outbound/*/`, `internal/adapters/kafka/cloudevents/`.
- Composition roots: `cmd/netfulfil`, `cmd/mcp`, `cmd/netfulfil-projector`,
  `cmd/netfulfil-reports`.
- Schema: `migrations/0001`–`0004` (OLTP) and `migrations/analytics/0001`.
- Contracts: `apis/openapi.yaml`, `apis/asyncapi.yaml` (both lag the code in
  places; see the discrepancy notes on [domain-events.md](domain-events.md)
  and [bounded-context-canvas.md](bounded-context-canvas.md)).
- Decisions: [`docs/adr/`](../adr/README.md), chiefly ADR 0001 (with 0009
  and 0014 amending it).

Strategic classification (**Supporting Subdomain**, Conformist upstream,
ACL downstream) comes from ADR 0001 and is not re-decided here.
Neighbour classifications follow the fleet's: `order-management`
Generic/Supporting; `inventory-storage`, `wes-work-planning`,
`fulfillment-execution`, `warehouse-planning` Core; `workforce-management`,
`labor-performance`, `warehouse-ops-agent`, `network-fulfillment`
Supporting; `facility-layout`, `process-path-management` Generic.
