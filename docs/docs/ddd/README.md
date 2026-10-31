# DDD artifact pack — network-fulfillment

Plain GitHub-rendered markdown (this repo has no docs site). Every diagram
is Mermaid, derived from the code on `develop`, with a **Source:** line
naming the files it came from and a note on what it leaves out.

Start with **[ddd-artifacts.md](/docs/ddd/ddd-artifacts)**: the index of the pack,
which ddd-crew tool each page follows, and the sources of truth.

| Page | What it is |
| --- | --- |
| [ddd-artifacts.md](/docs/ddd/ddd-artifacts) | Index + sources of truth |
| [core-domain-chart.md](/docs/ddd/core-domain-chart) | ddd-crew Core Domain Chart |
| [bounded-context-canvas.md](/docs/ddd/bounded-context-canvas) | ddd-crew Bounded Context Canvas v5 |
| [context-map.md](/docs/ddd/context-map) | ddd-crew Context Mapping (this context's slice) |
| [aggregate-design-canvas.md](/docs/ddd/aggregate-design-canvas) | ddd-crew Aggregate Design Canvas v1.1 |
| [domain-message-flow.md](/docs/ddd/domain-message-flow) | ddd-crew Domain Message Flow Modelling |
| [eventstorming.md](/docs/ddd/eventstorming) | EventStorming, design level |
| [ubiquitous-language.md](/docs/ddd/ubiquitous-language) | Glossary, mapped to code identifiers |
| [class-diagram.md](/docs/ddd/class-diagram) | UML class diagrams + hexagonal ports/adapters |
| [entity-relationship.md](/docs/ddd/entity-relationship) | ER diagrams (OLTP + analytics) |
| [sequence-diagrams.md](/docs/ddd/sequence-diagrams) | UML sequence diagrams per use case |
| [domain-events.md](/docs/ddd/domain-events) | Every event published and consumed |
| [use-cases.md](/docs/ddd/use-cases) | Every application use case: trigger, inputs, invariants, events |
| [subdomain-classification.md](/docs/ddd/subdomain-classification) | Supporting, `wes` tier, with the neighbours' fleet classifications |

ADRs: [../adr/README.md](https://github.com/IQVO/network-fulfillment/blob/develop/docs/adr/README.md). Operations: [../operations/runbook.md](/docs/operations/runbook).
All documentation: [../overview/introduction.md](/docs/overview/introduction#documentation-map).
