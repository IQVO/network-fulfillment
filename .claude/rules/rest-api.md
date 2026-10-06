---
paths:
  - "internal/adapters/inbound/http/**"
  - "apis/openapi*.yaml"
  - "apis/openapi/**"
---

# REST API (inbound adapter)

`internal/adapters/inbound/http`, contract in `apis/openapi.yaml` (linted by
Spectral in the `api-lint` CI job; this repo has no docs site and no
`docs-api-drift` job). Routes are registered on a stdlib `http.ServeMux`
in `server.go`'s `Routes()`.

**Read-only by design, with one explicit write.** Demand arrives only by
polling the network (ADR 0001 section 5); an intake endpoint would be a second,
fictional inbound path, and `NETWORK_SEED_FILE` already injects stub demand
declaratively. Do not add one without a new ADR. The only write is
`POST /network-orders/{networkRef}/shipment-confirmation` (below).

Handlers read `ports.NetworkOrderRepo` directly rather than through a use
case — every route is a pure projection of stored state.

- `GET /healthz` — liveness only; never touches Postgres or the poller.
- `GET /inbound-status` — `networkMode`, poller counters (`polls`,
  `received`, `failed`, `since`) and `unanswered`/`overdue` counts.
- `GET /network-orders` — orders still `NEW` (`{"networkOrders": [...]}`).
- `GET /network-orders/{networkRef}` — one order with its lines, `state`,
  `acknowledgeBy`, `acknowledgementOverdue` and `localOrderId` (once linked).
- `GET /readyz` — readiness; `GET /metrics` only when a metrics registry is wired.
- `GET /capability-offers` — registered only when `Server.Offers` is set
  (ADR 0001 section 8).
- `POST /network-orders/{networkRef}/shipment-confirmation` — registered only
  when `Server.ConfirmShipment` is set. Confirming an order that is not yet
  acknowledged returns 409 `confirm-before-acknowledge`
  (`ErrConfirmBeforeAcknowledge`). `corsMiddleware` allows POST for it.

In the kind cluster Kong serves these at
`http://localhost:8000/api/network-fulfillment/...`.

## Conventions

- Error shape: RFC 7807 `application/problem+json`. `errors.go` maps each
  typed error to a status (`statusFor`) and a type/title (`problemFor`);
  the two tables must stay one-for-one (`errors_test.go` enforces it).
  `ErrOrderNotFound` -> 404; `ErrConfirmBeforeAcknowledge` -> 409; domain
  validation errors and
  `ErrUnknownProduct` -> 422; anything else -> 500 `internal-error`.
- No response carries customer PII. The network's own references are
  exposed because they identify the order, not the person.
- Auth: none. The fleet-wide REST+MCP auth was reverted and
  `internal/architecture/fitness_test.go`'s `TestNoAuthMiddlewareReintroduced`
  guards against re-adding it. ADR 0001 records that this context cannot
  stay unauthenticated once it holds credentials and real PII — that needs
  a new decision, not a quiet middleware.
