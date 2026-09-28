
# How to add a REST endpoint

> **In network-fulfillment, read this first.** The REST surface is
> **read-only by design** (see `.claude/rules/rest-api.md` and ADR 0001
> section 5): demand arrives only by polling, so do not add a write/intake
> endpoint without a new ADR. For a new *read* route the real files are
> `internal/adapters/inbound/http/server.go` (`Routes()` on a stdlib
> `http.ServeMux`, e.g. `mux.HandleFunc("GET /inbound-status", ...)`),
> `dto.go`, and `errors.go` (keep `statusFor` and `problemFor` one-for-one).
> Read handlers use `ports.NetworkOrderRepo` directly, not a use case.
> The worked example below (`POST /bins/{binId}/cycle-count`,
> `RunCycleCount`, `r.Post`, `shared.NewBinId`, `features/`,
> `docs/` regeneration) comes from the fleet template and does **not**
> exist here: this repo has no docs site, no `docs-api-drift` job and no
> `features/`/`bdd` job — update `apis/openapi.yaml` and let `api-lint`
> (Spectral) check it.

Use when asked to add a new REST use case/endpoint to this service. Follow
this order — domain first, adapter last — never the reverse; writing the
HTTP handler before the domain invariant it enforces produces handlers
that validate nothing and use cases that get bypassed.

This walks the exact path `POST /bins/{binId}/cycle-count` took
(`internal/application/usecases/run_cycle_count.go` +
`internal/adapters/inbound/http/server.go`'s `handleRunCycleCount`) as the
concrete worked example — read those two files alongside this guide.

## 1. Domain first: does an invariant already exist, or do you need one?

Check `internal/domain/<aggregate>/` for the rule this endpoint enforces.
A REST endpoint should almost never contain business logic itself — it
decodes a request, calls a use case, encodes the result. If the operation
needs a new domain rule (e.g. "a counted quantity below zero is invalid"),
add it to the aggregate/value-object in `internal/domain/`, with its own
table-driven unit test, BEFORE touching the application or adapter layers.

## 2. Application: define the use case

Add a new file in `internal/application/usecases/` (one file per use
case, this repo's convention — not one giant `usecases.go`). Shape:

```go
package usecases

type <Verb><Noun>Result struct {
    // fields the caller needs back — domain types, not DTOs
}

// <Verb><Noun> — one sentence: what business capability this represents,
// and the domain rule it enforces (mirror RunCycleCount's doc comment,
// which states the Unlocated-on-shortfall rule right in the doc comment).
type <Verb><Noun> struct {
    Repo   ports.<Aggregate>Repo   // driven ports only — never a concrete adapter
    Events ports.EventPublisher    // if this raises a domain event
    Clock  ports.Clock             // if it needs "now" (never call time.Now() directly)
}

func (uc *<Verb><Noun>) Execute(ctx context.Context, /* domain-typed args */) (<Verb><Noun>Result, error) {
    // 1. load aggregate(s) via the port
    // 2. call the aggregate's own method to apply the rule (never inline
    //    the invariant here — that belongs in internal/domain/)
    // 3. persist via the port
    // 4. publish the domain event via Events, if any
    // 5. return the result
}
```

Add the port to `internal/application/ports/` if it doesn't exist yet —
ports are interfaces ONLY (`TestPortsAreCustomerOwned`/
`TestApplicationPortsContainOnlyInterfaces`-style fitness tests in
`internal/architecture/` enforce this; a struct or function in a ports
package fails CI).

Write the use case's unit test against the in-memory adapter
(`internal/adapters/outbound/memory/`) — never a real Postgres/HTTP call
in a unit test. Cover the success path AND the domain-rule failure path.

## 3. Adapter: wire the HTTP handler

In `internal/adapters/inbound/http/`:

1. `dto.go` — add the request/response DTO structs (JSON tags, this repo's
   naming convention: `<verb><noun>Request`/`<verb><noun>Response`).
   DTOs live ONLY in the adapter layer — domain types never carry JSON
   tags.
2. `server.go` — add the route (`r.Post("/path/{param}", s.handle<Name>)`
   in the router setup) and the handler function:
   - decode + validate the request (`decodeJSON`), converting to domain
     value objects immediately (`shared.NewBinId`, `shared.NewQuantity`,
     etc.) — a bad value fails here as an RFC 7807 validation error, never
     reaches the use case
   - call the use case's `Execute`
   - map use-case errors to HTTP status via `writeError` (check
     `errors.go` for the existing error→status mapping before adding a new
     error type)
   - encode the domain result back to the response DTO and `writeJSON`
3. Add the new use case field to the `Server`/`Deps` struct and wire it in
   the composition root (`cmd/<service>/main.go`).

Write at least one httptest per endpoint: one success path, one error
path (validation failure AND/OR the domain-rule failure, whichever this
endpoint can produce).

## 4. Contract: update OpenAPI, then regenerate docs

Add the path to `apis/openapi.yaml` (request/response schemas, the RFC
7807 problem-detail response for each error case — see the existing
`/bins/{binId}/cycle-count` entry for the shape).

Regenerate the Docusaurus REST reference — this repo's `docs-api-drift`
CI job fails the PR if you skip this:

```bash
cd docs
npm run clean-api-docs   # or the repo's own script name — check package.json
npm run gen-api-docs
```

## 5. Behaviour: add a godog scenario

If this endpoint is user-facing behaviour (not purely internal
plumbing), add a `.feature` file under `features/` exercising it
end-to-end against the real HTTP server — see `features/cycle_count.feature`
for the exact shape this repo's `bdd` CI job expects (Given/When/Then over
real HTTP, not mocked).

## 6. Verify before opening the PR

```bash
make check       # fmt-check vet build lint test
make check-all    # + coverage (90% gate) + arch-test + bdd
```

`make coverage` gates `./internal/domain/...,./internal/application/...`
at 90% — a new use case with no test on its failure path is the most
common way to miss this gate.
