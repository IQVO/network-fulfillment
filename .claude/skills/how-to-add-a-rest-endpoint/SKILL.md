---
name: how-to-add-a-rest-endpoint
description: Add or change a REST endpoint in this service in the fleet's hexagonal order (domain invariant, use case, port, HTTP adapter, apis/openapi.yaml). Use when touching internal/adapters/inbound/http, apis/openapi.yaml, or exposing a use case over HTTP.
---

# How to add a REST endpoint

> **Read this first.** The REST surface is **read-only by design** (see
> `.claude/rules/rest-api.md` and ADR 0001 section 5): demand arrives only
> by polling, so do **not** add a write/intake endpoint without a new ADR.
> This guide therefore covers adding a new *read* route. There is no docs
> site, no `docs-api-drift` job and no `features/` directory here: update
> `apis/openapi.yaml` and let the `api-lint` CI job (Spectral) check it.

Follow the fleet's order: domain first, adapter last. A handler written
before the domain invariant it exposes validates nothing.

The worked example is `GET /inbound-status`: route in
`internal/adapters/inbound/http/server.go` (`Routes()` registers
`mux.HandleFunc("GET /inbound-status", s.handleInboundStatus)` on a stdlib
`http.ServeMux`), response mapping `toInboundStatusResponse` in `dto.go`,
contract under `/inbound-status` in `apis/openapi.yaml`. Read those
alongside this guide.

## 1. Domain first

Check `internal/domain/networkorder/` for the rule the route exposes (for
example `acknowledgementOverdue` is a domain predicate, not handler logic).
A handler decodes, reads, maps and encodes; it holds no business rule. If
a new rule is needed, add it to the aggregate with a table-driven unit
test before touching any adapter.

## 2. Application: only if a read is not a pure projection

The existing handlers read `ports.NetworkOrderRepo` directly because every
route is a projection of stored state. If your route needs orchestration
or a rule, add a use case as one file in `internal/application/usecases/`
(see `receive_network_demand.go`), taking driven ports only
(`internal/application/ports/ports.go` holds interfaces ONLY, enforced by
`internal/architecture/`). Unit-test it against the in-memory adapter in
`internal/adapters/outbound/memory/`, covering the success and the
domain-failure path.

## 3. Adapter: wire the handler

In `internal/adapters/inbound/http/`:

1. `dto.go` — response struct with JSON tags and a `to...Response` mapper.
   DTOs live only in this adapter; domain types never carry JSON tags.
2. `server.go` — register the route in `Routes()` and add
   `handle<Name>(w, r)`: parse path/query into domain value objects
   (`shared.NetworkRef(r.PathValue("networkRef"))`), read via the port,
   translate the repo's `(nil, nil)` into `usecases.ErrOrderNotFound`,
   then `writeJSON`; on any error call `writeError(w, r, err)`.
3. `errors.go` — a new error type needs entries in BOTH `statusFor` and
   `problemFor`; `errors_test.go` enforces they stay one-for-one.
4. Add the dependency to the `Server` struct and wire it in
   `cmd/netfulfil/main.go` (the composition root).

Add httptest cases next to `server_test.go` (success + each error path),
including a no-PII assertion if the payload carries order data
(`TestGetNetworkOrder_CarriesNoCustomerPII` is the model).

## 4. Contract

Add the path, schemas and the RFC 7807 `application/problem+json` error
responses to `apis/openapi.yaml`; `.spectral.yaml` is the ruleset
(`spectral lint apis/openapi.yaml --ruleset .spectral.yaml`, run in CI as
`api-lint`).

## 5. Verify

```bash
make check-fast   # fmt-check vet arch-test
make check-all    # check + coverage (90% gate) + arch-test + bdd
```

`make coverage` gates `./internal/domain/...,./internal/application/...`
at 90%; a new use case with no failure-path test is the usual miss.
