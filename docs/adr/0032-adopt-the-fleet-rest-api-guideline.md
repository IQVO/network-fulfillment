---
id: 0032-adopt-the-fleet-rest-api-guideline
slug: /adr/0032-adopt-the-fleet-rest-api-guideline
title: "32. Adopt the fleet REST API guideline"
sidebar_label: "32. Fleet REST API guideline"
sidebar_position: 32
description: "ADR 0032 adopts the fleet REST API guideline: the one POST sits behind an Idempotency-Key middleware (409 reuse, Idempotency-Replayed), both collection GETs are keyset-paginated with {items, nextCursor}, and the order-management release call moves to POST /orders/{id}/releases."
---

# 32. Adopt the fleet REST API guideline

## Status

**Accepted** (2026-11-01). Part of the fleet REST guideline cutover set: it must
deploy together with the other services' cutover PRs (order-management in
particular, whose release route this context calls).

## Context

The fleet REST API guideline (warehouse-docs, REST guideline) makes four rules
mandatory for every `apis/openapi.yaml`, enforced by the `rest-guideline`
Spectral rule in `.spectral.yaml` (R1-R8):

1. URLs are nouns: no `:verb` suffix and no verb path segment; a command is a
   `PUT` of an attribute, a `DELETE`, or a `POST` creating a noun (a transition
   resource such as `/orders/{id}/releases`).
2. Every `POST` and `PATCH` requires `Idempotency-Key` (1-128 printable ASCII).
   The fingerprint is method + path + SHA-256 of the body; the same key and
   fingerprint replays the stored answer with `Idempotency-Replayed: true`; the
   same key with another fingerprint is `409 idempotency-key-reused`; a missing
   key is `400 idempotency-key-required`. 2xx and 4xx are stored, a 5xx is not.
3. Every collection `GET` pages with `limit` (1-500, default 100) and an opaque
   keyset `cursor`, answers `{ "items": [...], "nextCursor": "..." }`, and
   rejects an undeclared query parameter with `400 unknown-query-parameter`.
4. Consumers send the key, read `items` and follow (or bound) `nextCursor`.

This context had no idempotency middleware (its one write,
`POST /network-orders/{networkRef}/shipment-confirmation`, ADR 0014, relied on
the domain's own "already CONFIRMED" rule), answered its two lists under
per-resource keys (`networkOrders`, `capabilityOffers`) without paging, and
called order-management's retired `POST /orders/{id}/release`.

## Decision

- **Routes.** None of this context's own routes is renamed: the rename map has
  no entry for it, and `shipment-confirmation` is already a noun.
- **Idempotency.** `POST /network-orders/{networkRef}/shipment-confirmation`
  sits behind `inboundhttp.Idempotent`, route-scoped. Records live in the
  `idempotency_keys` table (migration 0012), keyed by the key ALONE (global, not
  per route), kept at least 24 hours. Concurrent requests with one key are
  serialized by the primary key: one claims, the others wait and replay its
  answer. The key is required (`required: true`): Postgres is the only runtime
  (ADR 0025), so the middleware is active in every mode.
- **Deviation: the key row does not join the order's unit of work.** The use
  case is a saga that commits in phases around two external calls (it asks the
  network for a label BEFORE the transaction that saves `CONFIRMED` and writes
  the outbox rows, and submits the confirmation AFTER it, ADR 0014). Forcing one
  transaction around the whole request would hold a database transaction open
  across network calls and hide the intermediate state from the sweeps. The key
  is therefore claimed before the handler runs, completed after it answers, and
  released on a 5xx or panic. Duplicate execution across replicas is still
  prevented by the claim (first insert wins, the loser waits and replays), and
  the domain rule (an order already `CONFIRMED` is answered 204 without a second
  submission or event) still covers a crash between the order's commit and the
  key's completion.
- **Pagination.** `GET /network-orders` (unanswered working set) is ordered by
  (`acknowledgeBy`, `networkRef`); `GET /capability-offers` by (`sku`,
  `siteId`). Both use the fleet `page` helper copied verbatim to
  `internal/adapters/inbound/http/page`, in the Postgres and in-memory
  repositories alike. No exemptions are declared. The MCP and A2A tools keep
  their own (unchanged) schemas and read the repositories directly.
- **Consumers in this repository.**
  - `internal/adapters/outbound/ordermanagement/planner.go`: release is
    `POST /orders/{id}/releases` with a stable key `netful-release-<id>`.
  - `web/` (netfulfil_mfe remote): reads `items`, calls with `limit=500` and
    reports a returned `nextCursor` (guideline 5.2, bounded single page).
  - BDD, unit and integration HTTP clients send a fresh key per submit.

## Consequences

- Breaking for every client of the two lists and of the POST; it must ship in
  the same set as the other cutover PRs.
- A retried shipment confirmation with the same key now replays the first
  answer byte for byte, including a 4xx (`confirm-before-acknowledge` replays
  even after the order was acknowledged): a new user action needs a new key.
- `make check` covers the middleware with the in-memory store; the
  `-tags=integration` suite covers it over real Postgres (fresh key, replay with
  `Idempotency-Replayed`, 409 reuse, 400 missing key, 4xx cached, N concurrent
  requests producing one `NetworkOrderShipmentConfirmed`).
