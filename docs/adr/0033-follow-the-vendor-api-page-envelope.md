---
id: 0033-follow-the-vendor-api-page-envelope
slug: /adr/0033-follow-the-vendor-api-page-envelope
title: "33. Follow the Vendor API page envelope, and gate the pinned copy against develop"
sidebar_label: "33. Vendor API page envelope"
sidebar_position: 33
description: "ADR 0033: the retail-network adapter reads { items, nextCursor } from the Vendor API, refuses any 200 that is not a page, repeats the filters on every page, and a CI job fails when the pinned vendor-connectivity spec differs from its develop."
---

# 33. Follow the Vendor API page envelope, and gate the pinned copy against develop

## Status

**Accepted** (2026-11-05). Amends the paging parts of [ADR 0026](./0026-live-retail-network-adapter-demand-loop.md)
and [ADR 0028](./0028-inbound-loop-seller-shipments-and-receipts.md) (`nextToken`, a token that
replaces the filters). Those bodies stay as written.

## Context

On 2026-11-01 vendor-connectivity adopted the fleet REST guideline: every collection answers
`{ "items": [...], "nextCursor": "..." }`, pages with `limit` (1-500) and `cursor`, requires the
caller to repeat the filters on every page, and answers `400 unknown-query-parameter` to a key it
does not declare. This adapter still decoded `purchaseOrders`, `inboundShipments` and `nodes` and
followed `nextToken`.

The failure was silent. `encoding/json` ignores unknown keys, so the retired shape decoded to an
empty list; the poll returned 200 with zero purchase orders and zero inbound shipments, and no log
line, metric or test said so. The contract test stayed green because its spec copy was pinned at
the 2026-10-24 file, so it validated the adapter against a contract the vendor no longer served.

## Decision

1. The adapter reads one generic page shape, `pageDTO[T]` (`items`, `nextCursor`), for purchase
   orders, inbound shipments and the vendor's nodes. `collectPages` follows `nextCursor` until it
   is absent, refuses a repeated cursor and a backlog of more than `maxPollPages` pages.
2. **A 200 that is not a page is an error.** `pageDTO` refuses a body without an `items` array
   (absent or `null`). The vendor never answers `items: null`, so a retired or misspelt shape
   fails the poll loudly instead of reading as "nothing to do". This is the guard against the
   incident's failure mode; the new field names alone only fix today's drift.
3. The filters (`vendorId`, `nodeId`, `createdAfter`, `limit`) are sent on every page request,
   next to `cursor`. The overlap window is still applied once, from the watermark.
4. The contract test also checks that every query key the client sends is declared by the
   operation in the spec, because an undeclared key is now a `400`.
5. The pinned copy is checked against the producer. `contracts/pinned.json` lists each pin and
   its producer branch; the `pinned-contracts` CI job (`scripts/harness/pinned_contract_gate.py`)
   fails on any byte difference with vendor-connectivity's `develop`, and fails (exit 2) when the
   producer cannot be read. Refreshing a pin stays a deliberate act: copy the file, fix what fails.

## Consequences

- The next vendor contract change turns this repo's CI red until the copy is refreshed and the
  adapter follows it. That is the point: the failure appears in a PR, not in production traffic.
- The gate reads the network on every PR. If that proves flaky, run it on `schedule` only; do not
  turn it into a skip on read errors.
- Not covered here: the other consumers of the Vendor API (see the fleet audit in the PR that
  introduced this ADR).
