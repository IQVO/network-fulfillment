---
id: 0012-network-seed-file-stub-demand-seeding
slug: /adr/0012-network-seed-file-stub-demand-seeding
title: "12. NETWORK_SEED_FILE seeds demand into the stub gateway only"
sidebar_label: "12. NETWORK_SEED_FILE stub seeding"
sidebar_position: 12
description: "ADR 0012 — NETWORK_SEED_FILE loads a fixed set of purchase orders into the stub NetworkGateway at boot for local/demo/e2e runs, and refuses to boot if set against the live gateway instead of silently doing nothing."
---

# 12. NETWORK_SEED_FILE seeds demand into the stub gateway only

## Status

Accepted — implemented in the same change that introduced this record
(`cmd/netfulfil/main.go` `seedStubDemand`, `network.LoadSeedFile`).

## Context

`NETWORK_MODE=stub` (ADR 0002/0009's default; the kind cluster, `e2e-tests`
and CI must never need a credential) means there is no real network
sending demand. Without a way to pre-load purchase orders, every local run
and e2e suite starts empty and someone has to manually poke the stub
through code or REST before a poll finds anything, which is exactly the
kind of hidden setup step that makes a demo or a test flaky and
undocumented. The seeding mechanism also has to be unambiguous about what
happens if it's misconfigured against a *real* gateway — silently doing
nothing there would look identical to "no demand has arrived yet" and
waste an operator's time debugging the wrong layer.

## Decision

`NETWORK_SEED_FILE`, if set, is loaded once at boot via
`network.LoadSeedFile` into the stub gateway
(`seedStubDemand` in `cmd/netfulfil/main.go`). The loader type-asserts the
configured `ports.NetworkGateway` to `*network.StubGateway` — if
`NETWORK_SEED_FILE` is set while `NETWORK_MODE=live` (a real gateway is
wired), boot **fails with a clear error**
(`"NETWORK_SEED_FILE is set but the gateway is not the stub: seeded demand
would never be delivered"`) rather than silently ignoring the variable;
someone who set it expected demand to appear and it silently never would.
A successful load logs the file and the number of demands seeded at
`INFO`.

## Consequences

Easier: local runs, demos and e2e tests get deterministic starting demand
with one env var and no manual setup step; a misconfigured seed file
against a live gateway fails loudly at boot instead of being a silent
no-op discovered hours later. Harder: the seed file format is coupled to
`network.LoadSeedFile`'s current parsing and is another boot-time
behaviour to keep in sync if the stub gateway's purchase-order shape
changes.
