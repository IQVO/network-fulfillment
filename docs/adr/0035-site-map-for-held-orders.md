---
id: 0035-site-map-for-held-orders
slug: /adr/0035-site-map-for-held-orders
title: "35. A site map turns the network's site into the warehouse site on the held order"
sidebar_label: "35. Site map for held orders"
sidebar_position: 35
description: "ADR 0035: NETWORK_SITE_MAP translates the network's site id (the node, 'site-1') to the warehouse site code (facility-layout, 'AUS3') before the held order is sent, so fulfillmentSiteId is no longer omitted for a node whose id is not a site code. Node adoption, SITE_ID and the product dictionary do not change."
---

# 35. A site map turns the network's site into the warehouse site on the held order

## Status

**Accepted** (2026-11-14). **Extends [ADR 0029](./0029-fulfillment-site-on-the-held-order.md)**:
that ADR sends the order's site as `fulfillmentSiteId` and omits it when it is not a site code;
this one adds the translation in front of that rule. It **supersedes one line of ADR 0029**, the
last item of its Decision, "No new environment variable and no chart value: infra values
required are none": there is now one optional environment variable and one chart value. ADR 0029
is not edited and stays as written. Records decision D1 of the e2e-tests repository's ADR 0001
(the warehouse building that serves the node `site-1` is facility-layout site `AUS3`).

## Context

The live deployment runs `SITE_ID=site-1`. That string is three things at once:

- the `vendorNodeCode` of the network node this service adopts (ADR 0026 Decision 7),
- the key of the CPT schedule in process-path-management that `RecomputeCapabilityOffers`
  looks up (ADR 0031), and
- the site of every `NetworkOrder` received for that node.

ADR 0029 sends the order's site to order-management as `fulfillmentSiteId`, but only when it
matches `^[A-Z0-9]+$` (order-management's rule, its ADR 0044; anything else is a 400
`invalid-fulfillment-site-id`). `site-1` has a lowercase letter and a dash, so the field was
**omitted**. Storefront-style orders therefore reached order-management with no site, and
everything downstream that routes by site (WES, fulfillment-execution, outbound-shipping)
could not route them. Nothing failed: the omission is a WARN
(`network order site is not a site code`) and the order is held and acknowledged as normal.

The warehouse building that serves the node `site-1` is facility-layout site `AUS3`. The
network's name for the node and the warehouse's name for the building are two different ids for
one place, and this service is the only place that knows both.

## Decision

1. **A new optional configuration, `NETWORK_SITE_MAP`.** Comma-separated
   `networkSite=warehouseSite` pairs, for example `site-1=AUS3`. Whitespace around every part is
   trimmed. Unset or blank is no map, and the behaviour is exactly the one before this ADR.
2. **It is strict, and it refuses to boot.** `loadBootConfig` (`cmd/netfulfil/config.go`)
   parses it with the other mandatory-config checks, before telemetry or any dial. The boot is
   refused, with an error that names `NETWORK_SITE_MAP` and the entry, for: a warehouse site that
   does not match `^[A-Z0-9]+$` (the same `ordermanagement.ValidSiteCode` the planner applies, so
   there is one rule, not two copies), a duplicate network site (even with the same value), an
   empty network site, an empty warehouse site, an entry with no `=`, and an empty entry (a
   stray comma). A typo in a map that was silently ignored would look healthy and route nothing,
   which is the failure this ADR exists to end. Two network sites may share one warehouse site.
3. **The planner applies it before the site check.** `ordermanagement.NewPlanner` takes a
   `WithSiteMap` option, passed in by `wirePlanner` from the boot configuration: configuration
   through the existing wiring, never a global, and the adapter reads no environment. In
   `RaiseHeldOrder`:
   - a **mapped** network site is sent as its warehouse site;
   - an **unmapped** site is decided as ADR 0029 says: sent when it is a site code, omitted (and
     logged) when it is not;
   - the shape check also runs on the translated value. Boot cannot produce an invalid mapped
     value, so this only matters to a caller that builds the planner by hand, and it keeps the
     rule that the planner never sends what order-management would answer 400 for.
4. **It applies in every network mode.** The planner is wired in stub and live alike, so the map
   is read in both and the chart renders it in both.
5. **What does not change.**
   - Node adoption stays keyed by `SITE_ID`: the node whose `vendorNodeCode` equals it is
     adopted (ADR 0026), and `site-1` stays the id of its CPT schedule.
   - `NetworkOrder.SiteId`, the events this service publishes, the offers it computes, the
     product dictionary's optional `site`, the node map tables and every request to the network
     keep the network's own id. The map is read at one place only: the `fulfillmentSiteId` of the
     held order.
   - The ports and the public Vendor API are unchanged. The request body to order-management
     changes only for a mapped site, and only by gaining the field.

### Rejected: rename the node

Renaming the network node (and so `SITE_ID`) to `AUS3` would have made the two ids equal, but it
creates a new node on the network, needs 86 new listing ids on it, and orphans the old node (the
rejection recorded in the e2e-tests repository's ADR 0001 D1). The map is one line of
configuration and moves nothing the network already knows.

### Rejected: loosen order-management's pattern, or translate there

order-management's `^[A-Z0-9]+$` mirrors facility-layout's site code; loosening it to accept
`site-1` would put a network's node name into the fleet's site vocabulary. Translating inside
order-management would make it learn the network's ids. The fleet's vocabulary stays warehouse
vocabulary, and the anti-corruption layer is where network vocabulary stops (ADR 0009 and
ADR 0026: the network's names end at the `retailnetwork` adapter).

## Consequences

- With `site-1=AUS3` the held order carries `fulfillmentSiteId: AUS3`, and order-management
  projects demand to AUS3 and downstream services can route by it. Without the entry, nothing
  changes.
- It does nothing useful alone. Downstream routing on `AUS3` needs that site to exist there (a
  CPT schedule in process-path-management and a layout in facility-layout). Those, and the
  deployment value, are infra and data outside this repository.
- **Deployment.** The chart value is `config.networkSiteMap` (empty by default, so nothing is
  rendered), set per environment in warehouse-infra's values. The chart does not parse the map;
  the binary does, and a bad value fails the rollout (the new pod exits at boot, naming the
  variable) instead of being a silent no-op.
- **Only new raises change.** A held order raised before the map was enabled already exists in
  order-management without a site, and it stays that way. And the body of `POST /orders` is
  hashed by order-management's idempotency middleware (ADR 0029 has the same window): a raise
  that was sent without a site and is retried after the map is enabled carries a different body
  under the same key and is refused `idempotency-key-reused` (409 in the pinned order-management
  spec). It can only touch an order in flight at the moment of the rollout and needs no code.
- A site that is mapped to the wrong building is the operator's mistake and is not detectable
  here: this service mirrors the shape of a site code and never looks one up (ADR 0029).
- The mapping is one-way and per process. A second network node served by a second building is a
  second pair in the same variable.
