---
id: 0013-product-translation-file-acl-dictionary
slug: /adr/0013-product-translation-file-acl-dictionary
title: "13. PRODUCT_TRANSLATION_FILE loads the ACL's product dictionary"
sidebar_label: "13. PRODUCT_TRANSLATION_FILE ACL dictionary"
sidebar_position: 13
description: "ADR 0013 — the Anti-Corruption Layer's network-identifier-to-local-SKU dictionary is loaded from PRODUCT_TRANSLATION_FILE at boot into an in-memory lookup; an unset file leaves the dictionary empty and every order is correctly, but silently, rejected as untranslatable."
---

# 13. PRODUCT_TRANSLATION_FILE loads the ACL's product dictionary

## Status

Accepted — implemented in the same change that introduced this record
(`cmd/netfulfil/main.go` `loadProductTranslation`,
`internal/adapters/outbound/memory` `NewProductTranslation` /
`LoadProductTranslationFile`).

## Context

The network's vocabulary stops at the ACL boundary
(`docs/adr/0001-network-fulfillment-bounded-context.md` Hard rule 1): a
network-side product identifier must be translated to a local SKU before
`ReceiveNetworkDemand` ever constructs a `NetworkOrder`. That translation
table changes per deployment/catalog and is operational data, not code —
hardcoding it would mean a code change (and redeploy) for every catalog
update, and checking it into the repo would put retailer-specific product
identifiers in version control.

## Decision

`ports.ProductTranslation` is backed by an in-memory lookup
(`memory.NewProductTranslation`) populated once at boot from the file path
in `PRODUCT_TRANSLATION_FILE`
(`loadProductTranslation` in `cmd/netfulfil/main.go`) via
`memory.LoadProductTranslationFile`. If the env var is unset, the
dictionary stays empty and boot continues — rejecting demand as
untranslatable is itself correct behaviour for unknown products, so an
empty dictionary cannot be distinguished from "no bad products yet" by its
effects alone. Because of that, an unset `PRODUCT_TRANSLATION_FILE` is
logged at **`WARN`**, not `INFO`, specifically calling out that *every*
order will be rejected as untranslatable: a deployment that forgot to
mount the file looks alive and is lying about why nothing is flowing, and
the only way to make that debuggable is a loud startup log line. A
successful load logs the file path and the number of products loaded.

## Consequences

Easier: the ACL's product dictionary is swappable per deployment/catalog
without a code change, and a missing dictionary is visible in startup
logs instead of manifesting only as a wall of rejected orders. Harder: the
dictionary is loaded once at boot — a catalog update today requires a
restart to pick up, there is no hot-reload or Kafka-fed refresh path (that
would be its own ADR if the catalog starts changing often enough for a
restart-per-update to be unacceptable).
