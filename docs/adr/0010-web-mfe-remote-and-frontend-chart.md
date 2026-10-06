---
id: 0010-web-mfe-remote-and-frontend-chart
slug: /adr/0010-web-mfe-remote-and-frontend-chart
title: "10. web/ is a Module Federation remote, served by its own chart Deployment"
sidebar_label: "10. web/ MFE remote + frontend chart"
sidebar_position: 10
description: "ADR 0010 — the context's operator-facing SPA (web/) is built as a Module Federation remote (netfulfil_mfe) rather than a standalone app, and is deployed by the chart's own stateless frontend Deployment/Service behind warehouse-infra's Nginx web gateway."
---

# 10. web/ is a Module Federation remote, served by its own chart Deployment

## Status

Accepted — implemented in the same change that introduced this record
(`web/`, `charts/network-fulfillment/templates/frontend-deployment.yaml`,
`frontend-service.yaml`).

## Context

`network-fulfillment` needed an operator-facing UI (viewing `NetworkOrder`
status, acknowledgement deadlines, reconciliation state) without forcing
every fleet context's operators to juggle N unrelated single-page apps on N
different ports. Two options were weighed: (1) ship `web/` as a fully
standalone SPA with its own top-level navigation and auth-less direct
exposure, or (2) build it as a remote that a fleet-wide shell host composes
at runtime. A standalone app is simpler to build in isolation but means an
operator has to know this context's URL by heart and there is no shared
chrome/navigation across contexts — every team reinvents the same shell.

## Decision

`web/` is a Vite + Module Federation **remote** named `netfulfil_mfe` (see
`web/vite.config.ts`), exposing its routes/components for a fleet shell
host to consume, while still being independently buildable and runnable
standalone for local development (`web/package.json` dev script). It is
packaged as its own `nginx-unprivileged` container (`web/Dockerfile`,
`web/nginx.conf`) and deployed by the chart's own `frontend` Deployment and
Service (`charts/network-fulfillment/templates/frontend-deployment.yaml`,
`frontend-service.yaml`), reached through warehouse-infra's Nginx web
gateway rather than given its own public ingress host. The chart exposes
`frontend.*` values (image repository/tag, replica count, resources,
autoscaling under `autoscaling.frontend.*`) following the same shape as the
API Deployment. The frontend workload is purely static-asset serving —
no server-side session, no per-request state — so `autoscaling.frontend`
defaults the same way as the API (`enabled: false`, `minReplicas: 1`,
`maxReplicas: 3`, `targetCPUUtilizationPercentage: 70`) per
`charts/network-fulfillment/values.yaml`.

## Consequences

Easier: a fleet shell can mount `netfulfil_mfe` next to other contexts'
remotes with shared chrome and auth, and the frontend scales and deploys
independently of the API process. Harder: the remote's exposed module
surface becomes a cross-context contract (a breaking change to exposed
routes/props affects whatever shell consumes it) that this ADR does not
yet version — a future ADR should assign the Module Federation remote
contract the same discipline this repo gives Kafka events (ADR 0008) if
more than one shell starts consuming it.
