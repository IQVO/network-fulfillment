---
id: 0011-boot-retry-for-istio-first-dial-reset
slug: /adr/0011-boot-retry-for-istio-first-dial-reset
title: "11. Retry the first outbound dial at boot to survive the Istio sidecar race"
sidebar_label: "11. Boot-time dial retry (Istio)"
sidebar_position: 11
description: "ADR 0011 — cmd/netfulfil retries its first database migration/ping dial with backoff instead of failing immediately, because this fleet's Istio native sidecars reset a pod's first outbound connection roughly 10 seconds after the app container starts."
---

# 11. Retry the first outbound dial at boot to survive the Istio sidecar race

## Status

Accepted — implemented in the same change that introduced this record
(`cmd/netfulfil/retry.go`, tested in `cmd/netfulfil/retry_test.go`).

## Context

This fleet's Istio native sidecars are not guaranteed to have their
iptables redirect and upstream connection ready the instant the app
container's process starts; empirically the sidecar resets the pod's
**first** outbound TCP dial roughly 10 seconds in
(`read: connection reset by peer`). `cmd/netfulfil` dials Postgres exactly
once at boot to run migrations and ping the connection before accepting
traffic. A single failed attempt there was being treated as fatal,
producing `CrashLoopBackOff` on a perfectly healthy database purely
because of sidecar startup ordering — indistinguishable, from the pod's
exit code alone, from a genuinely unreachable database.

## Decision

Boot-time dial attempts (run-migrations, ping) go through `retryWithDelay`
/ `retry` (`cmd/netfulfil/retry.go`): a small bounded number of attempts
(`bootRetries`) with an exponential backoff delay, starting short enough
that a transient first-dial reset is absorbed within a few seconds. A
success on attempt 2+ is logged at `INFO` (not silently swallowed — an
operator should be able to see the sidecar race happening in logs). If
every attempt fails, the fail-closed rule from `DATABASE_URL` configured
is preserved exactly: the function returns the **last real error**
wrapped with the attempt count, never a generic timeout, so a truly
unreachable database still refuses to boot and still reports why
(`TestRetry_GivesUpAndReportsTheLastError`).

## Consequences

Easier: pods no longer crash-loop on Istio sidecar startup timing;
operators get a visible log line when the retry saved the boot instead of
a silent extra few seconds. Harder: a genuinely flaky or slow Postgres now
takes a few seconds longer to be correctly reported as unreachable, and
the retry count/backoff constants are a manually-tuned guess at the
sidecar race window rather than derived from a readiness signal — if
Istio's startup behaviour changes, `bootRetries`/backoff in
`cmd/netfulfil/retry.go` may need revisiting.
