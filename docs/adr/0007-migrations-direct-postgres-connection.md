---
id: 0007-migrations-direct-postgres-connection
slug: /adr/0007-migrations-direct-postgres-connection
title: "7. Run golang-migrate against a direct Postgres connection, not PgBouncer"
sidebar_label: "7. Migrations bypass PgBouncer"
sidebar_position: 7
description: "ADR 0007 — Phase 4 fleet-wide finding, ported from order-management's reference PR (ADR-0029): golang-migrate's postgres driver takes a session-scoped pg_advisory_lock to serialize concurrent migration runs, which is incompatible with PgBouncer's transaction-pooling mode (warehouse-infra PR #43). Two or more network-fulfillment replicas (cmd/netfulfil or cmd/mcp) starting concurrently -- an HPA scale-out, or an ordinary rolling deploy -- crash-loop until one wins the advisory-lock race. Fix: a second env var, MIGRATIONS_DATABASE_URL, carries a direct (non-pooled) connection string used ONLY for the migration step; DATABASE_URL/the runtime pgxpool is untouched and keeps going through PgBouncer. warehouse-infra PR #44 provisions the secret key for network-fulfillment's own (non-shared) Terraform wiring."
---

# 7. Run golang-migrate against a direct Postgres connection, not PgBouncer

## Status

Accepted — implemented in the same change that introduces this record.
Ports `order-management`'s reference PR #115 (its own ADR-0029) to this
repo's binary set and chart shape, following the same pattern already
used for ADR 0005 (Kafka Hash balancer) and ADR 0006 (HPA + pgxpool
tuning): a fleet-wide fix found in order-management first, then fanned
out to the other 8 OLTP services. `warehouse-infra` PR #44 already
provisions the `MIGRATIONS_DATABASE_URL` secret key for all 9 OLTP
services, including this one, in one pass — this record and its
accompanying code/chart change are this repo's half of that fan-out.

## Context

`warehouse-infra`'s PgBouncer rollout (PR #43, Phase 3) repointed this
service's `DATABASE_URL` secret (`kubernetes_secret.network_fulfillment_db`
in `terraform/network-fulfillment.tf` — network-fulfillment's Terraform
wiring is a standalone resource, not the shared
`kubernetes_secret.service_db[each.key]` map the other 8 OLTP services
use, but it carries the identical two keys) at PgBouncer
(`terraform/pgbouncer.tf`), in **transaction-pooling** mode
(`pool_mode = "transaction"`). That is the correct mode for this
service's steady-state OLTP traffic.

What PR #43 did not carve out: **migrations**. Both of this repo's
Postgres-backed composition roots run golang-migrate's postgres driver
(`github.com/golang-migrate/migrate/v4/database/postgres`) against
`DATABASE_URL` at process startup, before serving any traffic:

- `cmd/netfulfil/main.go`'s `wireOrders` (the OLTP HTTP service + inbound
  poller)
- `cmd/mcp/main.go`'s `buildOrders` (the MCP server, a second independent
  deployable over the same schema)

`cmd/netfulfil-projector/main.go` and `cmd/netfulfil-reports/main.go` are
**not affected**: both read `ANALYTICS_DATABASE_URL`, which
`warehouse-infra`'s `analytics_database_urls` local already points
directly at Postgres (never PgBouncer, for the unrelated reason of low
QPS) — this ADR's fix has nothing to add there.

golang-migrate's postgres driver calls `SELECT pg_advisory_lock($1)` to
serialize concurrent migration runs — by design, so two processes
starting at once and racing the same migration don't corrupt the schema.
`pg_advisory_lock` is **session-scoped**: the lock is held by whichever
physical backend connection issued it. PgBouncer's transaction-pooling
mode does not preserve that mapping — each statement in a client's
logical session can be routed to a different physical backend
connection, because the backend connection is returned to the pool the
instant its transaction commits. So two `network-fulfillment` replicas
(any two of `netfulfil`, or two of `mcp`, or one of each against the same
database) starting concurrently can each get a different physical
backend connection mid-"session", the advisory lock never behaves as a
real mutex, and the losing replica's migration statements land on a
backend connection with unexpected transaction/prepared-statement state
— `pq: unnamed prepared statement does not exist` / `pq: canceling
statement due to statement timeout` — crash-looping for roughly 1-2
minutes until the race resolves.

This is the same **latent, fleet-wide, production-blocking bug**
order-management's ADR-0029 found and fixed first: it fires on any
ordinary rolling ArgoCD deploy with more than 1 replica, and on every
HPA scale-out event (ADR 0006 here already provisions `api`'s HPA,
default-disabled but ready to enable). It blocks safely enabling that
HPA fleet-wide, exactly as it blocked order-management's own ADR-0026
HPAs.

## Decision

Give this service a **second** connection string,
`MIGRATIONS_DATABASE_URL` — a direct (non-pooled, session-mode) Postgres
connection string, same user/password/dbname as `DATABASE_URL`, pointed
at Postgres itself rather than PgBouncer — used **only** for the
golang-migrate startup step in both `wireOrders` (`cmd/netfulfil`) and
`buildOrders` (`cmd/mcp`). `DATABASE_URL` and the pgxpool built from it
are completely unchanged in both binaries: every request either process
serves still goes through PgBouncer in transaction-pooling mode, exactly
as PR #43 set up.

`warehouse-infra` PR #44 provisions `MIGRATIONS_DATABASE_URL` as a new
key alongside the existing `DATABASE_URL` key in
`kubernetes_secret.network_fulfillment_db` (this service's own,
non-shared Terraform resource — verified directly against the live
secret below, not assumed from the PR's summary). This repo's
`cmd/netfulfil/main.go` and `cmd/mcp/main.go` now read
`MIGRATIONS_DATABASE_URL` for the migration step only:

```go
// cmd/netfulfil/main.go's wireOrders
migrationsDatabaseURL := databaseURL
if v := os.Getenv("MIGRATIONS_DATABASE_URL"); v != "" {
    migrationsDatabaseURL = v
}
...
postgres.RunMigrations(migrationsDatabaseURL, migrationsPath()) // migrations only
pool, err := postgres.NewPool(ctx, databaseURL) // unchanged, still PgBouncer
```

```go
// cmd/mcp/main.go's run/buildOrders
migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
orders, closeOrders, err := buildOrders(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
```

The fallback to `databaseURL` when `MIGRATIONS_DATABASE_URL` is unset
keeps every environment that doesn't provision the split — local dev, CI
integration tests, or a cluster whose Terraform predates this fix —
working exactly as before, byte-for-byte.

`charts/network-fulfillment`: a new `database.migrationsExistingSecretKey`
value (default `"MIGRATIONS_DATABASE_URL"`) renders the env var in both
the `api` (`templates/deployment.yaml`) and `mcp`
(`templates/mcp-deployment.yaml`) Deployments, sourced from the same
`database.existingSecret`, with `optional: true` on the `secretKeyRef` so
a secret that predates this key still starts the pod.

### Why network-fulfillment's Terraform wiring differs, and why it doesn't change this fix

Unlike the other 8 OLTP services (`kubernetes_secret.service_db[each.key]`
in `terraform/postgres.tf`, driven by `local.services`/`local.database_urls`
/`local.direct_database_urls`), network-fulfillment's database Secret is
its own standalone resource,
`kubernetes_secret.network_fulfillment_db` in
`terraform/network-fulfillment.tf`, backed by its own
`local.network_fulfillment_database_url` /
`local.network_fulfillment_migrations_database_url` locals. PR #44 added
the `MIGRATIONS_DATABASE_URL` key to **both** shapes in the same commit —
this repo's chart and Go code don't need to know or care which Terraform
pattern provisioned the Secret they read; `database.existingSecretKey` /
`database.migrationsExistingSecretKey` name the same two keys either way.

### Why not just make PgBouncer's pool_mode session for this fleet?

Rejected, for the identical reason order-management's ADR-0029 rejected
it: session pooling would fix the advisory-lock problem but throws away
the entire point of PgBouncer for this fleet's OLTP traffic.

### Why not remove the advisory lock / run migrations from one leader replica only?

Rejected, for the identical reason order-management's ADR-0029 rejected
it: the advisory lock is the right mechanism given a session-scoped
connection — the bug is the pooling-mode mismatch, not the mechanism —
and an init-container/Job alternative is a bigger architectural change
for the same outcome this two-line fallback already achieves.

## Consequences

- **Fixes** the crash-loop bug for both of this service's
  Postgres-backed composition roots (`cmd/netfulfil`, `cmd/mcp`).
- **No runtime behavior change**: `DATABASE_URL` is untouched in both
  binaries, so request-serving connection pooling and PgBouncer's own
  configuration are unaffected.
- **No behavior change for environments without the split**: the
  fallback means local dev and CI integration tests keep using
  `DATABASE_URL` for everything, exactly as before.
- **No change needed for the analytics pair** (`netfulfil-projector`,
  `netfulfil-reports`): `ANALYTICS_DATABASE_URL` was never routed through
  PgBouncer.
- **Unblocks ADR 0006's `api` HPA**: an HPA scale-out is exactly the "2+
  replicas start concurrently" trigger for this bug.
- One more secret key to keep in sync going forward; mechanically
  generated by Terraform (both the shared and this service's own
  standalone resource), so there is no new per-service manual step.

## Verification

Confirmed directly against the live `kind-warehouse` cluster — not
assumed from warehouse-infra PR #44's summary, per this task's own
instruction that network-fulfillment's non-shared Terraform wiring be
checked directly:

```
$ kubectl get secret network-fulfillment-db -n warehouse-systems -o jsonpath='{.data}' | jq keys
[
  "DATABASE_URL",
  "MIGRATIONS_DATABASE_URL"
]
```

Both keys decode to `network_fulfillment` DSNs with identical
user/password/dbname, differing only in host:port: `DATABASE_URL` still
points at `pgbouncer.warehouse-data.svc.cluster.local:6432`;
`MIGRATIONS_DATABASE_URL` is new and points directly at
`postgres-postgresql.warehouse-data.svc.cluster.local:5432` — the same
shape order-management's ADR-0029 verified for its own secret.

Local quality gate (this repo's own commands, mirroring order-management's
`make check`/`make check-all`/`make integration`/`helm lint` verification):
`make check`, `make check-all`, `make integration`, `helm lint` all pass
(see this PR's description for the actual run output). `go test
./cmd/netfulfil/... -race` includes the two new regression tests added by
this change: `TestMigrationsDatabaseURLFallback` (the env-var fallback
both directions) and
`TestWireOrders_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations`
(`wireOrders` itself threads `MIGRATIONS_DATABASE_URL`, not
`DATABASE_URL`, into the migration step).

This change was not live-verified with a forced concurrent-replica
scale-out against the cluster's ArgoCD Application (order-management's
ADR-0029 did this for the reference implementation); the chart and Go
wiring were confirmed to render/compile/test correctly, and the secret
key's existence and shape were confirmed live, but a scale-out
reproduction specifically for network-fulfillment was out of scope for
this fan-out PR.
