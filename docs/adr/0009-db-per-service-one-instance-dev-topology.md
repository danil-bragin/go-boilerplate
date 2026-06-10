# ADR 0009 — Database-per-service, one Postgres instance (dev topology)

**Status:** Accepted  
**Date:** 2026-06-10

## Context

Each service owns its schema: own tables, own migrations, own pool, no cross-service joins (cross-service reads go through Kafka events). The question is physical placement: one Postgres instance with one logical database per service, or one instance per service.

## Decision

**Logical isolation always; physical isolation is a deployment choice.** The repo's compose stack runs **one Postgres instance** with four databases (`deploy/postgres/init.sql`) — that is explicitly the **dev topology**. Nothing in the code assumes co-location: every service reaches its database only through its own `PG_DSN`, so pointing services at separate instances is a pure config change.

**Production recommendation:** separate instances (or a managed Postgres per service — RDS/Cloud SQL/Aurora) for any service with independent scaling, blast-radius, or compliance needs. At minimum, separate the gateway projection DB from the order-of-record DBs before taking real traffic.

## Consequences

- Dev stays one container; production isolation costs only DSN changes.
- **Shared-instance hazards to keep in mind while on the dev topology:** one noisy service can exhaust shared connections/IO; a single instance is a single failure domain for all four services; `cmd/migrate -service all` is only convenient *because* of this topology.
- **Backups:** on the shared instance, a physical backup (basebackup/WAL) captures all services at one instant — convenient, but restoring it rolls back *every* service together. With per-service instances, each service gets independent PITR windows and restore decisions, at the cost of per-instance backup configuration. Outbox/inbox tables live inside each service's database on purpose: a restore stays internally consistent with the service's domain rows (see the backup/DR runbook in `docs/operations.md` for the re-publish caveat).
