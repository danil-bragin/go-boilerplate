# AWS hosting notes (condensed)

Service-by-service mapping for running this stack on AWS, with the
non-obvious gotchas that drove each pick. Pair with `docs/operations.md`
(scaling guide, lifecycle contracts) and `deploy/k8s/`.

## Kafka → MSK **Provisioned**, not Serverless

The platform leans on transactional producers / EOS boundaries (ADR-0006)
and admin-API topic tooling. MSK **Serverless** caps partition counts and
its support for transactional/EOS workloads is unverified for this stack —
do not assume `franz-go` EOS semantics hold there. MSK Provisioned is plain
Apache Kafka: everything here works unchanged. Set `ENSURE_TOPICS=false`
and manage topics as IaC; `TOPIC_RF=3`.

## Postgres → RDS/Aurora, **no RDS Proxy**

RDS Proxy *pins* any connection that uses session state — and this codebase
uses **session-level advisory locks** by design: migrations
(`pg.Migrate`), outbox single-active relay, and leader-elected workers all
hold `pg_advisory_lock` on a long-lived session. Behind RDS Proxy those
sessions pin 1:1 to backend connections (no multiplexing win) and risk
surprise lock loss on proxy failover. Use direct connections with the
per-replica pool budget (`PG_MAX_CONNS`, see operations.md connection
arithmetic) or self-managed PgBouncer in **session** mode for those paths /
transaction mode only for the request pools (compose `pgbouncer` profile
shows the wiring; `PG_MIGRATE_URL` must stay direct).

## Redis → ElastiCache **Valkey, node-based**, not Serverless

ElastiCache Serverless restricts `CLIENT TRACKING` (rueidis client-side
caching — the cache L1 invalidation backbone) and pattern subscriptions
(`PSUBSCRIBE`). Node-based Valkey/Redis-OSS supports both. Related design
note: the SSE streamer broadcasts on a **single non-pattern channel**
(`orders:status`) precisely so it never depends on `PSUBSCRIBE`/keyspace
notifications — keep it that way when adapting.

## Load balancer → ALB, SSE settings matter

- **Idle timeout ≥ 300 s** (default 60 s is survivable with the gateway's
  15 s heartbeats, but a generous margin avoids mid-stream cuts on
  heartbeat hiccups).
- **Deregistration delay** ≥ `DRAIN_GRACE` + a few seconds: the gateway
  closes SSE streams server-side on SIGTERM (clients auto-reconnect via
  `Last-Event-ID`), so a long delay only postpones rollout; too short cuts
  in-flight request drains.
- ALB buffers are fine for `text/event-stream` (no special flag needed);
  TLS terminates here (the stack is plaintext inside — see ARCHITECTURE.md).

## Compute → EKS; scale projection with KEDA on MSK lag

`deploy/k8s/` manifests map 1:1 (probes on the admin port, preStop =
`DRAIN_GRACE`, `GOMEMLIMIT` from limits). Split the projection
(ADR-0008) and drive `cmd/projection` replicas with KEDA's Kafka scaler on
consumer-group lag (`gateway-projection`); the partition count stays the
parallelism ceiling. HPA on CPU is enough for the stateless edge.

## Identity → Keycloak stays

Cognito's token customization, realm import, and standards coverage don't
match what the gateway's JWKS/azp/RBAC setup assumes. Run Keycloak on EKS
(or use the JWKS envs against any OIDC IdP); nothing in the code is
Keycloak-specific beyond the realm export.

## Observability → AMP + AMG + ADOT; X-Ray speaks OTLP

Prometheus-pull `/metrics` scrapes into Amazon Managed Prometheus via the
ADOT collector (drop-in for the otel-collector config); dashboards/alerts
import into Amazon Managed Grafana. Traces: point
`OTEL_EXPORTER_OTLP_ENDPOINT` at ADOT — X-Ray now ingests OTLP directly,
or keep an OSS backend (Jaeger/Tempo) unchanged.
