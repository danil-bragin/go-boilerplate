# Operations

Runtime tuning, runbooks (DLT redrive, projection rebuild, migrations,
backup/DR), scaling guidance, and the Kubernetes reference manifests.

---

## Compose profiles

The `docker-compose.yml` is split into three profiles so every developer runs
only the services they actually need.

### Profile matrix

| Profile | Services | Command |
|---|---|---|
| _(none — core)_ | postgres, redpanda, redpanda-console, redis, seaweedfs, seaweedfs-setup, keycloak | `just up` |
| `observability` | core + otel-collector, jaeger, prometheus, grafana, pyroscope | `just up-obs` |
| `apps` | core + gateway, orders, payments, notifications | `just up-apps` |
| `observability` + `apps` | Everything | `just up-full` |

### Notes

- **Core is always included.** Profile-less services (postgres, redpanda, redis,
  seaweedfs, keycloak) start with any `docker compose up` invocation regardless
  of which profiles are active.
- **Apps are independent of observability.** The four application services
  (`gateway`, `orders`, `payments`, `notifications`) do not `depends_on` any
  observability service.  When the observability profile is absent, the apps
  still start and run normally — OTLP export failures are non-fatal; traces
  and metrics are simply uncollected.
  > **Note:** running `--profile apps` without `--profile observability` is
  > fully supported, but each service will log periodic OTLP export errors
  > because the `otel-collector` hostname is not resolvable. These errors are
  > non-fatal and can be ignored in local development. Run `just up-full`
  > (`--profile observability --profile apps`) to get collected telemetry.
- **`just down` stops everything** (`--profile observability --profile apps`)
  regardless of which profiles were originally used to start the stack, and
  removes volumes (`-v`).

### Dev workflow

```bash
# Option A — full stack (everything running in containers)
just up-full
# Edit code, rebuild a single service:
docker compose --profile apps up -d --build gateway

# Option B — local development (run one service on the host, rest in containers)
just up          # starts only core infra
go run ./examples/gateway/cmd/gateway  # runs against postgres/redpanda/redis/seaweedfs/keycloak on localhost
# or with hot-reload:
just dev gateway

# Option C — core + observability (no app containers; run services via go run)
just up-obs
just dev gateway  # OTLP traces sent to otel-collector on localhost:4317

# Tail logs from whatever is running
just logs

# Tear down
just down
```

---

This document explains the container runtime knobs applied to every application
service in this repo and the reasoning behind the chosen values.

---

## GOMAXPROCS — match the container CPU quota

**Problem.** The Go runtime defaults `GOMAXPROCS` to the number of logical CPUs
visible to the process, which inside a container is the **host** CPU count.  If
the container is given a fractional CPU quota (e.g. `cpus: "0.5"`) the Linux
CFS scheduler throttles the container when it exceeds the quota, causing
stop-the-world GC pauses and tail-latency spikes that are hard to diagnose.

**Solution.** `platform/servicekit/main.go` blank-imports
`go.uber.org/automaxprocs`:

```go
import _ "go.uber.org/automaxprocs"
```

The import lives in `servicekit.Main` — the single process entry point every
service binary goes through — so one import sizes every binary correctly.

`automaxprocs` reads the cgroup CPU quota at startup and calls
`runtime.GOMAXPROCS` with the correct value (e.g. quota=1.0 → GOMAXPROCS=1).
Go 1.25+ also does this natively when `GOMAXPROCS` is unset, so `automaxprocs`
is belt-and-suspenders that ensures the right behaviour across all supported
toolchains.

**Recommendation.** Use whole-number CPU limits (1.0, 2.0, …) in
`deploy.resources.limits.cpus` / Kubernetes `resources.limits.cpu` to avoid
fractional CFS throttling artefacts.  Fractional limits such as `"0.5"` are
valid but harder to reason about; prefer dedicated cores and scale horizontally.

---

## GOMEMLIMIT — prevent OOM-kill

**Problem.** By default the Go GC targets a heap-growth ratio (controlled by
`GOGC`) but places no absolute upper bound on heap size.  Inside a cgroup the
Linux OOM-killer fires when the container's resident-set size exceeds
`memory.limit_in_bytes`, which can happen before the GC has had a chance to
collect.

**Solution.** Set `GOMEMLIMIT` (introduced in Go 1.19) to a value slightly
below the container memory limit so the GC aggressively trims the heap before
the hard limit is hit:

```
GOMEMLIMIT ≈ 0.90 × container_memory_limit
```

The docker-compose services use:

| Service | `memory` limit | `GOMEMLIMIT` |
|---|---|---|
| gateway | 512 MiB | 460 MiB |
| orders | 512 MiB | 460 MiB |
| payments | 512 MiB | 460 MiB |
| notifications | 512 MiB | 460 MiB |

These are **demo values**.  Size the memory limit to your measured steady-state
RSS + headroom for bursts, then set `GOMEMLIMIT` to 90% of that.

**Why 90%?** The 10% margin accounts for non-heap allocations (stack frames,
off-heap cgo, goroutine metadata, mmap regions) that count toward the cgroup
RSS but are invisible to the Go GC.  A tighter value (e.g. 95%) increases GC
pressure; a looser value (e.g. 80%) wastes RAM.  90% is the community-accepted
rule of thumb.

**Setting `GOMEMLIMIT` in Kubernetes:**

```yaml
env:
  - name: GOMEMLIMIT
    valueFrom:
      resourceFieldRef:
        resource: limits.memory
        divisor: "1"   # returns bytes; Go runtime accepts bare-byte values
```

Or set it statically as a string: `value: "920MiB"` for a 1 GiB limit.

---

## CPU limits — avoid CFS throttling

Linux CFS (Completely Fair Scheduler) implements CPU quotas with a
quota/period pair (e.g. 100 ms quota per 100 ms period = 1.0 CPU).  A
container that exhausts its quota mid-period is throttled until the next
period, causing latency spikes even if the host has idle cores.

**Recommendations:**

1. Prefer **whole-number** CPU limits (1.0, 2.0, …).  Fractional limits such
   as `"0.5"` work but produce shorter throttle windows that are harder to
   tune.
2. Set `request ≈ limit` in Kubernetes to place the container in the
   `Guaranteed` QoS class and avoid noisy-neighbour throttling.
3. Profile under realistic load before tightening limits.  The
   `GOMAXPROCS` value set by `automaxprocs` will adapt automatically.

---

## Environment namespacing

Each service runs in its own container and therefore has its own environment
namespace.  There is no need for per-service env-variable prefixes: the
`gateway` container has its own `PG_DSN`, the `orders` container has its own
`PG_DSN`, and so on.

Configuration is read from the environment exactly once, inside each
service's `NewApp` (`config.Load`). To run multiple services in a single
process (e.g. an integration test), set each service's variables with
`t.Setenv` immediately before calling its `NewApp` — later assignments to the
same variable (`PG_DSN`, `KAFKA_BROKERS`, …) do not affect services that were
already constructed. See `examples/e2e/e2e_test.go` for the pattern. Note
that `t.Setenv` disallows `t.Parallel()` in the same test, which is the right
constraint here: the wiring sequence is inherently order-dependent.

---

## Logging

Every service logs structured JSON to **stdout** (`LOG_LEVEL`, `LOG_FORMAT`);
the container runtime / log shipper owns collection. Context-aware calls
(`InfoContext` etc.) inside a span carry `trace_id`/`span_id` fields for
trace correlation.

**OTLP log export (opt-in):** set `TELEMETRY_LOGS=true` to additionally ship
log records to the OTel collector over OTLP/gRPC (`OTEL_EXPORTER_OTLP_ENDPOINT`,
same endpoint as traces/metrics). Mechanics:

- `telemetry.SetupAll` constructs an `otlploggrpc` exporter + SDK
  `LoggerProvider` (batched); servicekit wires it into `log.New` via
  `log.WithOTelBridge`, which fans every record out to stdout **and** the
  collector — stdout always stays the primary sink, and a down collector
  never blocks logging (export is best-effort, flushed on shutdown).
- Records emitted inside a span carry native OTLP trace correlation
  (trace/span IDs on the log record itself), so a logs backend can join them
  to Jaeger traces without parsing JSON fields.
- The collector's `logs` pipeline (`deploy/otel-collector.yaml`) currently
  ends in the `debug` exporter; point it at Loki/Elastic/etc. when a log
  backend joins the stack.

Default is `false`: with no log backend in the compose stack, exporting to
the collector only duplicates stdout into the collector's own stdout.

---

## Per-IP rate limiting (gateway)

The gateway applies a per-client-IP token-bucket rate limiter at the edge. Configure via:

| Variable | Default | Description |
|---|---|---|
| `RATELIMIT_RPS` | `50` | Sustained token refill rate (requests per second per IP) |
| `RATELIMIT_BURST` | `100` | Maximum burst depth per IP |
| `RATELIMIT_REDIS` | `false` | Use Redis-backed distributed limiter (requires `REDIS_ADDRS`) |
| `TRUSTED_PROXIES` | _(empty)_ | Comma-separated CIDRs for trusted reverse proxies (e.g. `10.0.0.0/8`). When set, `X-Forwarded-For` is consulted for client-IP extraction. |

**Memory vs Redis:** The default in-memory limiter is process-local. For multi-replica deployments set `RATELIMIT_REDIS=true` so all instances share a single Redis-backed counter. If Redis is unavailable the gateway falls back to in-memory (graceful degradation, WARN logged). The distributed limiter uses Redis server time (via `TIME` inside the Lua script) so all replicas share a single clock — immune to wall-clock skew between application instances.

**Trusted proxies:** XFF is ignored unless `TRUSTED_PROXIES` is set. Invalid CIDRs cause the gateway to refuse to start (fail-fast). Use network-level trust only — never trust an IP that end-users can set.

---

## JWKS outage semantics (gateway auth)

When `GATEWAY_AUTH_DISABLED=false`, the gateway verifies bearer JWTs against the IdP's JWKS. The key set is fetched once at startup (startup FAILS fast if the initial fetch cannot complete) and then cached and refreshed in the background (`jwk.Cache`). During an IdP/JWKS outage:

- Tokens verifiable with the **cached** keys keep working — a short JWKS outage is invisible to clients.
- If verification needs a key fetch that fails (e.g. unknown `kid` after a key rotation mid-outage), the request is rejected with **401** and the generic detail `authentication failed`. This is **by design**: the edge never fails open, and infrastructure details (JWKS URL, network errors) are never echoed to clients.
- The real cause is logged at **ERROR** (`auth: token verification failed with non-token error`). Alert on a sustained rate of this log line — a spike means JWKS trouble, not bad client tokens (those produce plain `invalid token` 401s with no ERROR log).

---

## Kafka retry tiers & DLT redrive runbook

The `platform/messaging/retry` package implements tiered retry routing.
Tier topics are **index-named**: `<base>.retry.0`, `<base>.retry.1`, … — the
tier's delay travels in the `retry-due-at` record header, never in the topic
name, so retry policies can be retuned without stranding in-flight records.
The consumer group for all of a service's retry tiers is `<group>.retry`.
After the last tier a record lands on `<base>.DLT`.

### Day-2 checks

| Step | Action |
|---|---|
| Check tier lag | `rpk group describe <group>.retry` (or `kafka-consumer-groups.sh --describe --group <group>.retry`) — lag per `.retry.<idx>` topic |
| Check DLT depth | `rpk topic describe orders.commands.DLT -p` (high watermarks) — alert when > 0 for longer than your triage SLO |
| Tune tiers | Edit the `retry.Policy` passed to `AddConsumerWithRetry` and redeploy. Topic names do not change (index-named); in-flight records keep their original `retry-due-at` |
| Size key parking | `retry.Policy.KeyParkingWindow` must be ≥ `Tiers[0]` + the retry consumer's redelivery lag (poll interval + processing), or the key un-parks before the escalated record is redelivered and per-key order breaks anyway. For `DefaultPolicy` (tier-0 = 5s) use ~10s as the floor |

### DLT redrive procedure

1. **Inspect** what dead-lettered and why (the last error is on the record):

   ```bash
   rpk topic consume orders.commands.DLT --format json -n 10 \
     | jq '{key: .key, headers: (.headers | map({(.key): .value}) | add)}'
   ```

2. **Fix the underlying cause** (bad deploy, schema mismatch, downstream outage).

3. **Dry-run** the redrive — lists destination, key, message-id per record,
   commits nothing:

   ```bash
   just redrive --dlt orders.commands.DLT --dry-run
   ```

4. **Live run** (add `--limit N` to canary a bounded batch first):

   ```bash
   just redrive --dlt orders.commands.DLT --limit 100
   just redrive --dlt orders.commands.DLT
   ```

Semantics worth knowing (see `cmd/redrive` package docs):

- Each record is republished to the topic named by its `x-original-topic`
  (set by `kafka.WithRetry`) or `retry-orig-topic` (set by the tiered
  escalator) header, with all retry/diagnostic headers stripped — it re-enters
  the pipeline as a clean first attempt. A record with **neither** header
  aborts the run; nothing is guessed and nothing past the previous record is
  committed.
- Progress is tracked in consumer group `redrive` (override `--group`) and
  committed only **after** a successful republish — an interrupted run resumes
  where it left off (at-least-once; duplicates collapse in consumer inboxes).
- `--fresh-ids` mints a new `message-id` per record, **bypassing inbox dedup**
  so side effects run again on purpose. Default (no flag) preserves the
  original id: consumers that already processed the message before it
  dead-lettered skip it via the inbox.
- **Dedup caveat — records without a `message-id` header.** "Consumers dedup
  on redrive" holds only for records that carry a `message-id`. Without one,
  the consumer inbox falls back to `topic:partition:offset` as the identity —
  and the republished record lands at a **new** offset, so the fallback
  identity changes and the side effect **runs again**. Redrive prints a
  `WARN` line per such record and totals them in the run summary
  (`N record(s) without message-id`); review those records before a live run
  if their handlers are not naturally idempotent.

### Record headers reference

| Header | Written by | Meaning |
|---|---|---|
| `message-id` | outbox publisher (row UUID) / gateway (order id) | inbox dedup key; `topic:partition:offset` fallback when absent |
| `event-type` | producer | versioned type, e.g. `orders.OrderCreated.v1`; `consume.Typed` dispatch key |
| `correlation-id` | gateway (chain root) / propagated | constant across one chain; == root command's message id |
| `causation-id` | outbox enqueue (from ctx) | message id of the direct parent message |
| `principal-sub`, `principal-roles` | `auth.InjectHeaders` at the edge | propagated actor for audit; transport metadata, NOT authentication |
| `retry-attempt` | retry escalator | escalations performed so far |
| `retry-orig-topic` | retry escalator | original base topic |
| `retry-due-at` | retry escalator | unix-millis redelivery due time |
| `retry-last-error` | retry escalator | last handler error (truncated to 512 bytes) |
| `x-error`, `x-attempts`, `x-original-topic` | `kafka.WithRetry` on DLT produce | last error / attempt count / source topic for in-process-retry DLTs |

---

## Projection rebuild / replay runbook (gateway read model)

The gateway's `orders_read` projection is derived state — it can be rebuilt
from the event topics, **within topic retention only** (`TOPIC_RETENTION`,
default 168 h; events older than that are gone — DB rows are the source of
truth, ADR-0011). Two distinct situations:

### A. Reconverge / repair (preferred — no data loss window)

Projection upserts are idempotent and reorder-safe (`pending < created <
terminal` precedence), so a full re-consume converges to the same state
without truncating anything. Procedure:

1. Stop the projection consumers (gateway replicas in embedded mode, or the
   `cmd/projection` deployment when split).
2. **Clear the inbox window for the projection group** — this is the critical
   step: replayed records carry their original `message-id`, and any record
   still inside the inbox dedup window (`INBOX_RETENTION`, default 168 h)
   would otherwise be silently skipped:

   ```sql
   DELETE FROM inbox WHERE consumer = 'gateway-projection';
   ```

3. Rewind the consumer group to the start of retention:

   ```bash
   rpk group seek gateway-projection --to start \
     --topics orders.events,payments.events
   ```

4. Restart the consumers and watch lag drain:
   `rpk group describe gateway-projection`.

### B. Rebuild from scratch (schema change in the read model)

Same as A, plus `TRUNCATE orders_read;` between steps 1 and 2. **Accept the
consequence first:** rows whose source events have already aged out of topic
retention are lost from the read model permanently. If that is not acceptable,
backfill `orders_read` from the owning services' databases instead (they are
the system of record), or raise `TOPIC_RETENTION` ahead of planned rebuilds.

### What `cmd/redrive` does and does not do here

`cmd/redrive` reads **DLT topics only** and routes records by their
orig-topic headers — it cannot replay an ordinary event topic (records there
have no `x-original-topic`/`retry-orig-topic` header, and redrive aborts by
design). Its `--fresh-ids` mode exists for the DLT case of this same inbox
caveat: redriving records the consumer already inboxed, when you explicitly
want the side effects to run again. For projection rebuilds use consumer-group
seek (above), not redrive.

---

## Database migrations

Migrations are embedded goose SQL files (`examples/<svc>/internal/migrations/sql`)
applied by `pg.Migrate`, which serializes replicas with a Postgres
session-level advisory lock held on the SAME single connection that runs the
goose statements.

### Who runs them

| Mode | How | When to use |
|---|---|---|
| On startup (default) | `MIGRATE_ON_START=true` — servicekit applies migrations in `New` | dev, tests, small single-team deploys |
| Migrate job (prod) | `MIGRATE_ON_START=false` on app replicas; run `just migrate <svc>` / `go run ./cmd/migrate -service <svc>` as a pre-deploy job (Kubernetes reference: `deploy/k8s/migrate-job.yaml`) | production rollouts — replicas never race a long migration |

### PgBouncer / pooled DSNs

`pg.Migrate` needs a real Postgres SESSION (advisory locks are session-scoped).
If `PG_DSN` points at PgBouncer in transaction-pooling mode, set
`PG_MIGRATE_URL` to a direct-Postgres DSN — `Config.MigrateDSN()` (used by
servicekit and `cmd/migrate`) prefers it automatically.

### Long operations: `-- +goose NO TRANSACTION` + CONCURRENTLY

goose wraps each migration in a transaction. `CREATE INDEX CONCURRENTLY`
cannot run inside one — opt the file out:

```sql
-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS orders_status_idx ON orders (status);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS orders_status_idx;
```

Rules for NO TRANSACTION files: one statement per file where possible, always
idempotent (`IF NOT EXISTS` / `IF EXISTS`) — statements auto-commit
individually, so a midway failure leaves earlier ones applied and goose will
re-run the whole file. The boilerplate's own migrations use plain
`CREATE INDEX` because they index tables created in the same change (empty at
that point); use CONCURRENTLY for any index on a populated production table.

### Linting (squawk)

`just lint-sql` (and the blocking `sql-lint` CI job) runs
[squawk](https://squawkhq.com) over `**/migrations/**/*.sql`. Repo policy in
`.squawk.toml`: rules are evaluated as in-transaction (goose), Down-drop and
dev-only noise rules are excluded, everything else (table rewrites,
lock-heavy ALTERs, volatile defaults, …) is blocking. Suppress a deliberate
violation per-statement with `-- squawk-ignore <rule>` on the line above it,
plus a comment explaining why it is safe.

---

## Backup & disaster recovery

The boilerplate ships a **dev topology** (one Postgres container, one volume —
see ADR-0009); nothing below is wired out of the box. For production:

### What to back up

| Data | System of record? | Backup approach |
|---|---|---|
| Service databases (orders, payments, …) | **YES** (ADR-0011: rows are truth) | Continuous WAL archiving + base backups → PITR (managed Postgres does this for you; self-hosted: pgBackRest / WAL-G) |
| Gateway `orders_read` projection | No — derived | Restore by rebuild/reconverge (runbook above) or restore its DB like any other; cheaper to rebuild |
| Kafka topics | No — transport with finite retention | Do not treat as backup; analytics/archive goes through CDC-to-warehouse (ADR-0011) |
| Keycloak realm, Grafana dashboards | Config | Keep as IaC/exports in git (`deploy/keycloak`, `deploy/grafana`) |

### Restore-ordering concern: the outbox re-publishes after restore

A restored service database contains whatever was in its `outbox` table at the
backup instant — including rows already published between the backup and the
failure, now marked unpublished again (or published rows whose effects already
propagated). When the service starts, the relay will (re-)publish them: **a
restore is a burst of stale, duplicate events.**

Mitigations, in order:

1. **Keep relay startup gated after a restore.** Start the restored service
   with the relay effectively disabled (e.g. a deploy variant that skips
   `AddOutboxRelay`, or network-isolate Kafka) until you have reconciled.
2. **Reconcile the outbox against the topic** before re-enabling: compare the
   newest `published_at IS NOT NULL` rows with the topic's tail (`rpk topic
   consume`), and mark rows that demonstrably made it to Kafka as published
   (`UPDATE outbox SET published_at = now() WHERE id IN (…)`).
3. **Rely on consumer inboxes for the remainder** — duplicates with preserved
   `message-id`s are dropped by every consumer **whose inbox window still
   covers them**. This is the hard constraint: if the restore point is older
   than the consumers' `INBOX_RETENTION` (default 168 h), inbox rows for those
   messages may already be cleaned up and duplicates WILL re-run side effects.
   Keep `INBOX_RETENTION` ≥ your worst-case restore age, or accept and handle
   the replays.

The same reasoning applies to the **inbox** after restore: inbox rows lost to
the restore window mean already-processed messages still on the topic will be
processed again (at-least-once). Handlers are idempotent by design
(inbox + upserts), but money-grade side effects deserve a manual check.

### Recovery order

Restore and verify the **system-of-record service(s)** first (orders,
payments), then notifications, then rebuild/reconverge the gateway projection
last (runbook above) — it derives from the others' events.

---

## Scaling guide (per component)

| Component | Scale how | Hard limits / caveats |
|---|---|---|
| Gateway (HTTP edge) | Horizontal replicas behind the LB; `deploy/k8s/gateway-deployment.yaml` includes an HPA sketch | Stateless. With `RATELIMIT_REDIS=true` the per-IP limit is shared across replicas; in-memory mode multiplies the effective limit by replica count. In embedded-projection mode every replica is also a consumer-group member — see next row |
| Gateway projection / any consumer group | More members in the consumer group — embedded: more gateway replicas; split: scale `cmd/projection` | **Members beyond the partition count idle** (`TOPIC_PARTITIONS`, default 6, is the parallelism ceiling). Repartitioning later is an ops event — size partitions for target parallelism up front. Split the projection (ADR-0008) before scaling the edge aggressively |
| Orders / payments / notifications consumers | Replicas up to partition count per topic | Same partition ceiling. Per-key order is per-partition; tiered retry breaks per-key order unless key parking is enabled (`retry.Policy.KeyParkingWindow`) |
| Outbox relay | Do NOT scale for throughput by default: `OUTBOX_SINGLE_ACTIVE=true` runs one active relay per service (advisory-lock leader); extra replicas are warm standbys (takeover ≤ ~2× poll interval) | The single relay drains the table until empty per tick and batch-publishes; if it still lags, raise `OUTBOX_BATCH_SIZE`, lower `OUTBOX_POLL_INTERVAL`, then consider the LISTEN/NOTIFY tier (ADR-0004 amendment). Setting `OUTBOX_SINGLE_ACTIVE=false` trades per-aggregate ordering for parallel publish — only with reorder-safe consumers |
| Postgres | Per-service instances first (ADR-0009), then read replicas (`PG_READER_DSN` reader pool), PgBouncer in front (set `PG_MIGRATE_URL` for migrations) | Writer is per-service vertical + partitioning territory; the platform's reader/writer split is already plumbed |
| Redis (cache L2 + rate limit) | Standard Redis scaling; rueidis supports clustering | Every cache `Set`/`Delete` publishes an invalidation message; **every app instance subscribes** — pub/sub fan-out grows O(instances × writes). At high replica counts consider shorter L1 TTLs instead of chatty invalidation. L2 outage is absorbed by the circuit breaker (L1-only mode), so Redis HA is a latency/coherence concern, not availability |
| Kafka/Redpanda | Cluster sizing per vendor guidance | Topics here are created with `TOPIC_RF` (default 1 — dev only); production needs RF ≥ 3 and `ENSURE_TOPICS=false` with topics as IaC |

---

## Kubernetes reference manifests

`deploy/k8s/` contains **reference manifests — not production-ready**: a
gateway Deployment/Service/HPA (probes on the admin port, preStop drain
matching `DRAIN_GRACE`, `GOMEMLIMIT` from the memory limit) and a migrate Job
(`MIGRATE_ON_START=false` pattern). They encode the lifecycle contracts this
repo's code actually implements; secrets management, network policies, TLS,
and registry plumbing are deliberately out of scope. Validate with
`kubectl apply --dry-run=client -k deploy/k8s/`.

---

## PGO (profile-guided optimization)

Since Go 1.21, `go build` defaults to `-pgo=auto`: if a file named
`default.pgo` exists in the **main package directory**, the compiler uses it
to guide inlining/devirtualization (typical win: a few percent CPU on hot
services); if the file is absent, the build is a plain non-PGO build. Nothing
in the build pipeline (Dockerfile, goreleaser, CI) needs flags — presence of
the file is the switch, absence can never break a build.

### Producing a profile

Production-shaped profiles come from the continuous profiler: set
`PYROSCOPE_ADDR` on the service (servicekit starts `pyroscope-go`; the
application name is `Telemetry.ServiceName`, i.e. `OTEL_SERVICE_NAME`). Then:

```bash
just pgo-fetch gateway                          # local pyroscope, last 24h
just pgo-fetch gateway http://pyroscope:4040 7d # explicit addr + window
```

The recipe calls the Pyroscope render API and installs the result as the main
package's `default.pgo`:

```
GET <addr>/pyroscope/render
      ?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="<svc>"}
      &from=now-<window>&until=now&format=pprof
  →  examples/<svc>/cmd/<svc>/default.pgo   (or cmd/<svc>/ for repo-level commands)
```

If Pyroscope is unreachable or has no samples, the recipe exits non-zero
**without touching an existing `default.pgo`** — builds simply continue with
the previous profile or without PGO. Any pprof CPU profile works as input
(e.g. `curl <admin>/debug/pprof/profile?seconds=30` under load) if you don't
run Pyroscope.

### Workflow recommendation

1. Commit `default.pgo` per service (it is an opaque, mergeable-by-replace
   binary; Go tolerates stale profiles — they just optimize yesterday's hot
   paths). Refresh on a cadence (e.g. before each release) with
   `just pgo-fetch`, not on every commit.
2. Profile the PROFILE SOURCE deployment, not a laptop: PGO amplifies
   whatever workload shaped the profile.
3. Verify the build picked it up: `go version -m <binary> | grep -- -pgo`
   shows the build setting.
