# Operations: Go runtime tuning

---

## Compose profiles

The `docker-compose.yml` is split into three profiles so every developer runs
only the services they actually need.

### Profile matrix

| Profile | Services | Command |
|---|---|---|
| _(none — core)_ | postgres, redpanda, redpanda-console, redis, minio, minio-setup, keycloak | `just up` |
| `observability` | core + otel-collector, jaeger, prometheus, grafana, pyroscope | `just up-obs` |
| `apps` | core + gateway, orders, payments, notifications | `just up-apps` |
| `observability` + `apps` | Everything | `just up-full` |

### Notes

- **Core is always included.** Profile-less services (postgres, redpanda, redis,
  minio, keycloak) start with any `docker compose up` invocation regardless of
  which profiles are active.
- **Apps are independent of observability.** The four application services
  (`gateway`, `orders`, `payments`, `notifications`) do not `depends_on` any
  observability service.  When the observability profile is absent, the apps
  still start and run normally — OTLP gRPC connections to `otel-collector` are
  established lazily and fail silently, so traces and metrics are simply
  uncollected rather than fatal.
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
go run ./examples/gateway/cmd/gateway  # runs against postgres/redpanda/redis/minio/keycloak on localhost
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

**Solution.** Every `cmd/*/main.go` blank-imports
`go.uber.org/automaxprocs`:

```go
import _ "go.uber.org/automaxprocs"
```

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

To run multiple services in a single process (e.g. an integration test), pass
distinct configs explicitly via code rather than relying on env variables — see
`examples/e2e/e2e_test.go` for the pattern.

---

## Per-IP rate limiting (gateway)

The gateway applies a per-client-IP token-bucket rate limiter at the edge. Configure via:

| Variable | Default | Description |
|---|---|---|
| `RATELIMIT_RPS` | `50` | Sustained token refill rate (requests per second per IP) |
| `RATELIMIT_BURST` | `100` | Maximum burst depth per IP |
| `RATELIMIT_REDIS` | `false` | Use Redis-backed distributed limiter (requires `REDIS_ADDRS`) |
| `TRUSTED_PROXIES` | _(empty)_ | Comma-separated CIDRs for trusted reverse proxies (e.g. `10.0.0.0/8`). When set, `X-Forwarded-For` is consulted for client-IP extraction. |

**Memory vs Redis:** The default in-memory limiter is process-local. For multi-replica deployments set `RATELIMIT_REDIS=true` so all instances share a single Redis-backed counter. If Redis is unavailable the gateway falls back to in-memory (graceful degradation, WARN logged).

**Trusted proxies:** XFF is ignored unless `TRUSTED_PROXIES` is set. Invalid CIDRs cause the gateway to refuse to start (fail-fast). Use network-level trust only — never trust an IP that end-users can set.

---

## Retry-topics runbook (Kafka)

The `platform/messaging/retry` package implements tiered retry routing. Topics are named `<base>.retry.<dur>` (e.g. `orders.commands.retry.5s`, `orders.commands.retry.30s`). The consumer group for retry topics is `<group>.retry`.

| Step | Action |
|---|---|
| Check lag | `kafka-consumer-groups.sh --describe --group <group>.retry` — watch lag on each retry tier |
| DLT redrive | Messages on `<topic>.DLT` can be republished to the original topic; procedure unchanged from standard DLT redrive |
| Tuning tiers | Edit `platform/messaging/retry` tier definitions and redeploy — each tier is a separate consumer group with its own lag metric |
