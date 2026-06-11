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

## Secrets sourcing

`config.Secret` fields support two file-based sourcing mechanisms; both keep
raw credentials out of the process environment (`/proc/<pid>/environ`, crash
dumps, `docker inspect`):

1. **`<NAME>_FILE` convention (preferred)** — implemented in
   `platform/config.Load`: when `NAME` is unset but `NAME_FILE` is set, the
   secret is read from that file with the trailing newline trimmed.
   Precedence: explicit `NAME` env var > `NAME_FILE` > `envDefault`. A set
   but unreadable `NAME_FILE` fails startup (fail-fast — an empty credential
   would otherwise surface as confusing auth errors much later). No struct-tag
   changes needed; it works for every `config.Secret` field automatically,
   including nested/embedded configs (`servicekit.Config` → `pg.Config`).
2. **caarlos0 `,file` tag modifier** — `env:"PG_DSN,file"` makes `PG_DSN`
   itself hold a *path*. Works, but changes the meaning of the variable for
   every environment (local dev must then also point at a file), so the
   boilerplate's own configs do not use it.

How the platforms map onto `_FILE`:

- **Docker (Swarm secrets / compose `secrets:`):** the secret is mounted at
  `/run/secrets/<name>`; set `PG_DSN_FILE=/run/secrets/pg_dsn`. Same shape for
  plain bind-mounted credential files.
- **Kubernetes:** mount a `Secret` as a volume and point `PG_DSN_FILE` at the
  mounted key (e.g. `/var/run/secrets/app/pg-dsn`). With
  [external-secrets-operator](https://external-secrets.io) the `Secret` object
  is synced from AWS Secrets Manager / Vault / GCP SM — the pod spec stays
  identical. Prefer the volume + `_FILE` route over `secretKeyRef` env
  injection: volume-mounted secrets are updated in place on rotation and never
  appear in the pod's environment.
- **SOPS (encrypted files in git):** decrypt at deploy time
  (`sops -d secrets.enc.yaml`) into a mounted file or a K8s `Secret` (via
  ksops/FluxCD's SOPS integration); the service still just reads `_FILE`. Do
  not decrypt into `.env` files on long-lived hosts.

`.env` files remain the local-dev path: insecure defaults live in
`.env.example`, and `config.Secret` keeps whatever arrives out of logs and
`%v` dumps (audit leak points with `git grep "\.Reveal()"`).

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

## Order-status streaming (SSE)

`GET /v1/orders/{id}/events` streams the order lifecycle
(`pending → created → paid | payment_failed | payment_timeout`) as
Server-Sent Events. Browser clients use the native `EventSource`; curl:

```bash
curl -N -H "Authorization: Bearer $(just token)" \
  http://localhost:8080/v1/orders/<id>/events
```

| Variable | Default | Description |
|---|---|---|
| `GATEWAY_SSE_HEARTBEAT` | `15s` | Keep-alive comment interval (keep below any LB idle timeout) |
| `GATEWAY_SSE_POLL_INTERVAL` | `2s` | Store-polling cadence with no Redis configured; ×30 = the Redis-mode safety-poll cadence |

**Transport:** the projection publishes every committed status change as
`{"order_id":…,"status":…}` on the single Redis channel `orders:status` (it
re-reads the row first, so the payload is always the authoritative current
status). Each gateway replica holds exactly ONE Redis subscription to that
channel and fans messages out to its open streams in-process — streams cost
zero Redis connections, so the open-stream count is bounded by memory and
file descriptors, not by Redis. If the subscriber connection drops, the
replica resubscribes (bounded backoff) and every open stream re-reads its
order from the store, with a slow safety poll (30×`GATEWAY_SSE_POLL_INTERVAL`)
as the net while Redis stays down — streams never silently stall. With
`REDIS_ADDRS` unset, streams degrade to polling the projection store every
`GATEWAY_SSE_POLL_INTERVAL` — same events, higher latency. The standalone
projection deployment (`cmd/projection`) publishes to the same channel, so
SSE works in both embedded and split topologies.

**Reconnects:** events carry a monotone id (status ordinal). `EventSource`
re-sends it as `Last-Event-ID` on reconnect and the gateway replays the
current status only when newer — reconnect storms cost one row read each, no
duplicate events.

**Edge budgets:** the SSE route group is deliberately exempt from
`http.TimeoutHandler` (it buffers and kills streaming responses) and the
request body cap (GET, body never read). The gateway server is therefore
built `WithoutTimeout`/`WithoutMaxBytes`, and the JSON API and attachments
groups re-apply both with the same `HTTP_HANDLER_TIMEOUT` /
`HTTP_MAX_BODY_BYTES` values — the `WithoutMaxBytes` startup WARN is the
expected audit reminder for the SSE exemption. On graceful shutdown active
streams are closed immediately (server `OnShutdown` hook); clients reconnect
to another instance and resume.

---

## Background workers (periodic & single-active)

Services register ticker-driven background work via
`servicekit.AddPeriodicWorker(name, interval, jitter, singleActive, fn)`:

- **interval** — the tick cadence. **jitter** adds a uniformly random extra
  delay in `[0, jitter)` per tick so replicas don't hammer shared
  dependencies in lock-step (`0` = none).
- **fn errors are logged, never fatal** — the loop keeps ticking. A worker
  that wants to stop the service should not exist; workers are maintenance
  loops.
- **singleActive=true** runs the loop under `pg.RunAsLeader`: a session-scoped
  Postgres advisory lock (key `leader:<name>:<schema>`) elects ONE instance
  across all replicas; the others stay hot-standby and re-try every second.
  The leader health-checks its lock connection every second — on a lost
  session the worker's context is cancelled and leadership is re-contested
  (failover within a few seconds). This is leader *election*, not fencing:
  a brief overlap window (≤ one health-check interval) is possible, so the
  worker body must still be idempotent (CAS guards, upserts).
- **singleActive=false** runs the worker on EVERY instance — right for
  naturally concurrency-safe loops.

The harness uses it internally:

| Worker | singleActive | Why |
|---|---|---|
| `inbox-cleanup` (`INBOX_CLEANUP_INTERVAL`) | `false` | age-based DELETE is idempotent; concurrent replicas delete disjoint rows |
| `audit-cleanup` (`AUDIT_CLEANUP_INTERVAL`) | `false` | same idempotent DELETE |
| `unpaid-watcher` (orders, `ORDERS_UNPAID_CHECK_INTERVAL`) | `true` | one scanner is enough; the CAS guard covers the overlap window |

The outbox relay keeps its own (pre-existing) advisory-lock leader mode
(`OUTBOX_SINGLE_ACTIVE`, lock key `outbox_relay:<schema>`): its leadership is
re-verified *between batches* of a drain — a tighter dual-publish bound than
the generic worker offers — so it intentionally does not use `RunAsLeader`.

---

## JWKS outage semantics (gateway auth)

When `GATEWAY_AUTH_DISABLED=false`, the gateway verifies bearer JWTs against the IdP's JWKS. The key set is fetched once at startup (startup FAILS fast if the initial fetch cannot complete) and then cached and refreshed in the background (`jwk.Cache`). During an IdP/JWKS outage:

- Tokens verifiable with the **cached** keys keep working — a short JWKS outage is invisible to clients.
- If verification needs a key fetch that fails (e.g. unknown `kid` after a key rotation mid-outage), the request is rejected with **401** and the generic detail `authentication failed`. This is **by design**: the edge never fails open, and infrastructure details (JWKS URL, network errors) are never echoed to clients.
- The real cause is logged at **ERROR** (`auth: token verification failed with non-token error`). Alert on a sustained rate of this log line — a spike means JWKS trouble, not bad client tokens (those produce plain `invalid token` 401s with no ERROR log).

### Machine-to-machine (service account) tokens

Non-interactive callers (cron jobs, sibling systems, smoke tests) use the
`gateway-m2m` confidential client (client-credentials grant, no user). The dev
realm ships it with secret `gateway-m2m-dev-secret` — **rotate for anything
non-local**. Its service account carries the realm role `user`, and the same
`oidc-audience-mapper` as the interactive `gateway` client stamps `aud:
gateway`, so M2M tokens pass the gateway's verifier (`GATEWAY_JWKS_AUDIENCE`)
and RBAC unchanged:

```bash
TOKEN=$(just token-m2m)   # client_credentials against localhost:8180
curl -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"customer_id":"batch-42","amount_cents":1234,"currency":"USD"}' \
  http://localhost:8080/v1/orders
```

Note the ownership consequence: the principal's subject is the *service
account's* user id, not a customer's — so non-admin M2M callers can only read
back orders whose `customer_id` equals that subject (same read-path ownership
rule as human users). Grant the `admin` realm role to the service account if
the integration legitimately needs cross-customer reads.

---

## Latency tails & SLOs

### Histogram engine: exponential → Prometheus native

Every duration instrument (`http.server.duration`, `cqrs.handler.duration_ms`
and the per-signal kafka/outbox/pg/lifecycle histograms) is aggregated as a
**base-2 exponential histogram** by an SDK view
(`platform/observability/telemetry/views.go`, MaxSize 160 / MaxScale 20),
applied to both the OTLP-push and Prometheus-pull readers. Exponential
histograms keep ~1% relative error from microseconds to minutes with a fixed
memory budget — accurate p99s without per-signal bucket tuning.

Downstream they become **Prometheus native histograms**:

- collector lane: otelcol-contrib ≥ 0.153 `prometheus` exporter converts them
  automatically; Prometheus v3.12 ingests them because
  `deploy/prometheus.yml` sets `scrape_native_histograms: true` (which also
  flips scrape negotiation to the protobuf format);
- app-direct lane (`/metrics`): the OTel Go prometheus exporter emits
  client_golang native histograms — same protobuf scrape, no `otel_`
  namespace on the names.

Both lanes were verified live (probe → `histogram_quantile` returns tail
values; see the PromQL below).

**Classic fallback:** environments whose Prometheus cannot ingest native
histograms set `TELEMETRY_CLASSIC_HISTOGRAMS=true` — duration instruments
flip to per-signal tuned explicit buckets (tables in `views.go`), exported as
ordinary `*_bucket` series. The recording rules/dashboards in this repo are
native-first; under the fallback adapt expressions to
`sum by (le, ...) (rate(<metric>_bucket[5m]))`.

### PromQL: native histograms have no `le`

Native histograms are ONE series per label set — no `_bucket`, `_sum`,
`_count`. The query shapes change:

```promql
# quantile: rate the histogram series directly, no le grouping
histogram_quantile(0.99, sum by (job, http_route) (rate(otel_http_server_duration_milliseconds[5m])))

# request count (replaces rate(..._count)):
histogram_count(sum by (job) (rate(otel_http_server_duration_milliseconds[5m])))

# fraction of observations in [0, 500ms] — powers the latency SLO:
histogram_fraction(0, 500, sum(rate(otel_http_server_duration_milliseconds[5m])))
```

Note the OTLP push interval (`OTEL_METRIC_EXPORT_INTERVAL`, default 60s)
quantizes the collector lane: `rate()` windows must span at least two export
intervals — the standard 5m windows are fine, ad-hoc `[1m]` queries return
empty/NaN between exports.

### Recording rules (`deploy/prometheus/rules-latency.yaml`)

Per-signal p50/p95/p99 over 5m windows, named
`<level>:<metric>:<quantile>_<window>`, e.g.
`job_route:http_server_duration_milliseconds:p99_5m`,
`job_topic:kafka_consumer_handler_duration_seconds:p99_5m`,
`job_query:pg_query_duration_seconds:p99_5m`,
`terminal_status:orders_lifecycle_duration_seconds:p99_5m`. Dashboards
(`latency-tails`, `edge`) consume the rules; ad-hoc exploration can always
fall back to the raw native series.

### SLOs & burn-rate alerts (`deploy/prometheus/slo.yaml`)

The SLO targets live in ONE place — the comment block at the top of
`deploy/prometheus/slo.yaml`; docs and the k6 thresholds reference it:

- **SLO-1**: 99% of HTTP requests < 500 ms and non-5xx, 30-day window
  (k6 mirrors this as `http_req_duration: p(99)<500`).
- **SLO-2**: 99% of orders reach a terminal status < 60 s, 30-day window.

Alerts follow the Google SRE workbook multiwindow shape: **fast burn** at
14.4× the budget rate over 1h AND 5m (critical — 2% of the 30d budget/hour),
**slow burn** at 6× over 6h AND 30m (warning). The long window catches the
sustained problem; the AND with the short window stops alerting promptly
after recovery and ignores brief spikes.

**Changing a target:** edit the error-ratio expressions (the `500`/`60`
thresholds inside `histogram_fraction`) and the alert thresholds
(`14.4 * <budget>` / `6 * <budget>` where budget = 1 − target), update the
slo.yaml comment block, re-run the promtool unit tests
(`deploy/prometheus/tests/slo_test.yaml` — synthetic native-histogram series
prove the burn arithmetic), and align `scripts/k6/order-flow.js` thresholds:

```bash
just promtool   # rules + config check + rule unit tests (same gate as CI)
# or just the unit tests:
docker run --rm -v $PWD/deploy/prometheus:/r --entrypoint promtool \
  prom/prometheus:v3.12.0 test rules /r/tests/slo_test.yaml
```

### Exemplars (latency spike → exact trace)

Set `OTEL_METRICS_EXEMPLAR_FILTER=trace_based` (and keep
`TELEMETRY_TRACE_RATIO > 0`): histogram recordings made inside a sampled span
carry trace-linked exemplars, and Grafana renders them as dots that pivot to
Jaeger. Caveats today: the collector's pull exporter does not attach
exemplars to NATIVE histograms (classic fallback only), and recording-rule
series never carry exemplars — query the raw series for exemplar overlays.

---

## Load testing

Two complementary tools, different questions:

| Tool | Question it answers | Traffic | Pass/fail signal |
|---|---|---|---|
| **k6** (`just load`) | "Does the stack hold the external SLO under volume?" | Happy-path only, VU-driven | SLO thresholds (p99 latency, check rate) |
| **trafficgen** (`just traffic`) | "Does the system stay CORRECT under load?" | Seeded adversarial mix (idempotency races, invalid payloads, SSE drops) | Invariant violations (exit 1) |

Use k6 for capacity/SLO work and performance regressions; use trafficgen
when you change anything on the correctness path (idempotency, projection,
outbox/inbox, SSE) and want a reproducible storm to prove invariants still
hold. The e2e variant of the same mix runs in CI
(`examples/e2e/traffic_test.go` — see docs/testing.md §Traffic emulation).

### k6 (external SLO/perf)

`scripts/k6/order-flow.js` exercises the full asynchronous order flow:
`POST /v1/orders` with a unique `Idempotency-Key` per iteration (amount below
the payments decline threshold), then polls `GET /v1/orders/{id}` until the
projection reaches `created`/`paid`. Thresholds: `http_req_duration p(99)<500`
and check success rate > 99% — k6 exits non-zero when either trips. Both
mirror SLO-1 in `deploy/prometheus/slo.yaml` (the single source of truth for
SLO targets; see §Latency tails & SLOs above) — change targets there first.

```bash
just up-apps            # gateway + services + infra
just load               # 10 VUs, 30s (dockerized grafana/k6)
just load 50 2m         # 50 VUs for 2 minutes

# Authenticated run (auth enabled): the script uses the token's `sub` claim
# as customer_id so the GET ownership check passes.
TOKEN=$(just token) just load

# Custom target
BASE_URL=https://staging.example.com just load 20 1m
```

**Docker networking (macOS vs Linux):** the recipe runs k6 in a container, so
`localhost` inside the container is the k6 container itself, not your machine.
The default `BASE_URL` is therefore `http://host.docker.internal:8080`:
resolved natively by Docker Desktop on macOS/Windows, and mapped on Linux via
the recipe's `--add-host=host.docker.internal:host-gateway` flag. With a k6
binary installed on the host you can skip Docker entirely:
`k6 run --vus 10 --duration 30s scripts/k6/order-flow.js` (plain
`http://localhost:8080` works there).

**Interpreting failures:** against an absent/unreachable gateway every request
fails to connect, the `checks` threshold trips, and k6 exits non-zero — that
is the harness working, not a script bug. A failing
`order reached created/paid within poll budget` check with passing POSTs means
the projection lags: check consumer lag and outbox backlog (see the dashboards
and `rpk group describe`).

### trafficgen (correctness under load)

`cmd/trafficgen` runs the gateway scenario pack (`examples/gateway/traffic`)
against a live stack through the seeded generator
(`platform/testkit/traffic`): weighted Poisson traffic across rate phases,
including the adversarial scenarios k6 deliberately avoids — concurrent
idempotency-key mismatch races, edge-validation rejects, SSE subscribers
that drop mid-stream. Every accepted order is tracked in a ledger; after
generation the CLI polls the read API and reports invariant violations
(every order terminal, exactly one order id per idempotency key, documented
rejection codes). Exit 1 on any violation.

```bash
just up-apps                                        # gateway + services + infra
just traffic                                        # 20 rps for 30s against localhost:8080
just traffic --rate 50 --duration 1m
just traffic --phases "10rps:5s,40rps:20s,80rps:5s" # ramp → plateau → spike
just traffic --mix happy=80,sse=0                   # reweight/drop scenarios
just traffic --seed 1718041200000000000             # replay a previous run exactly
TOKEN=$(just token) just traffic --token "$TOKEN"   # auth-enabled stack
```

The resolved seed is always printed: generation decisions (scenario
sequence, payloads, arrival gaps) replay exactly under the same seed, so a
violation found at 3am is reproducible at 9am (wall-clock interleaving is
not reproducible — invariants must hold under any interleaving, which is
the point). Note the per-IP rate limiter (`RATELIMIT_RPS`, default 50 rps)
WILL throttle high-rate runs from one machine — raise it on the gateway or
keep `--rate` below the limit; a throttled run shows up as `HTTP_429`
scenario failures, not as invariant violations. Unlike the e2e traffic
test, the CLI has no orders-DB access, so the row-count cross-check is
HTTP-only (terminal-status polling).

---

## Feature flags (flagd)

`platform/featureflags` wraps the OpenFeature SDK; the in-memory provider is
wired today (see the package godoc). To evaluate flags from a real backend
without code changes beyond provider registration, run
[flagd](https://flagd.dev) next to the stack. Nothing in the repo depends on
it, so the compose wiring ships commented-out — paste into
`docker-compose.yml` and create the flags file when you need it:

```yaml
#  flagd:
#    image: ghcr.io/open-feature/flagd:v0.11.8
#    restart: unless-stopped
#    profiles: ["flags"]      # opt-in: docker compose --profile flags up -d
#    command: ["start", "--uri", "file:/etc/flagd/flags.json"]
#    volumes:
#      - ./deploy/flagd/flags.json:/etc/flagd/flags.json:ro
#    ports:
#      - "8013:8013"          # gRPC evaluation API (the Go provider's default)
```

`deploy/flagd/flags.json` (flagd's file syntax; hot-reloaded on change):

```json
{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "new-checkout": {
      "state": "ENABLED",
      "variants": { "on": true, "off": false },
      "defaultVariant": "off",
      "targeting": {
        "if": [{ "in": [{ "var": "tier" }, ["beta", "internal"]] }, "on", "off"]
      }
    },
    "checkout-banner": {
      "state": "ENABLED",
      "variants": { "spring": "spring-sale", "none": "" },
      "defaultVariant": "none"
    }
  }
}
```

Go wiring (replaces `featureflags.NewInMemory`; requires adding the
`open-feature/go-sdk-contrib` flagd provider module):

```go
provider := flagd.NewProvider(flagd.WithHost("localhost"), flagd.WithPort(8013))
_ = openfeature.SetProviderAndWait(ctx, provider, openfeature.WithDomain("gateway"))
flags := featureflags.New(openfeature.NewClient(openfeature.WithDomain("gateway")))
```

Evaluation context (the `tier` in the targeting rule above) comes from the
`featureflags.BoolValue(ctx, …, map[string]any{"tier": …})` call sites; flag
*definitions* stay in flagd, so toggles do not require a deploy.

---

## Kafka retry tiers & DLT redrive runbook

The `platform/messaging/retry` package implements tiered retry routing.
Tier topics are **index-named**: `<base>.retry.0`, `<base>.retry.1`, … — the
tier's delay travels in the `retry-due-at` record header, never in the topic
name, so retry policies can be retuned without stranding in-flight records.
The consumer group for all of a service's retry tiers is `<group>.retry`.
After the last tier a record lands on `<base>.DLT`.

**Permanent errors skip the tiers.** When the handler failure chain contains a
permanent `apperr` error (`apperr.IsPermanent` — validation failures, invalid
state transitions: nothing a redelivery can fix), both `kafka.WithRetry` and
the tiered escalator short-circuit after the **first** attempt and produce the
record straight to the DLT, stamping the `x-error-code` header with the apperr
code. Triage DLT records by that header first — the code's meaning, HTTP
mapping and permanence are documented in the generated registry,
[`docs/errors.md`](errors.md). A DLT dominated by one permanent code is a
producer-side bug (bad payloads), not a downstream outage; redriving those
records unchanged will only dead-letter them again.

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
| `x-error-code` | `kafka.WithRetry` and the retry escalator on DLT produce | `apperr` code of the dead-lettering error (when the chain carries one) — triage key; see [`docs/errors.md`](errors.md) |

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
| SSE streams (`/v1/orders/{id}/events`) | Horizontal with the gateway — streams are replica-local; clients reconnect and resume via `Last-Event-ID` | One Redis subscriber connection per REPLICA (not per stream): open streams are bounded by memory + file descriptors, not Redis. Keep `GATEWAY_SSE_HEARTBEAT` below the LB idle timeout. At extreme broadcast volume, shard `orders:status` into hash-named channels (see `internal/sse` package doc — documented, not built) |
| Gateway projection / any consumer group | More members in the consumer group — embedded: more gateway replicas; split: scale `cmd/projection` | **Members beyond the partition count idle** (`TOPIC_PARTITIONS`, default 6, is the parallelism ceiling). Repartitioning later is an ops event — size partitions for target parallelism up front. Split the projection (ADR-0008) before scaling the edge aggressively |
| Orders / payments / notifications consumers | Replicas up to partition count per topic | Same partition ceiling. Per-key order is per-partition; tiered retry breaks per-key order unless key parking is enabled (`retry.Policy.KeyParkingWindow`) |
| Outbox relay | Do NOT scale for throughput by default: `OUTBOX_SINGLE_ACTIVE=true` runs one active relay per service (advisory-lock leader); extra replicas are warm standbys (takeover ≤ ~2× poll interval) | The single relay drains the table until empty per tick and batch-publishes; if it still lags, raise `OUTBOX_BATCH_SIZE`, lower `OUTBOX_POLL_INTERVAL`, then consider the LISTEN/NOTIFY tier (ADR-0004 amendment). Setting `OUTBOX_SINGLE_ACTIVE=false` trades per-aggregate ordering for parallel publish — only with reorder-safe consumers |
| Postgres | Per-service instances first (ADR-0009), then read replicas (`PG_READER_DSN` reader pool), PgBouncer in front (set `PG_MIGRATE_URL` for migrations) | Writer is per-service vertical + partitioning territory; the platform's reader/writer split is already plumbed |
| Postgres **connections** | Budget per replica: `N_replicas × (PG_MAX_CONNS + reader pool if PG_READER_DSN) + standalone projection replicas × pool + migrate job ≤ max_connections − superuser_reserved_connections (3)` | **Default pools hit the wall at N≈4**: `PG_MAX_CONNS=25 × 4 = 100` = Postgres' default `max_connections`; the 5th replica fails readiness with "too many clients". Remedies: size per-replica pools down (`PG_MAX_CONNS=10`/`PG_MIN_CONNS=2`, as in `deploy/k8s/gateway-deployment.yaml`) and/or front with PgBouncer in transaction mode (compose profile `pgbouncer`; requires `PG_STATEMENT_CACHE_MODE=1` and a direct `PG_MIGRATE_URL` — see `.env.example`) |
| Redis (cache L2 + rate limit) | Standard Redis scaling; rueidis supports clustering | Every cache `Set`/`Delete` publishes an invalidation message; **every app instance subscribes** — pub/sub fan-out grows O(instances × writes). At high replica counts consider shorter L1 TTLs instead of chatty invalidation. L2 outage is absorbed by the circuit breaker (L1-only mode), so Redis HA is a latency/coherence concern, not availability |
| Kafka/Redpanda | Cluster sizing per vendor guidance | Topics here are created with `TOPIC_RF` (default 1 — dev only); production needs RF ≥ 3 and `ENSURE_TOPICS=false` with topics as IaC |

### Write-path ceiling (gateway orders flow)

Adding gateway replicas scales HTTP, **not order throughput** — every
accepted order costs the shared read-model database, per order:

1. **Pending INSERT** (POST, synchronous by default) — writer pool.
2. **Idempotency lookup** (POST with `Idempotency-Key`) — reader pool
   (`PG_READER_DSN`; the writer when no replica is configured).
3. **~3 projection transactions** — one inbox-deduped tx per consumed event
   (OrderCreated, the payment outcome, plus a timeout event when payment
   stalls) — writer pool, regardless of embedded vs split projection.

That aggregate writer cost caps the system at roughly **2–3.5k orders/s on a
mid-size single Postgres instance, independent of replica count** (ADR-0008
amendment: split mode is not a pure edge). Levers, in escalation order:
`PG_READER_DSN` (moves the idempotency read off the writer),
`GATEWAY_PENDING_ASYNC=true` (pending inserts collapse into one batched
multi-row INSERT per ≤50 ms/≤100 rows — trades GET-after-POST
read-your-writes for writer relief), partitioning `orders_read`, then a
bigger writer (e.g. Aurora).

---

## Kubernetes reference manifests

`deploy/k8s/` contains **reference manifests — not production-ready**: a
gateway Deployment/Service/HPA (probes on the admin port, preStop drain
matching `DRAIN_GRACE`, `GOMEMLIMIT` from the memory limit) and a migrate Job
(`MIGRATE_ON_START=false` pattern). They encode the lifecycle contracts this
repo's code actually implements; secrets management, network policies, TLS,
and registry plumbing are deliberately out of scope. Validate with
`kubectl apply --dry-run=client -k deploy/k8s/`.

Hosting on AWS? See [`docs/aws-notes.md`](aws-notes.md) for the condensed
service mapping (MSK Provisioned, no RDS Proxy, node-based ElastiCache
Valkey, ALB SSE settings, KEDA lag scaling, AMP/AMG/ADOT).

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
