# Round 5 — Latency Tails Observability (p50/p95/p99)

> **STATUS: COMPLETE (2026-06-11).** Lanes A+B + fix round merged (~20 commits). Engine:
> exponential histograms → Prometheus v3.12 NATIVE histograms, proven empirically end-to-end
> (zero _bucket series in TSDB; histogram_quantile sans le). Signals: kafka handler+producer,
> outbox publish_lag, pg.query.duration{query,pool} via pgx tracer, orders.lifecycle.duration.
> 23 recording rules + 2 SLOs w/ multiwindow burn-rate alerts (promtool-tested incl. the
> 100%-outage NaN case found by review — good leg NaN-proofed). Live validation: real
> choreography traffic, ALL rules returned data (table in transcript). Fix round: attr-set
> caches on hot paths, lifecycle placeholder-insert bias removed (xmax=0), promtool in CI,
> collector healthcheck. Final suite: 54+notifications-rerun = all green (one env-collision
> test fixed: ephemeral admin ports now a recorded convention).

> User decisions: exponential histograms + Prometheus native histograms (classic-buckets fallback documented);
> signals: kafka consumer handler + producer, outbox e2e publish lag, pg queries (pgx tracer), business
> order→terminal; SLO burn-rate multiwindow alerts. TDD; one commit per task; normal-English messages +
> "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"; `--no-verify`.

Current state: histograms only `http.server.duration{method,route,status}` (RouteTag) and
`cqrs.handler.duration_ms{handler,status}` — both on DEFAULT OTel explicit buckets (tail resolution
garbage past 1s); kafka/outbox/pg/redis have zero duration metrics. Pipeline: app OTel SDK → OTLP →
collector (prometheusexporter :8889, `otel_` ns) → Prometheus v2.55 scrape; app /metrics direct too.

Lanes: **A** histogram engine + deploy configs + dashboards/rules/alerts ∥ **B** new signal
instrumentation. Merge A→B, then end-to-end live validation + review + gate.

## Lane A — engine + consumption

| id | task |
|---|---|
| A1 | telemetry: exponential-histogram aggregation for ALL duration instruments — sdkmetric.View (instrument name wildcard `*duration*` + `*lag*` + `*.duration_ms`) → `AggregationBase2ExponentialHistogram{MaxSize:160, MaxScale:20}` on BOTH readers (OTLP + prometheus). Env `TELEMETRY_CLASSIC_HISTOGRAMS=false` (true → per-signal tuned explicit buckets table: http/cqrs 1ms..10s ×~14, kafka handler 1ms..30s, outbox lag 5ms..5m, pg 0.1ms..5s — documented fallback for environments without native-histogram support). Unit test: manual reader asserts exponential aggregation applied to a matching instrument; fallback env flips to explicit. |
| A2 | pipeline configs for native histograms — VERIFY each hop against current versions and FIX as needed: collector prometheusexporter native-histogram translation (otelcol-contrib ≥0.111 — check flag/config; if exporter can't, switch Prometheus to scrape the collector's prometheus endpoint with protobuf OR add prometheusremotewrite path — pick the minimal working shape and PROVE it), Prometheus: bump image to v3.x, `--enable-feature=native-histograms` (+ scrape protobuf negotiation), app-direct /metrics scrape job: otel prom exporter native-histogram support — verify; if unsupported, document that app-direct lane stays classic and collector lane is the native source. END-TO-END PROOF REQUIRED: compose up (obs profile) + skeleton/gateway emitting → `histogram_quantile(0.99, ...)` over a NATIVE histogram returns a value via Prometheus API; record the exact queries in the report. |
| A3 | recording rules `deploy/prometheus/rules-latency.yaml`: per-signal p50/p95/p99 5m recording rules (`:p99_5m` naming) for http(route), cqrs(handler), kafka handler(topic), outbox lag, pg(query), business lifecycle; keep native-histogram-compatible expressions (`histogram_quantile(0.99, sum(rate(metric[5m])) by (le?,labels))` — native syntax differs: no `le`; write BOTH variants? No — native-first since A2 makes it canonical). promtool check. |
| A4 | SLO burn-rate: `deploy/prometheus/slo.yaml` — SLOs as code comments + rules: HTTP availability+latency SLO (99% of requests <500ms, 30d window) and choreography SLO (99% orders reach terminal <60s, 30d) → error-budget burn multiwindow alerts (fast: 14.4× over 5m AND 1h; slow: 6× over 30m AND 6h — Google SRE workbook ch.5 shape), runbook_url annotations. promtool + unit-test rules w/ promtool test (write promtool test file with synthetic series — do it, it's the only way to prove burn math). |
| A5 | Grafana: new `latency-tails.json` dashboard — rows per signal: quantile timeseries (p50/p95/p99 from recording rules), native-histogram heatmap panel, exemplar toggle on (trace pivot); update edge/choreography dashboards' latency panels to recording rules. Valid JSON + provisioning load proof (live Grafana check like round 2). |
| A6 | docs: operations.md §Latency tails (engine choice, native-vs-classic fallback, recording-rule naming, SLO/burn-rate explainer + how to change targets, exemplars usage); k6 thresholds aligned to the SLO (p99<500ms); README observability row. |

## Lane B — signal instrumentation

| id | task |
|---|---|
| B1 | kafka: `kafka.consumer.handler.duration` histogram (seconds, {topic}) around handler call in run.go per-partition loop (success+error label? status={ok,error} — bounded); `kafka.producer.publish.duration` {topic} in producer.Produce/ProduceBatch (sync path measures full RTT). Extend kafka/metrics.go. Unit tests w/ manual reader. |
| B2 | outbox: `outbox.publish_lag` histogram (seconds) = clock.Now()−created_at per successfully published row in relay ProcessBatch (created_at already scanned? check sqlc row — add column to query if absent); records REAL DB→Kafka delivery delay incl. drain backlog. Test: pgtest+fake publisher, inject old created_at → lag bucket observed. |
| B3 | pg query duration: custom `pgx.QueryTracer` in platform/storage/pg — `pg.query.duration` histogram {query, pool} where query = sqlc name parsed from leading `-- name: X :kind` comment (sqlc always embeds it; fallback label "raw" — cardinality bounded by sqlc registry; pool = writer|reader). TraceQueryStart/End; batch + CopyFrom too if cheap. Wire into BuildPoolConfig (both pools; opt-out env `PG_QUERY_METRICS=true` default true). Tests: tracer unit (name parse table incl. no-comment), integration: one query → histogram sample w/ correct label. |
| B4 | business: `orders.lifecycle.duration` histogram (seconds, {terminal_status}) — gateway projection: on terminal upsert that APPLIED (rows_affected>0 — no double-count on dup/reorder), observe terminal_now − created_at (row's created_at returned by the upsert query — extend sqlc query RETURNING created_at if needed). Buckets via A1 exponential. Test: projection integration — created→paid sequence observes once; duplicate paid does NOT double-observe. |
| B5 | servicekit/docs touch: nothing env-wise expected; conventions §observability one paragraph: "every new RPC-ish path ships a duration histogram; name `<area>.<thing>.duration`, seconds, low-cardinality labels". |

## Final
Merge A→B → live end-to-end validation (compose obs profile + apps smoke: run e2e flow, query all 6 quantile rules return data, screenshot-equivalent via API asserts in report) → adversarial review of diff → fix MUST-FIX → full `-p 1` suite + lint + promtool + dashboards JSON → archive plan + memory.
