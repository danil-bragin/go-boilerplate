# Round 6 — Traffic Emulation in testkit

> **STATUS: COMPLETE (2026-06-11).** T1–T5 done (~8 commits incl. arch-rule exception).
> Gates: gofumpt clean, golangci-lint 0 (after cache clean — shared cache had cross-worktree
> phantoms), `go test -short -count=1 ./...` green (~37s wall), e2e traffic green ×2
> (57.65s / 59.65s; generation ~30s, verify <1s), full `go test -p 1 ./examples/...` green,
> `just -n traffic` parses. Seed workflow proven: buggy stub gateway → 35 winner violations,
> `--seed` replay reproduced identical violation groups. Found+fixed during T3 gate: SSE
> client must close (never drain) live event-stream bodies — drain blocked until ctx budget.

> User decisions: both levels (testkit primitives + in-process integration test + live-stack CLI);
> weighted scenarios + Poisson arrivals + phases; adversarial mix; seeded determinism;
> CI asserts INVARIANTS, latency asserts behind env flag. TDD; one commit per task;
> "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"; `--no-verify`.

## T1 — `platform/testkit/traffic` core
- `Scenario{Name string; Weight int; Run func(ctx, *rand.Rand, *Ledger) error}` — target-agnostic.
- `Generator`: `Config{Seed int64 (0 → time-derived, ALWAYS logged), Workers int, Phases []Phase{Rate float64 /*mean rps*/, Duration}}` + `Mix []Scenario`. Poisson arrivals (exp inter-arrival from rng) feeding a worker pool; weighted scenario pick per op; per-worker rng derived from master seed (reproducible regardless of goroutine scheduling for GENERATION decisions; wall-clock interleaving documented as non-reproducible).
- `Result`: per-scenario {started, ok, failed, errors by code}, latency samples per scenario (bounded reservoir ≤100k), `Quantile(scenario, p)`, `String()` summary table.
- `Ledger` (thread-safe): scenarios record expectations — `ExpectTerminal(orderID, allowed []status, deadline)`, `ExpectExactlyOneWinner(group string, id)`, `ExpectRejected(opID, code)` + observations; `Verify(ctx, Probes{OrderStatus func(ctx,id)(string,error); CountOrders func(ctx,ids)(int,error)}) []Violation` — polls until deadline, returns violations (empty = pass).
- Unit tests (-short, no Docker): determinism (same seed → identical scenario/op sequence via recording target), Poisson mean within tolerance, weighted mix distribution, phases timing, ledger verify logic (fake probes), reservoir quantiles sanity.

## T2 — gateway scenario pack (reusable example)
- `examples/gateway/traffic` (exported package, importable by e2e AND cmd): `Pack(base string, client *http.Client, tok string) []traffic.Scenario`:
  happy POST→ledger ExpectTerminal{paid}; decline (amount≥1e6)→ExpectTerminal{payment_failed}; invalid payload→ExpectRejected(VALIDATION_FAILED); idempotent retry (same key+body ×2-3 concurrent)→ExpectExactlyOneWinner + same id; mismatch race (same key, different body, concurrent)→exactly one 202 winner group, others 409 (or absorbed-within-window — encode the documented tolerance); GET/LIST reads (no ledger); SSE subscribe→read to terminal (weight small) + SSE early-drop client.
- Payloads generated from rng (amounts, currencies from allowlist, customer ids pool — bounded cardinality).
- Unit-testable parts (-short): payload gen determinism, mismatch-group bookkeeping.

## T3 — e2e traffic test
- `examples/e2e/traffic_test.go`: existing e2e harness (full stack, shared containers), phases: ramp 5s@10rps → plateau 20s@40rps → spike 5s@80rps (in-process tolerances), full mix incl adversarial; then Ledger.Verify with real probes (gateway GET + orders DB count). Assertions (CI): zero violations — every accepted order terminal, exactly-one per idempotency group, orders rows == unique accepted, rejects carried expected codes. Latency asserts (p99 POST < 1.5s in-process etc.) ONLY when `TRAFFIC_ASSERT_LATENCY=1`. Budget ≤120s wall; `-short` skipped.
- Seed logged via t.Logf; on failure the seed reproduces the exact generation sequence.

## T4 — `cmd/trafficgen` + recipe
- CLI: `--base-url --rate --duration --workers --seed --phases "10rps:5s,40rps:20s" --mix happy=70,decline=10,invalid=5,idem=5,mismatch=2,reads=6,sse=2 --token`; reuses gateway pack + Generator; prints Result table + violations; exit 1 on violations. `just traffic [args]`; ops doc §Load testing extended (when k6 vs trafficgen: k6 = external SLO/perf, trafficgen = correctness-under-load + reproducible adversarial mix).

## T5 — docs
- testing.md §Traffic emulation (pyramid position, seed reproduction workflow, latency-flag rationale — CI runners can't hold p99); conventions one paragraph (scenario packs live with the service that owns the API; ledger invariants pattern).

## Final
review pass over diff → fixes → lint 0, short green (incl new unit tests), `go test -p 1 ./examples/e2e/ -run Traffic -count=1` green ×2 (flake check), full short lane timing unchanged (<35s), archive plan + memory.
