# Round 7 — S2 Hygiene + S3 Capabilities

> **STATUS: COMPLETE (2026-06-11).** W1 + lanes B/C/D + review fixes merged (~25 commits).
> Final: 60/60 packages ok ×2 (pre/post review fixes), lint 0, errgen sync, compose all
> profiles valid. Lane B's demo smoke found 3 latent compose bugs (pyroscope tag, console
> entrypoint crashloop, Keycloak issuer mismatch — every `just token` 401'd). Review caught
> a real MUST-FIX: redis-mode rate-limit tiers shared one bucket for anonymous requests
> (double debit, wrong clamps) — per-tier key prefixes now. Plus binding-tier RateLimit
> headers, DSAR poison-row resilience, gitleaks org-license note. S1 (remote + first CI run)
> deferred per user order.

> User order: S2 entirely → ALL of S3 → S1 (remote/CI) later. TDD; one commit per task;
> "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"; `--no-verify`.
> Waves: W1 = S2 (single lane, must land first — touches httpx/validation that S3-D touches);
> W2 = three parallel lanes B/C/D. Final gate + review per round ritual.

## W1 — S2 hygiene (single lane)

| id | task |
|---|---|
| S2.1 | cache L2 visibility: `cache.l2.errors` counter {op=get\|set\|del} + WARN log at the silent-ignore sites (cache.go:44,159,171 TODOs die); outboxkafka publisher.go:135 — WARN w/ outbox row id on malformed-headers drop (inject optional slog or use slog.Default pattern consistent w/ package). Tests: counter moves on forced L2 error; WARN emitted (log capture). |
| S2.2 | .env.example: + `TELEMETRY_LOGS=false` (OTel section), + `GATEWAY_PENDING_ASYNC=false` (gateway section), both commented w/ one-liner; pgbouncer profile: README profiles table + operations.md profile matrix get the 4th row; `just down`/`logs` recipes include `--profile pgbouncer`. |
| S2.3 | fast lane: TestGateway_AuthEnabledNoVerifierFailsClosed — verifier built w/ sub-second timeout (option exists from round-2 JWKS bound? pass small AUTH fetch timeout/ctx) → test <1s; assert lane total <30s after. |
| S2.4 | VALIDATION_FAILED unify on 400: httpx decode path 422→400 (problem.go:49,71 historical branch removed), update decode tests + any 422 references (openapi? attachments tests), errors.md regen (already 400 — now true). Note in commit: pre-1.0 contract alignment, registry is the source of truth. |
| S2.5 | errgen linkage guard: test in cmd/errgen asserting every `examples/<svc>` root package present in main.go imports (parse the file or build-tag import list) — new service w/ codes can't silently skip the registry. |
| S2.6 | dead exports: wire `validation.<rule>` catalog keys into VALIDATION_FAILED rendering (localized per-field messages in problem `errors`/params when localizer present — the seam conventions §12 already describes; test ru/en); DELETE httpx.WriteDecodeError + apperr.Codes() (unused; Registered() is the API); KEEP apperr.Newf w/ godoc note (public-API-by-intent). |
| S2.7 | docs truth: README testkit line (+traffic, goleakopts), justfile blurb (gen/errgen/traffic/promtool, drop buf/sqlc), integration timing estimate realistic (10-20 min serial), ARCHITECTURE + conventions §2 group map (+apperr,i18n,clock; testkit/traffic row); conventions §13 legacy-metrics note (cqrs duration_ms + http ms grandfathered — renaming = breaking metric change). |
| S2.8 | goleak symmetry: goleakopts TestMain in platform/testkit/traffic, examples/gateway/traffic, examples/e2e. |

## W2 — S3 capabilities (parallel lanes)

### Lane B — proof-of-life DX
| id | task |
|---|---|
| B1 | `cmd/probe`: static ~40-line GET $url (default http://127.0.0.1:9090/livez), exit 0/1, timeout 2s, no deps; built into the parametric Dockerfile (extra stage, COPY into distroless) + `HEALTHCHECK` in Dockerfile + compose `healthcheck` blocks for 4 app services + `depends_on: service_healthy` where meaningful (apps profile). Tests: probe unit (httptest 200/500/timeout); compose config -q. |
| B2 | `just demo`: up-apps → wait healthy (probe/curl loop) → POST order (happy) → poll GET to paid → print order JSON + Jaeger/Grafana/Console URLs → leave stack running (note `just down`). + CONTRIBUTING fresh-clone-to-green measured number (measure once locally, state hardware caveat). |

### Lane C — cross-service confidence
| id | task |
|---|---|
| C1 | fast-lane contract tests (no Docker): new `examples/e2e/contract` (or examples/contract — decide: must not violate arch cross-import guard; e2e already imports all services) package `-short`: orders' REAL producer path (domain service + outboxkafka encode) → fakes.Broker → gateway projection handler + payments consumer decode: assert field-level expectations (order id/customer/amount propagate, event-type names match EventTypeFor, headers msgctx/principal present). Catches semantic drift buf-breaking can't. |
| C2 | `sse-storm` traffic scenario in gateway pack: open N streams (cfg via scenario weight × rng 5-15), kill half mid-flow, reconnect w/ Last-Event-ID, ledger-assert no missed/regressed terminal transitions; weight default small; e2e traffic test mix includes it; regression-protects the round-3/round-5 SSE races under load. |

### Lane D — edge/security depth
| id | task |
|---|---|
| D1 | per-principal rate limit: `httpserver.PrincipalKey(fallback KeyFunc)` (sub from auth ctx; anonymous → fallback(IP)); gateway: second limiter for authed tier (env RATELIMIT_AUTHED_RPS/BURST default 200/400) chained after per-IP; tests (principal bucket isolation, anonymous falls back); ops doc para. |
| D2 | bulkhead decision: wire `resilience.Bulkhead` as SSE concurrent-streams cap in gateway (env GATEWAY_SSE_MAX_STREAMS default 0=off; reject w/ 503 RATE_LIMIT_UNAVAILABLE? no — new code GATEWAY_SSE_SATURATED 503, registered) — gives the dead export a real consumer + protects the known SSE memory surface; tests. |
| D3 | audit read path: `audit.PgStore.Query(ctx, actor string, since time.Time, limit int)` (+sqlc) + gateway admin endpoint `GET /v1/audit?actor=&since=` (admin role only, openapi, codes) — closes the data-privacy.md DSAR promise; tests (RBAC 403, owner query). |
| D4 | Deprecation/Sunset: `httpserver.Deprecate(sunset time.Time, successor string)` middleware (RFC 8594 Sunset + Deprecation + Link successor) + ops doc §API evolution: chi parallel-mount /v1+/v2 pattern + proto vN analog (doc only, no working v2); tests (headers emitted). |
| D5 | CI security: gitleaks job (pinned action, full-history scan) + dependency-review job (PR-trigger; dormant until remote — comment says so); both non-matrix, cheap. |

## Final
W1 merge → lanes B∥C∥D → merge B→C→D → review pass over round-7 diff → fixes → full gate (lint 0, short <30s, full -p 1 suite, e2e traffic incl sse-storm ×1, compose valid, errgen/doc-test/promtool) → archive + memory.
