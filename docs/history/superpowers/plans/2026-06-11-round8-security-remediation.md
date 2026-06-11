# Round 8 — Security Remediation

> **STATUS: COMPLETE (2026-06-12).** W1 + lanes A/B/C/D + 2 review rounds merged (~50 commits).
> 5-agent audit → all findings fixed; app layer was already strong, exposure was unenforced trust
> model + EOL infra. Security review of the diff found the headline audit hash-chain was BROKEN
> (ns-hash vs µs-storage → false-positive on every clean row) + a CRITICAL preflight gap
> (AUTH_ALLOW_INSECURE_JWKS unguarded in prod) — both fixed in a second review round along with
> HMAC-keying, table-ownership-transfer (so REVOKE actually bites), genesis/head truncation
> detection, and denial-storm coalescing. Final: gateway+e2e green, 49 other pkgs ok, lint 0,
> errgen sync, compose+secure-overlay valid. Bench: HMAC chain ~722µs vs sha256 ~684µs (~5%, DB-bound,
> no regression). S1 (remote + first CI) NEXT.

> Source: 5-agent security audit (2026-06-11). App layer is strong (IDOR/alg-confusion/idempotency/injection
> all clean); exposure is the documented-but-unenforced trust model + EOL infra images + secret-redaction
> JSON gap. User decisions (all max-depth): (1) full transport knobs + enforce; (2) audit INSERT-role +
> hash-chain + audit-on-denial; (3) all 4 infra images + digest-pin; (4) APP_ENV=production preflight.
> TDD MANDATORY — every fix covered by tests; security-sensitive hot-path fixes get BENCHMARKS proving no
> perf regression (user emphasis). One commit per task; "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"; `--no-verify`.

Waves: **W1** platform security primitives (foundations many lanes depend on) → **W2** parallel lanes
A(transport) ∥ B(audit-integrity) ∥ C(edge/secrets/preflight) ∥ D(infra/CI/supply-chain) → **W3** review+gate.

## W1 — platform security primitives (single lane, lands first)

| id | task |
|---|---|
| W1.1 | `config.Secret`: add `MarshalText() ([]byte,"[REDACTED]")` + `MarshalJSON` (covers json/yaml/text encoders); extend secret_test with json.Marshal/yaml.Marshal asserting no leak. **BENCH** none needed (cold path). MUST land first — every config touched below carries secrets. |
| W1.2 | config preflight: `config` package gains `ProductionGuard` helper + services call it: `APP_ENV` env (config.Config base? or per-service) — when `production`, a `Validate()` on gateway/service Config REJECTS: AuthDisabled=true, PG_DSN sslmode=disable, defaulted PG_DSN (postgres:postgres@localhost), plaintext S3 against non-localhost endpoint, CORS="*", ratelimit fail-open. Fail-fast at startup (apperr/clear error). Table test: each insecure default × production → start error; dev → allowed. |
| W1.3 | bearer-token size cap: auth.Middleware rejects Authorization tokens > `AUTH_MAX_TOKEN_BYTES` (default 8192) before jwt.Parse → 401 AUTH_UNAUTHENTICATED. **BENCH** BenchmarkVerify with 8KB vs over-cap (reject is cheap len check — prove negligible). Test: over-cap → 401 without parse. |
| W1.4 | regression tests for prior-closed defenses (audit asked for these): alg-confusion (HS256-signed-with-RSA-pubkey → ErrInvalidToken; alg:none → ErrInvalidToken) in auth_test; ListOrders IDOR (non-admin lists only own customer_id; admin all) in gateway_test; deep-nested-JSON seed added to decode_fuzz_test (Go 1.26 depth limit pinned). |

## W2-A — transport security (kafka/redis/JWKS)

| id | task |
|---|---|
| A1 | `kafka.Config` += `SASLMechanism` (none\|PLAIN\|SCRAM-SHA-256\|SCRAM-SHA-512), `SASLUser`, `SASLPassword config.Secret`, `TLSEnabled bool`, `TLSInsecureSkipVerify bool` (dev, loud doc); NewClient wires franz-go `kgo.SASL(...)` + `kgo.DialTLSConfig`. Env tags. Tests: config→kgo.Opt mapping unit; integration against Redpanda with SASL/SCRAM enabled (kafkatest variant or skip-if-unsupported — prove SASL handshake works). Servicekit/all callers pass through (Config embeds). |
| A2 | redis/cache: `cache.Config` += `Password config.Secret`, `TLSEnabled bool`; rueidis ClientOption Password+TLSConfig in cache.New AND the gateway's 2 extra rueidis clients (ratelimit, sse — deps.go). Env. Tests: config mapping; integration against redis-with-requirepass (testcontainer arg). |
| A3 | JWKS https-enforce: auth.NewJWKSVerifier rejects non-https JWKS URL unless `AUTH_ALLOW_INSECURE_JWKS=true` (default false; compose dev sets true for http keycloak). Error at construction (fail-closed). Test: http URL → error; with flag → allowed; https → allowed. |
| A4 | docs/ADR-0014 transport-security: SASL/mTLS/TLS knobs, JWKS-https rule, the principal-header forge caveat now has REAL controls; compose dev profile keeps plaintext (documented), a `compose.secure.yml` overlay OR documented env set showing SASL+TLS on. operations.md §Transport security. |

## W2-B — audit integrity

| id | task |
|---|---|
| B1 | audit append-only role: deploy/postgres/init.sql (+ migration note) — dedicated `audit_writer` role with INSERT-only on audit_log (`REVOKE UPDATE, DELETE`); app connects as it for audit writes OR (simpler, single-conn reality) document + a migration that REVOKEs UPDATE/DELETE from app on audit_log and routes cleanup through a privileged path. Decide minimal-correct: since one pool/role today, ship the REVOKE migration + a separate retention mechanism (cleanup uses a privileged DSN `PG_AUDIT_ADMIN_URL` OR partition-drop). Test: app role cannot UPDATE/DELETE audit_log (pgtest with role). |
| B2 | hash-chain tamper-evidence: audit_log += `prev_hash bytea`, `entry_hash bytea`; Record computes `entry_hash = sha256(prev_hash || canonical(entry))` under a per-(scope) serialized insert (advisory lock or `SELECT ... FOR UPDATE` of a chain-head row — must be tx-safe + concurrent-safe); `audit.VerifyChain(ctx, since)` walks + detects break. **BENCH** BenchmarkRecord (hash + chain-head contention) — prove acceptable overhead vs plain insert; document the serialization cost. Tests: chain verifies; tampered row (manual UPDATE via superuser in test) → VerifyChain detects; concurrent writers chain consistently. Expose via admin endpoint GET /v1/audit/verify (admin-only). |
| B3 | audit-on-denial + admin-read + attachment-access: emit audit entries (best-effort, OUTSIDE the failing tx — these are denials, no tx to join) on: authz/ownership 403 (authz behavior + gateway ownership checks), authn 401 (sample/rate-limited to avoid flood? — audit denials but guard against unauth flood: only audit AFTER a valid token fails authz, NOT every anonymous 401; document), admin DSAR read (QueryAudit/VerifyChain themselves audited), attachment upload/download/access. New audit action codes. Tests: denial → audit row; admin read → audit row. |
| B4 | principal multi-hop propagation: outbox.StampChainHeaders (or Enqueue) also stamps `principal-sub`/`principal-roles` from ctx so downstream consumers' audit attributes the real originating actor (same forge caveat — controls now exist via A1). consume.Typed extracts (already does). Test: gateway POST → orders consumes → orders' audit shows original principal sub, not anonymous. |
| B5 | DLT redrive allowlist: cmd/redrive validates destination (x-original-topic) against a `--allow-topics` allowlist (or derives from a known base-topic set); refuses forged/unknown destination; default = refuse-unknown. Test: forged original-topic record → refused, not republished. |

## W2-C — edge/secrets/SSE hardening

| id | task |
|---|---|
| C1 | attachment stored-XSS: blob PresignGet variant sets `ResponseContentDisposition=attachment` + `ResponseContentType=application/octet-stream`; Upload allowlists Content-Type (config allowlist, default common doc/image types) + rejects others 415; key built from PARSED uuid (canonical) not raw. Tests: presigned URL carries disposition=attachment; disallowed content-type → 415; canonical key. Upload streams to S3 with known length (map MaxBytesError → 413 not 500). |
| C2 | ratelimit fail-closed: `RATELIMIT_FAIL_CLOSED` env (default false=fail-open, documented trade-off) wired into gateway limiters via WithFailClosed; ALWAYS wire WithOnError → `ratelimit.errors` counter + WARN. Test: redis-error + fail-closed → deny; + counter moves. (Note: prod-preflight W1.2 can require fail-closed in production.) |
| C3 | SSE bulkhead sane default + HSTS: GATEWAY_SSE_MAX_STREAMS default → positive (e.g. 4096); SecurityHeaders adds `Strict-Transport-Security` (configurable, default on with 1y when not localhost — or always-on doc'd as ingress may override). idempotency empty-subject: JWKSVerifier rejects empty-sub tokens (or idempotency path refuses without subject). Tests: N+1 stream → 503; HSTS header present; empty-sub token → 401. **BENCH** none (TryAcquirePermit already O(1); confirm). |
| C4 | inbox retention ≥ topic retention guard: servicekit startup WARN/validate already exists? (round-1 had inbox-retention vs topic — verify); strengthen: production-guard asserts INBOX_RETENTION ≥ TOPIC_RETENTION (replay-after-cleanup defense). Test. |

## W2-D — infra images + supply-chain

| id | task |
|---|---|
| D1 | infra image bumps (docker-compose.yml + deploy): Keycloak 25.0.6→26.6.x (realm-export compat check — KC 26 import; smoke), Grafana 11.3.1→12.x (+ disable anonymous OR set admin password from _FILE; provisioning compat), Redpanda v24.2.7→v25.3.x (+ console v3.x — config migration), Jaeger 1.62→v2.x (collector-based image, OTLP config migration in otel-collector pipeline or jaeger config). Digest-pin the key images (comment w/ tag for readability). kafkatest Redpanda tag bump (test container — verify tests pass on v25.3). Smoke: `docker compose --profile observability --profile apps up` health green (do it, careful w/ host ports — use isolated project name). |
| D2 | digest-pin + Renovate note: pin golang/distroless/postgres/redis base images by digest in Dockerfile (comment tag); docs note on Renovate/Dependabot for digest bumps; k8s CHANGEME-digest addressed (sample digest or doc). |
| D3 | CI supply-chain: SHA-pin ALL github actions (checkout/setup-go/trivy/upload-artifact/codeql/buf/goreleaser/cosign/sbom — currently floating @vN); pin `@latest` tool installs (gofumpt, govulncheck, squawk-cli local lane, goimports) to exact versions matching CI; SBOM attestation note; verify govulncheck job blocking. YAML-validate. |
| D4 | redis/compose hardening: `read_only: true` + `cap_drop: [ALL]` on app services in compose (distroless compatible — verify probe/tmp needs); redis requirepass in compose (dev cred, _FILE-able) now that A2 supports it; redpanda SASL in a documented secure overlay (ties to A4). Smoke compose config -q. |

## W3 — final
review pass over round-8 diff (adversarial, security-focused) → fix MUST-FIX → full gate (lint 0, gofumpt,
short green, full -p 1 suite incl new SASL/audit-chain integration tests, benchmarks recorded + no regression,
compose all profiles + secure overlay valid, errgen/promtool/doc-test) → archive + memory. Then S1 (remote+CI) next round.
