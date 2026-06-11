# ADR 0014 — Transport security: SASL/TLS, Redis auth, JWKS-https enforcement

**Status:** Accepted
**Date:** 2026-06-11

## Context

The round-8 security audit found the app layer strong (IDOR / alg-confusion /
idempotency / injection all clean) but the **trust model documented rather than
enforced**. Three concrete gaps:

1. **Kafka was always `PLAINTEXT` with no SASL.** The principal-propagation
   headers (`principal-sub` / `principal-roles`, ADR see ARCHITECTURE
   "Cross-service principal propagation") are transport metadata that *any*
   client with produce rights can forge. The stated mitigation — "restrict
   produce with broker ACLs, authenticate inter-service connections
   (mTLS/SASL)" — had **no code knobs**, so the perimeter could not actually be
   built.
2. **Redis (cache, rate-limit, SSE pub/sub) connected with no password and no
   TLS**, so credentials and cached payloads crossed the network in cleartext
   and any reachable client could read/poison the cache.
3. **The JWKS URL could be plaintext `http://`.** A man-in-the-middle on that
   fetch can swap the public keys and then mint tokens this verifier trusts —
   a full authentication bypass.

## Decision

Add **opt-in, fail-safe-defaulted** transport-security knobs across the three
transports, wired into the franz-go / rueidis / jwx clients. Defaults preserve
the plaintext dev experience; production turns them on.

### Kafka SASL + TLS (`platform/messaging/kafka`)

`kafka.Config` gains:

| field | env | default | meaning |
|---|---|---|---|
| `SASLMechanism` | `KAFKA_SASL_MECHANISM` | `""` | `""`=none, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `SASLUser` | `KAFKA_SASL_USER` | `""` | SASL username |
| `SASLPassword` | `KAFKA_SASL_PASSWORD` | `""` | `config.Secret` (redacted in logs/JSON/YAML) |
| `TLSEnabled` | `KAFKA_TLS_ENABLED` | `false` | wrap broker conn in TLS (min TLS 1.2) |
| `TLSInsecureSkipVerify` | `KAFKA_TLS_INSECURE_SKIP_VERIFY` | `false` | **DEV ONLY** — disables cert verification |

`NewClient` maps these to franz-go options (`saslTLSOpts`):

- `PLAIN` → `kgo.SASL(plain.Auth{User,Pass}.AsMechanism())`
- `SCRAM-SHA-256` → `kgo.SASL(scram.Auth{User,Pass}.AsSha256Mechanism())`
- `SCRAM-SHA-512` → `kgo.SASL(scram.Auth{User,Pass}.AsSha512Mechanism())`
- `TLSEnabled` → `kgo.DialTLSConfig(&tls.Config{MinVersion: TLS12, InsecureSkipVerify})`

An empty mechanism leaves the connection unauthenticated (back-compat); an
**unrecognised** mechanism is a startup error (fail closed). The mapping is
proven end-to-end against a SASL/SCRAM Redpanda in `kafkatest.NewRedpandaSASL`.

### Redis password + TLS (`platform/storage/cache`)

`cache.Config` gains `Password config.Secret` (`REDIS_PASSWORD`) and
`TLSEnabled bool` (`REDIS_TLS_ENABLED`). A single seam `cache.BuildRueidisOption`
maps them to `rueidis.ClientOption{Password, TLSConfig}` (TLS 1.2 minimum). All
**three** rueidis clients route through it: the cache's L2 client plus the
gateway's dedicated rate-limit and SSE pub/sub clients (`examples/gateway/deps.go`).
rueidis dials eagerly, so a wrong/missing password fails closed at construction.

### JWKS https enforcement (`platform/security/auth`)

`NewJWKSVerifier` rejects a non-`https` JWKS URL **at construction** (before any
fetch) unless `WithAllowInsecureJWKS(true)` / `AUTH_ALLOW_INSECURE_JWKS=true`
opts into the dev escape hatch. The gateway threads `AuthAllowInsecureJWKS`;
docker-compose dev sets it true for the internal `http://keycloak` realm.

## Consequences

- **The principal-forge caveat now has real controls.** Restricting Kafka
  produce access (broker SASL/ACLs) plus authenticating inter-service
  connections is now configuration, not a TODO. The headers remain *metadata*,
  not authentication — never make authz decisions from them for data that may
  originate outside the SASL/ACL perimeter (the caveat stands; it is now
  *enforceable*).
- Dev stays plaintext (all defaults off) — no friction for `docker compose up`.
  See `operations.md` § Transport security for the production env block and the
  secure-overlay snippet.
- `TLSInsecureSkipVerify` and `AUTH_ALLOW_INSECURE_JWKS` are loud dev-only
  escape hatches; the production preflight (W1.2 `ProductionGuard`) is the place
  to additionally *reject* them when `APP_ENV=production`.
- `SASLPassword` and `REDIS_PASSWORD` are `config.Secret`, so they never appear
  in a config dump, slog line, or JSON/YAML serialization (ADR via the W1
  `config.Secret` redaction work).
