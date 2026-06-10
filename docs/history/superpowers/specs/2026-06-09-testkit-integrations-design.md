# SP8: Testkit + Integrations — Design

Status: approved (2026-06-09). Scope: testing flow/toolkit, wire blob+featureflags, full Keycloak, compose profiles.

## Goals
1. **Testing flow**: documented pyramid (unit/functional/integration/e2e) + reusable `platform/testkit` (moq mocks, fakes, mock HTTP servers, fixtures) + one worked example per layer + Taskfile targets.
2. **Close dead-package gap**: wire `blob` + `featureflags` into the gateway via an "order attachments" feature gated by an OpenFeature flag.
3. **Keycloak fully wired**: gateway auth ENABLED by default against the realm; roles→RBAC; `task token` helper; Keycloak integration test with a real token.
4. **Compose flexibility**: docker-compose profiles (core / observability / apps) + Taskfile targets.

## 1. Testing toolkit + flow

### `platform/testkit/` (shippable test support)
- `mocks/` — moq-generated mocks (`//go:generate moq -out X.go -pkg mocks ...`) for: `outbox.Publisher`, `cqrs.Cache`, `auth.Verifier`, `blob.ObjectStore`, `audit.Store`. Generated + committed. `moq` installed via `go run github.com/matryer/moq` (no runtime dep).
- `fakes/` — hand in-memory impls: `Cache` (map+mutex, implements cqrs.Cache), `ObjectStore` (in-mem map, implements blob.ObjectStore), `Publisher` (collects outbox.Message, implements outbox.Publisher + BatchPublisher), `Verifier` (returns a fixed auth.Principal, implements auth.Verifier).
- `mockhttp/` — httptest builders: `JWKS(t) (url string, sign func(claims) string)` (RSA keypair + JWKS endpoint + token signer, reusing the auth-test pattern); `Server(t, handler) *Recorder` (generic external-HTTP mock that records requests for assertions).
- `fixtures/` — builders: `Order(opts...)`, `CreateOrderCommand(opts...)`, `OrderCreatedEvent(opts...)` returning domain/proto values with sane defaults + functional options.

Boundary: `testkit` imports platform interfaces; only test code imports `testkit`. No production code depends on it. No import cycles (interfaces live in their own packages; mocks/fakes import them one-way).

### `docs/testing.md`
Explains the four layers, when to use each double (mock vs fake vs real), the `testing.Short()` convention (already enforced: integration tests skip under `-short`), how to add a test of each type, and points at the worked examples.

### Worked examples (templates, heavily commented)
- **Unit**: a small example under `examples/gateway/internal/app` or a dedicated `examples/testing/unit_example_test.go` — a cqrs command/query handler tested with moq mocks for its ports; pure, `-short`.
- **Functional**: a handler/HTTP-handler test wired with `testkit/fakes` for all ports + a `testkit/mockhttp` mock for an external HTTP dependency; no containers; `-short`.
- **Integration**: the canonical testcontainers pattern (reference an existing one, e.g. `examples/orders/orders_test.go`, and add a concise commented reference in docs).
- **E2E**: reference `examples/e2e`.

### Taskfile targets
`test:unit` (`go test -short ./...`), `test:integration` (`go test ./...` — full, Docker), `test:e2e` (`go test ./examples/e2e/...`), `test:cover` (coverage), `gen:mocks` (`go generate ./platform/testkit/...`).

## 2. blob + featureflags: order attachments (gateway)

- Gateway wires `blob.New(ctx, cfg.S3)` (MinIO/S3) and an OpenFeature `featureflags.Flags` (in-memory provider seeded with `order-attachments-enabled=true`; documented swap to flagd/cloud). Both optional: if S3 unconfigured or cache unreachable, log warn + disable the feature gracefully (consistent with the existing cache-optional pattern).
- New plain-chi routes on the gateway public mux (outside the JSON strict API — binary/multipart):
  - `POST /orders/{id}/attachment` (multipart form field `file`, or raw body + `X-Filename` header) → if flag off → 404; else validate order exists in read model → `blob.Put(ctx, "orders/{id}/{filename}", body, size, contentType)` → 201 `{key}`.
  - `GET /orders/{id}/attachment/{name}` → if flag off → 404; else `blob.PresignGet(ctx, key, 5m)` → 302 redirect to the presigned URL (or 200 `{url}` — choose 302 redirect; document).
- Auth/authz: attachment routes require auth (same middleware) and a role (`order:write` or reuse `order:create` via RBAC). Gated additionally by the flag.
- Tests: integration (MinIO testcontainer) — upload→presign→GET-follows-redirect round-trip; flag-off→404; functional — handler with `testkit/fakes.ObjectStore` + fake flags.

## 3. Keycloak fully wired

- `deploy/keycloak/realm-export.json`: ensure realm `app`, client `gateway` (public, direct-access-grants ON for password flow), roles `user`/`admin`, user `demo`/`demo` with role `user` (and an `admin` user optional). Audience: configure the token `aud` to include `gateway` (client scope / audience mapper) so `auth.NewJWKSVerifier(audience="gateway")` validates.
- `docker-compose.yml` gateway env: `GATEWAY_AUTH_DISABLED=false`, `GATEWAY_JWKS_URL=http://keycloak:8080/realms/app/protocol/openid-connect/certs`, `GATEWAY_JWKS_ISSUER=http://keycloak:8080/realms/app`, `GATEWAY_JWKS_AUDIENCE=gateway`.
- `Taskfile.yml` `token`: `curl -s -d client_id=gateway -d username=demo -d password=demo -d grant_type=password http://localhost:8080/realms/app/protocol/openid-connect/token | jq -r .access_token`. Document `curl -H "Authorization: Bearer $(task token)" localhost:18080/orders ...`.
- **Integration test** `examples/gateway/keycloak_test.go` (testcontainers): start Keycloak importing the realm (verify `testcontainers-go/modules/keycloak` exists; else generic container `quay.io/keycloak/keycloak` with `--import-realm` + a wait strategy on the realm endpoint). Build the gateway with auth enabled pointing at the container JWKS. Obtain a token via the password grant. Assert: valid token + role → POST /orders 202; missing/invalid token → 401; token whose user lacks the role → 403. Gated by `testing.Short()` skip. Generous timeout (~60s for Keycloak startup).

## 4. Compose profiles

- Assign `profiles: ["observability"]` to otel-collector, jaeger, prometheus, grafana, pyroscope. Assign `profiles: ["apps"]` to gateway, orders, payments, notifications. Core infra (postgres, redpanda, redpanda-console, redis, minio, minio-setup, keycloak) has no profile → always started.
- Apps must tolerate core-only (no observability): the OTLP exporter is lazy/non-fatal (already `WithInsecure`, connect is lazy); set apps' `OTEL_ENABLED` appropriately or rely on lazy connect. Document that `--profile apps` without `--profile observability` runs but traces/metrics aren't collected.
- `Taskfile.yml`: `up` (core), `up:obs` (`--profile observability`), `up:apps` (`--profile apps`), `up:full` (both profiles), `down`, `logs`.
- Update README quickstart + `docs/operations.md` with the profile matrix.

## Out of scope / deferred
- flagd container (in-memory provider only; documented swap). Distributed/per-IP rate limit. Multipart streaming for huge files (cap via existing MaxBytes). Admin user flows in Keycloak beyond the demo.

## Testing/verification
- All new tests pass; `go test -short ./...` (fast lane) stays green and fast; integration tests (incl. Keycloak, MinIO attachments) pass with Docker; e2e still green; build/vet/gofmt/golangci-lint clean; `docker compose config` valid for all profile combinations; `go generate ./platform/testkit/...` reproduces the committed mocks.
