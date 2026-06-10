# SP8: Testkit + Integrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`). Mature codebase — read existing patterns before writing; verify lib APIs with `go doc` and adapt; report deviations.

**Goal:** Add a documented testing toolkit (mocks/fakes/mock-servers/fixtures + examples), wire `blob`+`featureflags` via gateway order-attachments, fully wire Keycloak auth, and make docker-compose modular via profiles.

**Architecture:** New `platform/testkit/` (moq mocks + hand fakes + httptest mock servers + fixtures) consumed only by tests. Gateway gains binary attachment routes (blob + OpenFeature flag gate). Keycloak runs as the default IdP (compose auth-on + realm + token helper + real-token integration test). Compose split via profiles (core/observability/apps).

**Tech Stack:** matryer/moq · net/http/httptest · MinIO (blob) · open-feature/go-sdk · lestrrat jwx · testcontainers (keycloak/minio) · docker compose profiles · Taskfile.

**Spec:** `docs/superpowers/specs/2026-06-09-testkit-integrations-design.md`.

---

## Task 1: `platform/testkit/fakes` — hand in-memory test doubles

**Files:** Create `platform/testkit/fakes/fakes.go`, `platform/testkit/fakes/fakes_test.go`.

The fakes implement existing platform interfaces. Confirm exact signatures by reading: `platform/cqrs/caching.go` (`Cache`), `platform/blob/blob.go` (`ObjectStore`), `platform/outbox/message.go` (`Publisher`, `BatchPublisher`), `platform/auth/verifier.go` (`Verifier`) + `platform/auth/principal.go` (`Principal`).

- [ ] **Step 1: Write `fakes.go`** implementing:
  - `Cache` — `map[string][]byte` + `sync.RWMutex`, implements `cqrs.Cache` (`Get(ctx,key)([]byte,bool)`, `Set(ctx,key,val,ttl)`). Returns copies (no aliasing).
  - `ObjectStore` — in-mem `map[string][]byte` (+ content-types), implements `blob.ObjectStore` (`Put/Get/Delete/Exists/PresignGet/List`). `PresignGet` returns a fake `https://fake-blob/<key>` URL.
  - `Publisher` — collects `[]outbox.Message` under a mutex (`Messages()` accessor), implements `outbox.Publisher` (`Publish`) AND `outbox.BatchPublisher` (`PublishBatch`). Optional `FailNext bool` to simulate errors.
  - `Verifier` — implements `auth.Verifier`; `Verify(ctx, raw)` returns a configured `auth.Principal` (default subject/roles), or an error when `raw == ""` / a configured `RejectToken`.
  Add `var _ cqrs.Cache = (*Cache)(nil)` etc. compile assertions for all four.
- [ ] **Step 2: Write `fakes_test.go`** — unit tests (no infra, `-short`-safe): Cache set/get/miss + copy-no-alias; ObjectStore put/get/exists/delete/list/presign; Publisher collects + PublishBatch + FailNext; Verifier returns principal + rejects empty.
- [ ] **Step 3:** `go test -race ./platform/testkit/fakes/...` → PASS. `go build ./...`, `go vet`, `gofmt`, `golangci-lint run ./platform/testkit/...`.
- [ ] **Step 4: Commit** `git add platform/testkit/fakes && git commit -m "test(testkit): in-memory fakes for cache/blob/publisher/verifier"`.

---

## Task 2: `platform/testkit/mockhttp` — httptest mock servers

**Files:** Create `platform/testkit/mockhttp/mockhttp.go`, `mockhttp_test.go`.

- [ ] **Step 1: Write `mockhttp.go`:**
  - `JWKS(t *testing.T) *JWKSServer` — generates an RSA key, serves a JWKS at `/.well-known/jwks.json` (or `/certs`) via `httptest.NewServer`, `t.Cleanup` closes it. Methods: `URL() string` (JWKS url), `Sign(claims map[string]any) string` (signs an RS256 JWT with the key, `kid` set). Reuse the lestrrat jwx pattern from `platform/auth/auth_test.go` (read it). This lets any test mint tokens a `JWKSVerifier` accepts.
  - `Server(t *testing.T, handler http.Handler) *Recorder` — wraps `httptest.NewServer`, records each incoming request (method/path/body) into a thread-safe slice; `Recorder.URL`, `Recorder.Requests() []RecordedRequest`. For mocking an external HTTP dependency a service calls.
  - `JSON(t, status int, body any) http.HandlerFunc` — convenience handler returning a JSON response (for building mock endpoints).
- [ ] **Step 2: Write `mockhttp_test.go`:** `JWKS` — mint a token, parse it with jwx + the served JWKS, assert claims; `Server`/`Recorder` — hit it, assert recorded request; `JSON` handler returns the body.
- [ ] **Step 3:** `go test -race ./platform/testkit/mockhttp/...` PASS + build/vet/fmt/lint. (May need `go get github.com/lestrrat-go/jwx/v2` already present.)
- [ ] **Step 4: Commit** `test(testkit): httptest mock servers (JWKS + recording HTTP mock)`.

---

## Task 3: `platform/testkit/mocks` (moq) + `platform/testkit/fixtures`

**Files:** Create `platform/testkit/mocks/gen.go` (the `//go:generate` directives + doc), generated `*.go` mocks, `platform/testkit/fixtures/fixtures.go`, `fixtures_test.go`.

- [ ] **Step 1: moq mocks.** Install moq: `go run github.com/matryer/moq@latest` works via `go run` (add to go.mod tools or just use `go run`). In `platform/testkit/mocks/gen.go` add `//go:generate go run github.com/matryer/moq -out publisher_mock.go -pkg mocks ../../outbox Publisher` and similar for `cqrs.Cache`, `auth.Verifier`, `blob.ObjectStore`, `audit.Store`. Run `go generate ./platform/testkit/mocks/...`. Commit the generated `*_mock.go`. (Verify moq's CLI args via `go run github.com/matryer/moq@latest -h`; adapt `-pkg`/`-out`/source-dir/interface-name. moq generates a `XMock` struct implementing the interface with `XFunc` fields + call recorders.)
- [ ] **Step 2: fixtures.** `fixtures.go` — builders with functional options returning sane defaults:
  - `Order(opts ...OrderOpt) gatewayOrderView`? — fixtures should NOT import example services (keep testkit platform-only). Instead build PLATFORM-level fixtures: `OutboxMessage(opts...) outbox.Message`, `Principal(opts...) auth.Principal`, and proto event builders are example-level. Keep fixtures to platform types only: `OutboxMessage`, `Principal`, `Record(opts) kafka.Record`. Functional options like `WithID`, `WithRoles`, `WithTopic`.
- [ ] **Step 3:** `fixtures_test.go` — assert defaults + options apply. `go test -race ./platform/testkit/...` PASS; `go build ./...`; vet/fmt/lint clean. Confirm `go generate ./platform/testkit/mocks/...` is reproducible (re-run → no diff).
- [ ] **Step 4: Commit** `test(testkit): moq-generated mocks and platform fixtures`.

---

## Task 4: testing docs + worked examples + Taskfile targets

**Files:** Create `docs/testing.md`, `examples/testing/unit_example_test.go`, `examples/testing/functional_example_test.go`, `examples/testing/doc.go`; modify `Taskfile.yml`.

- [ ] **Step 1: `examples/testing/doc.go`** — package doc explaining this package holds reference test examples.
- [ ] **Step 2: `unit_example_test.go`** — a UNIT test template: define a tiny example "service" inline (or test a real platform handler) using `testkit/mocks` (e.g. a function that takes an `outbox.Publisher` and publishes; test with the moq `PublisherMock`, assert `Publish` called with expected args via the mock's call recorder). Heavily commented as "UNIT TEST TEMPLATE: no infra, mocks for collaborators, runs under -short." Must run under `-short`.
- [ ] **Step 3: `functional_example_test.go`** — a FUNCTIONAL test template: wire a small flow using `testkit/fakes` (FakePublisher + FakeCache) + a `testkit/mockhttp.Server` simulating an external HTTP dependency the code calls; assert end-to-end behavior across the slice without containers. Commented as "FUNCTIONAL TEMPLATE." Runs under `-short`.
- [ ] **Step 4: `docs/testing.md`** — the four-layer pyramid table, the `testing.Short()` convention (integration tests start with `if testing.Short() { t.Skip(...) }`), which double to use when (moq for strict call-assertions; fakes for stateful behavior; mockhttp for external HTTP; testcontainers for real infra), pointers to: the unit/functional examples, an integration example (`examples/orders/orders_test.go`), the e2e (`examples/e2e`). Document `task test:unit|integration|e2e|cover|gen:mocks`.
- [ ] **Step 5: `Taskfile.yml`** — add tasks: `test:unit` (`go test -short ./...`), `test:integration` (`go test ./...`), `test:e2e` (`go test ./examples/e2e/...`), `test:cover` (`go test -short -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1`), `gen:mocks` (`go generate ./platform/testkit/mocks/...`).
- [ ] **Step 6:** `go test -short ./examples/testing/...` PASS (fast). build/vet/fmt/lint clean. `task test:unit` runs green & fast.
- [ ] **Step 7: Commit** `docs(testing): testing pyramid guide + unit/functional example templates + task targets`.

---

## Task 5: blob + featureflags — gateway order attachments

**Files:** Create `examples/gateway/internal/attachments/attachments.go`, `attachments_test.go`; modify `examples/gateway/gateway.go` (wire blob + flags + mount routes).

Read first: `platform/blob/blob.go` (`ObjectStore`, `New`, `Config`), `platform/featureflags/flags.go` (`Flags`, `New`, `NewInMemory`/memprovider, `Bool`), `examples/gateway/gateway.go` (wiring + how the public mux + auth middleware are set up), `platform/httpx` (responses).

- [ ] **Step 1: `attachments.go`** — `Handler` struct holding `store blob.ObjectStore`, `flags *featureflags.Flags`, `flagKey string` (default `order-attachments-enabled`), `presignTTL time.Duration`. Methods:
  - `Upload(w,r)` — if `!flags.Bool(ctx, flagKey, false)` → `httpx.Error(w,404,...)`; parse `id` (chi URLParam) + filename (from `X-Filename` header or multipart `file`); read body capped (rely on the server MaxBytes middleware); `store.Put(ctx, "orders/"+id+"/"+filename, body, size, contentType)`; `httpx.JSON(w,201,{key})`.
  - `Download(w,r)` — flag-gate; build key from `id`+`name`; `url,err := store.PresignGet(ctx, key, ttl)`; on not-found → 404; else `http.Redirect(w,r,url,302)`.
  - `Mount(r chi.Router)` — `r.Post("/orders/{id}/attachment", h.Upload)`, `r.Get("/orders/{id}/attachment/{name}", h.Download)`.
- [ ] **Step 2: `attachments_test.go`** (functional, `-short`): build `Handler` with `testkit/fakes.ObjectStore` + a `featureflags` in-memory `Flags` with the flag ON → POST then GET returns 302 to the fake presign URL; flag OFF → POST/GET 404. (Use chi router + httptest.)
- [ ] **Step 3: wire into `gateway.go`** — build `blob.New(ctx, cfg.S3)` (add `blob.Config` to gateway config as `S3`); if it errors (S3 unconfigured), log warn + skip mounting attachments (graceful, like cache). Build `featureflags` via in-memory provider seeded `order-attachments-enabled=true` (document swap). Mount `attachments.Handler.Mount` on the public mux behind the existing auth middleware. Add config fields + env (`S3_*` already standardized; flag provider needs none for in-mem).
- [ ] **Step 4: integration test** `examples/gateway/attachments_integration_test.go` (testcontainers MinIO, `testing.Short()` skip): real `blob.New` against MinIO container; upload bytes via the handler, GET → follow the 302 → download from MinIO → bytes match. (Reuse the minio testcontainers pattern from `platform/blob/blob_test.go`.)
- [ ] **Step 5:** `go build ./...`; `go test -short ./examples/gateway/...` (functional) PASS; `go test ./examples/gateway/... -run Attachment` (integration, Docker) PASS; keep existing gateway tests + e2e green (`go test ./examples/gateway/... ./examples/e2e/...`). vet/fmt/lint clean.
- [ ] **Step 6: Commit** `feat(gateway): order attachments via blob (MinIO) gated by an OpenFeature flag`.

---

## Task 6: Keycloak fully wired

**Files:** Modify `deploy/keycloak/realm-export.json`, `docker-compose.yml`, `Taskfile.yml`; create `examples/gateway/keycloak_test.go`.

- [ ] **Step 1: realm export** — ensure realm `app`: client `gateway` (public client, `directAccessGrantsEnabled: true` for password grant); realm roles `user`,`admin`; user `demo`/`demo` with realm role `user`; an **audience mapper** so issued tokens include `aud: "gateway"` (add a protocol mapper of type `oidc-audience-mapper` with `included.client.audience=gateway`, or a client scope). Verify by reading the current realm-export.json and editing minimally. (The gateway verifier is configured with `audience=gateway`; the token MUST carry that aud.)
- [ ] **Step 2: compose** — gateway service env: `GATEWAY_AUTH_DISABLED=false`, `GATEWAY_JWKS_URL=http://keycloak:8080/realms/app/protocol/openid-connect/certs`, `GATEWAY_JWKS_ISSUER=http://keycloak:8080/realms/app`, `GATEWAY_JWKS_AUDIENCE=gateway`. Ensure gateway `depends_on` keycloak healthy. `docker compose config` valid.
- [ ] **Step 3: Taskfile `token`** — `curl -s -d client_id=gateway -d username=demo -d password=demo -d grant_type=password http://localhost:8080/realms/app/protocol/openid-connect/token | jq -r .access_token` (note: requires `jq`; document). Add a `token` task + a usage comment.
- [ ] **Step 4: Keycloak integration test** `examples/gateway/keycloak_test.go` (`testing.Short()` skip; Docker). Verify a Keycloak testcontainers module: `go doc github.com/testcontainers/testcontainers-go/modules/keycloak` — if it exists use it (`keycloak.Run(ctx, image, keycloak.WithRealmImportFile("../../deploy/keycloak/realm-export.json"), ...)` + `AuthServerURL`); ELSE run a generic container `quay.io/keycloak/keycloak:25.0` with cmd `start-dev --import-realm`, mount the realm file, wait on `/realms/app/.well-known/openid-configuration`. Steps in the test: start Keycloak; build gateway `NewApp` with auth enabled + JWKS pointing at the container realm certs (+ issuer/audience matching the container's base URL — note the issuer in the token is the container's external URL, so set `GATEWAY_JWKS_ISSUER` to the container base + `/realms/app`); obtain a token via password grant (`demo/demo`); assert: `POST /orders` with `Authorization: Bearer <token>` → 202; no header → 401; a tampered/expired token → 401; (optional) a user lacking the role → 403. Generous timeout (~90s, Keycloak is slow). `go get` the keycloak module if used.
- [ ] **Step 5:** `go build ./...`; `go test ./examples/gateway/... -run Keycloak` (Docker) PASS; `go test -short ./...` still fast+green (the Keycloak test skips under -short); existing gateway tests + e2e green. vet/fmt/lint clean. `docker compose config` valid.
- [ ] **Step 6: Commit** `feat(keycloak): enable gateway auth against Keycloak by default; realm audience/role; token helper + real-token integration test`.

---

## Task 7: docker-compose profiles + Taskfile up targets + docs

**Files:** Modify `docker-compose.yml`, `Taskfile.yml`, `README.md`, `docs/operations.md`.

- [ ] **Step 1: profiles** — add `profiles: ["observability"]` to `otel-collector`, `jaeger`, `prometheus`, `grafana`, `pyroscope` (and their volumes are fine). Add `profiles: ["apps"]` to `gateway`, `orders`, `payments`, `notifications`. Leave core infra (postgres, redpanda, redpanda-console, redis, minio, minio-setup, keycloak) profile-less (always up). Ensure apps' `depends_on` doesn't force-start observability (depends_on within the same profile or core only; if an app depends_on otel-collector, that pulls obs in — so REMOVE otel-collector from apps' depends_on, keep core deps). Apps must run core-only: rely on lazy OTLP connect (already `WithInsecure`, lazy) — confirm services start without the collector.
- [ ] **Step 2: validate profiles** — `docker compose config` (core); `docker compose --profile observability config`; `docker compose --profile apps config`; `docker compose --profile observability --profile apps config` — all valid.
- [ ] **Step 3: Taskfile** — `up` (`docker compose up -d` = core), `up:obs` (`docker compose --profile observability up -d`), `up:apps` (`docker compose --profile apps up -d --build`), `up:full` (`docker compose --profile observability --profile apps up -d --build`), `down` (`docker compose --profile observability --profile apps down -v`), `logs`.
- [ ] **Step 4: docs** — README quickstart + `docs/operations.md`: the profile matrix table (what each profile starts, the commands, that apps run core-only with traces uncollected). Update the existing quickstart to use `task up` (core) then `task up:full`.
- [ ] **Step 5: Commit** `feat(compose): split into core/observability/apps profiles + task targets + docs`.

---

## Task 8: final verification + review

- [ ] Whole-repo: `go build ./...`, `go vet ./...`, `gofmt -l .`, `golangci-lint run ./...`, `go test -short ./...` (fast lane), `go generate ./platform/testkit/mocks/...` (reproducible — no diff), `docker compose config` (+ each profile). 
- [ ] Run the new integration tests (Docker): `go test ./examples/gateway/... ./platform/testkit/...` and the e2e (`go test ./examples/e2e/...`).
- [ ] Confirm: `blob`+`featureflags` now imported by `examples/gateway` (`go list -deps ./examples/gateway/... | grep -E 'platform/(blob|featureflags)'`), Keycloak auth enabled by default in compose, profiles work, testkit examples documented.
- [ ] Commit any fixes. Dispatch a final adversarial review.

---

## Self-Review (completed)
- **Spec coverage:** testing toolkit (T1-T3) + docs/examples/tasks (T4) + blob+flags attachments (T5) + Keycloak full (T6) + compose profiles (T7) + verify (T8) — all spec sections mapped.
- **Placeholders:** none; every task has files + key code shape + tests + verification + commit. Library-API-variance points (moq CLI args, keycloak testcontainers module presence, realm audience mapper schema) each carry an explicit "verify with go doc / adapt + report" instruction rather than a vague gap.
- **Type consistency:** fakes/mocks implement the exact existing interfaces (`cqrs.Cache`, `blob.ObjectStore`, `outbox.Publisher`/`BatchPublisher`, `auth.Verifier`, `audit.Store`); attachments use `blob.ObjectStore` + `featureflags.Flags.Bool`; gateway wiring matches the existing graceful-degradation pattern. Flag key `order-attachments-enabled` consistent across T5. Keycloak audience `gateway` consistent between realm mapper (T6.1), compose (T6.2), and the verifier config (T6.4).
