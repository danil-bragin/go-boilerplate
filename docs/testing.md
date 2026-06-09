# Testing Guide

Practical reference for every layer of the testing pyramid in this repository.
Read this before writing a new test; copy the linked examples rather than inventing patterns.

---

## 1. Testing Pyramid

| Layer | Scope | Infra required | Test doubles | When runs |
|---|---|---|---|---|
| **Unit** | One function / method | None | moq mocks (`mocks.*`) | Always (`-short`) |
| **Functional** | A use-case slice across several collaborators | None | Hand fakes (`fakes.*`) + `mockhttp.*` | Always (`-short`) |
| **Integration** | A service wired to real DB / broker | Docker (testcontainers) | Real infra containers | Full `go test ./...` only |
| **E2E** | Multiple services via HTTP/gRPC/Kafka | Full stack (`docker compose up`) | None | `task test:e2e` / CI nightly |

---

## 2. The `-short` Convention

Integration tests guard themselves with:

```go
func TestMyIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("requires Docker")
    }
    // ... testcontainers setup ...
}
```

This creates two CI lanes:

- **Fast lane** — `go test -short ./...`: runs unit + functional tests only.
  No Docker daemon required. Should complete in seconds.
- **Full lane** — `go test ./...`: also runs integration tests that spin up
  real Postgres, Redpanda, Redis, and MinIO via testcontainers.

Use `task test:unit` for the fast lane and `task test:integration` for the full lane (see §5).

---

## 3. Choosing a Test Double

| Need | Use | Package |
|---|---|---|
| Assert exact arguments / call count | moq mock | `platform/testkit/mocks` |
| Stateful in-memory behaviour (cache, publisher, store) | Hand fake | `platform/testkit/fakes` |
| External HTTP dependency (REST API, JWKS endpoint, webhook) | `mockhttp.Server` + `mockhttp.JSON` | `platform/testkit/mockhttp` |
| Auth — live RS256 JWKS + JWT minting | `mockhttp.JWKS(t)` | `platform/testkit/mockhttp` |
| Real Postgres | `pgtest.NewDSN(t)` | `platform/pg/pgtest` |
| Real Kafka / Redpanda | `kafkatest.NewRedpanda(t)` | `platform/kafka/kafkatest` |
| Real Redis | `testcontainers-go/modules/redis` directly (see `platform/cache/cache_test.go`) | `github.com/testcontainers/testcontainers-go/modules/redis` |
| Real MinIO | `testcontainers-go/modules/minio` directly (see `platform/blob/blob_test.go`) | `github.com/testcontainers/testcontainers-go/modules/minio` |
| Real Keycloak | generic testcontainers container (see `examples/gateway/keycloak_test.go`) | `github.com/testcontainers/testcontainers-go` |
| Canonical test data | Builder functions | `platform/testkit/fixtures` |

### Quick reference

```go
// moq mock — strict call recording
pub := &mocks.PublisherMock{
    PublishFunc: func(_ context.Context, _ outbox.Message) error { return nil },
}
pub.PublishCalls() // []struct{ Ctx, Msg }

// hand fake — stateful, inspect state
pub := fakes.NewPublisher()
pub.FailNext = true          // inject next failure
pub.Messages()               // []outbox.Message

cache := fakes.NewCache()
cache.Set(ctx, "k", data, time.Minute)
data, ok := cache.Get(ctx, "k")

// mockhttp — external HTTP + request recorder
rec := mockhttp.Server(t, mockhttp.JSON(http.StatusOK, body))
rec.URL()       // "http://127.0.0.1:<port>"
rec.Requests()  // []RecordedRequest{Method, Path, Body}

// JWKS — auth integration
js := mockhttp.JWKS(t)
verifier, _ := auth.NewJWKSVerifier(ctx, js.URL(), "iss", "aud")
token := js.Sign(map[string]any{"iss": "iss", "aud": "aud", "sub": "u1", "roles": []string{"admin"}})

// fixtures — test data builders
msg := fixtures.OutboxMessage(fixtures.WithEventType("OrderCreated"))
p   := fixtures.Principal(fixtures.WithRoles([]string{"admin"}))
rec := fixtures.Record(fixtures.WithTopic("orders"))
```

---

## 4. Examples to Copy

| Layer | File | What it shows |
|---|---|---|
| Unit | [`examples/testing/unit_example_test.go`](../examples/testing/unit_example_test.go) | moq mock, success + error path, `PublishCalls()` assertion |
| Functional | [`examples/testing/functional_example_test.go`](../examples/testing/functional_example_test.go) | `mockhttp.Server` external HTTP, `fakes.Cache`, `fakes.Publisher`, behaviour assertions |
| Integration | [`examples/orders/orders_test.go`](../examples/orders/orders_test.go) | testcontainers Postgres + Redpanda, full service wire-up |
| HTTP integration | [`examples/gateway/gateway_test.go`](../examples/gateway/gateway_test.go) | HTTP handler integration, JWKS auth, testcontainers Postgres |
| E2E | [`examples/e2e/`](../examples/e2e/) | Full choreography across all services |

---

## 5. Running Tests

```bash
# Fast lane — unit + functional only (no Docker)
task test:unit

# Full lane — includes integration tests (requires Docker)
task test:integration

# E2E only
task test:e2e

# Coverage report (fast lane)
task test:cover

# Regenerate moq mocks
task gen:mocks
```

Raw `go test` equivalents:

```bash
go test -short ./...                                  # fast lane
go test ./...                                         # full lane
go test ./examples/e2e/...                            # e2e
go test -short -coverprofile=coverage.out ./... \
  && go tool cover -func=coverage.out | tail -1       # coverage summary
```

---

## 6. Mock Regeneration

Mocks are committed to source control and are reproducible. Regenerate after
changing a platform interface:

```bash
go generate ./platform/testkit/mocks/...
# or
task gen:mocks
```

The `go:generate` directives live in `platform/testkit/mocks/gen.go` and pin
the moq version (`v0.5.3`) so output is deterministic across machines.

Never edit generated mock files by hand — they will be overwritten on the next
`go generate` run.
