# Testing Guide

Practical reference for every layer of the testing pyramid in this repository.
Read this before writing a new test; copy the linked examples rather than inventing patterns.

---

## 1. Testing Pyramid

| Layer | Scope | Infra required | Test doubles | When runs |
|---|---|---|---|---|
| **Unit** | One function / method | None | moq mocks (`mocks.*`) | Always (`-short`) |
| **Functional** | A use-case slice across several collaborators | None | Hand fakes (`fakes.*`, incl. `fakes.Broker`) + `mockhttp.*` | Always (`-short`) |
| **Integration** | A service wired to real DB / broker | Docker (testcontainers) | Real infra containers | Full `go test ./...` only |
| **E2E** | All four services in-process, full choreography over real Kafka + Postgres | Docker (testcontainers — **self-provisioned**, no `docker compose` needed) | None (capture notifier only) | Full `go test ./...` / `just test-e2e` |

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
  No Docker daemon required. Should complete in seconds. The example services
  model this lane too: each has `-short` tests driving the real transport
  pipeline through `fakes.Broker` (no containers).
- **Full lane** — `go test ./...`: also runs integration tests (and the e2e
  choreography test) that spin up real Postgres, Redpanda, Redis, and
  SeaweedFS via testcontainers. Both lanes run on every CI push/PR — there is
  no nightly-only suite.

Use `just test-unit` for the fast lane and `just test-integration` for the full lane (see §5).

---

## 3. Choosing a Test Double

| Need | Use | Package |
|---|---|---|
| Assert exact arguments / call count | moq mock | `platform/testkit/mocks` |
| Stateful in-memory behaviour (cache, publisher, store) | Hand fake | `platform/testkit/fakes` |
| Kafka without Docker — drive `kafka.HandlerFunc` / `consume.Typed` pipelines | `fakes.Broker` (+ `consume.WithoutInbox()`) | `platform/testkit/fakes` |
| External HTTP dependency (REST API, JWKS endpoint, webhook) | `mockhttp.Server` + `mockhttp.JSON` | `platform/testkit/mockhttp` |
| Auth — live RS256 JWKS + JWT minting | `mockhttp.JWKS(t)` | `platform/testkit/mockhttp` |
| Real Postgres | `pgtest.NewDSN(t)` | `platform/storage/pg/pgtest` |
| Real Kafka / Redpanda | `kafkatest.NewRedpanda(t)` | `platform/messaging/kafka/kafkatest` |
| Real Redis | `testcontainers-go/modules/redis` directly (see `platform/storage/cache/cache_test.go`) | `github.com/testcontainers/testcontainers-go/modules/redis` |
| Real S3 (SeaweedFS) | generic testcontainers container (see `platform/storage/blob/blob_test.go`) | `github.com/testcontainers/testcontainers-go` |
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

// fakes.Broker — in-memory Kafka: outbox.Publisher in, kafka.HandlerFunc out,
// synchronous delivery with real event-type/message-id headers. Pair with
// consume.WithoutInbox() to run consume.Typed pipelines without a database.
// (See examples/payments/internal/transport/consumer_test.go.)
broker := fakes.NewBroker()
broker.Subscribe("orders.events", handler)        // handler: kafka.HandlerFunc
_ = broker.Publish(ctx, outboxMsg)                // relay-style publish path
recs := broker.Records("orders.events")           // delivered records

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
| Fast-lane transport | [`examples/payments/internal/transport/consumer_test.go`](../examples/payments/internal/transport/consumer_test.go) | `fakes.Broker` + `consume.WithoutInbox()` — real decode→dispatch pipeline, no Docker |
| Integration | [`examples/orders/orders_test.go`](../examples/orders/orders_test.go) | testcontainers Postgres + Redpanda, full service wire-up |
| HTTP integration | [`examples/gateway/gateway_test.go`](../examples/gateway/gateway_test.go) | HTTP handler integration, JWKS auth, testcontainers Postgres |
| E2E | [`examples/e2e/`](../examples/e2e/) | Full choreography across all services |

---

## 5. Running Tests

```bash
# Fast lane — unit + functional only (no Docker)
just test-unit

# Full lane — includes integration tests (requires Docker)
just test-integration

# E2E only (self-provisions Redpanda + Postgres via testcontainers; needs Docker, not compose)
just test-e2e

# Coverage report (fast lane)
just test-cover

# Regenerate moq mocks
just gen-mocks

# Scaffolding smoke test (new-service + rename-module --check) — same gate as CI
just test-scaffold

# Doc drift gate: compile the code blocks in docs/adding-a-service.md
just doc-test
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
just gen-mocks
```

The `go:generate` directives live in `platform/testkit/mocks/gen.go` and pin
the moq version (`v0.5.3`) so output is deterministic across machines.

Never edit generated mock files by hand — they will be overwritten on the next
`go generate` run.
