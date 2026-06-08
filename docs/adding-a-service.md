# Adding a New Service

This guide walks through adding a new service (e.g. `inventory`) to the boilerplate. The `examples/orders` service is the canonical reference — read its source alongside this guide.

---

## 1. Define the protobuf contract

Create your event and command types under `proto/<domain>/v1/`:

```
proto/
└── inventory/
    └── v1/
        └── inventory.proto
```

Register the new path in `buf.yaml` if it is not already under the `proto` module path. Then generate:

```bash
task buf        # buf lint && buf generate
```

Generated Go types appear in `gen/proto/inventory/v1/`.

---

## 2. Create the service directory structure

```
examples/inventory/
├── cmd/
│   └── inventory/
│       └── main.go          # wire + signal handling
├── internal/
│   ├── migrations/
│   │   └── migrations.go    # embed SQL migrations (goose)
│   ├── store/
│   │   ├── sqlc.yaml
│   │   ├── queries/         # .sql files
│   │   └── gen/             # sqlc output (committed)
│   ├── app/
│   │   └── reserve_item.go  # command handler + Decorate
│   └── transport/
│       └── consumer.go      # Kafka HandlerFunc wrapping inbox.ProcessOnce
├── inventory.go             # NewApp / Start / Stop / Closer
└── inventory_test.go        # integration test (testcontainers)
```

---

## 3. Write migrations

Create SQL migrations in `examples/inventory/internal/migrations/`. Use goose format:

```sql
-- +goose Up
CREATE TABLE inventory_items (
    id          UUID PRIMARY KEY,
    sku         TEXT NOT NULL,
    quantity    INT  NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox_messages (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        BYTEA       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

CREATE TABLE inbox (
    consumer   TEXT NOT NULL,
    message_id TEXT NOT NULL,
    PRIMARY KEY (consumer, message_id)
);

-- +goose Down
DROP TABLE inbox;
DROP TABLE outbox_messages;
DROP TABLE inventory_items;
```

Embed them in `migrations.go`:

```go
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
```

Services call `goose.Up` at startup (see `orders.NewApp` for the pattern).

---

## 4. Write sqlc queries and generate

Edit `examples/inventory/internal/store/sqlc.yaml` (copy from orders), write `.sql` queries in `store/queries/`, then:

```bash
task sqlc    # runs sqlc generate for all services
```

Commit the generated `gen/` output.

---

## 5. Write command/query handlers with CQRS behaviors

```go
// examples/inventory/internal/app/reserve_item.go

type ReserveItem struct {
    ItemID   string `validate:"required"`
    Quantity int    `validate:"gt=0"`
}
type ReserveItemResult struct{ ItemID string }

func ReserveItemHandler(pool *pg.Pool, outboxRepo *outbox.Repository) cqrs.HandlerFunc[ReserveItem, ReserveItemResult] {
    return func(ctx context.Context, cmd ReserveItem) (ReserveItemResult, error) {
        // pg.FromContext pulls the ambient transaction from ctx (set by inbox.ProcessOnce)
        q := gen.New(pg.FromContext(ctx, pool))
        // ... update quantity, enqueue outbox event ...
        return ReserveItemResult{ItemID: cmd.ItemID}, nil
    }
}

func DecorateReserveItemHandler(h cqrs.HandlerFunc[ReserveItem, ReserveItemResult], auditStore audit.Store) cqrs.HandlerFunc[ReserveItem, ReserveItemResult] {
    return cqrs.Decorate(h,
        cqrs.Logging[ReserveItem, ReserveItemResult]("ReserveItem"),
        cqrs.Tracing[ReserveItem, ReserveItemResult]("ReserveItem"),
        cqrs.Metrics[ReserveItem, ReserveItemResult]("ReserveItem"),
        cqrs.Validation[ReserveItem, ReserveItemResult](),
        audit.Audit[ReserveItem, ReserveItemResult](auditStore, "inventory:reserve", func(cmd ReserveItem) string { return cmd.ItemID }),
        // NOTE: Transaction behavior is omitted — inbox.ProcessOnce already opens a tx.
    )
}
```

For **query handlers** add `cqrs.Caching` instead of `audit.Audit`:

```go
cqrs.Caching[GetItem, GetItemResult](cache, ttl, keyFn),
```

---

## 6. Write the Kafka transport consumer

Wrap the handler in `inbox.ProcessOnce` to get effectively-once semantics:

```go
// examples/inventory/internal/transport/consumer.go

func NewCommandHandler(pool *pg.Pool, handler func(context.Context, app.ReserveItem) (app.ReserveItemResult, error)) kafka.HandlerFunc {
    return func(ctx context.Context, r kafka.Record) error {
        var cmd inventoryv1.ReserveItemCommand
        if err := proto.Unmarshal(r.Value, &cmd); err != nil {
            return fmt.Errorf("inventory consumer: unmarshal: %w", err)
        }
        msgID := r.Headers["message-id"]
        if msgID == "" {
            msgID = cmd.GetItemId()
        }
        _, err := inbox.ProcessOnce(ctx, pool, "inventory", msgID, func(ctx context.Context) error {
            _, err := handler(ctx, app.ReserveItem{
                ItemID:   cmd.GetItemId(),
                Quantity: int(cmd.GetQuantity()),
            })
            return err
        })
        return err
    }
}
```

---

## 7. Wire manual DI in `NewApp` / `Start` / `Stop`

Model after `examples/orders/orders.go`:

```go
// examples/inventory/inventory.go

type App struct {
    closer  *run.Closer
    consumer *kafka.Consumer
    relay   *outbox.Relay
    // ...
}

func NewApp(ctx context.Context) (*App, error) {
    cfg := config.Must[Config]()
    pool := pg.NewPool(ctx, cfg.PG)
    // run migrations
    // build outbox repo, relay, audit store
    // build handler, decorate it
    // build kafka consumer
    closer := run.NewCloser()
    closer.Add(pool.Close)
    closer.Add(consumer.Close)
    // ...
    return &App{closer: closer, consumer: consumer, relay: relay}, nil
}

func (a *App) Start() {
    go a.consumer.Run(context.Background())
    go a.relay.Run(context.Background())
}

func (a *App) Stop(ctx context.Context) error { return nil }
func (a *App) Closer() *run.Closer            { return a.closer }
```

In `cmd/inventory/main.go`:

```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    a, err := inventory.NewApp(ctx)
    if err != nil { log.Fatal(err) }
    a.Start()
    if err := run.Run(ctx, run.Options{ShutdownTimeout: 15 * time.Second}, a.Closer()); err != nil {
        log.Fatal(err)
    }
}
```

---

## 8. Add to `docker-compose.yml`

```yaml
  inventory:
    build:
      context: .
      dockerfile: Dockerfile
      args:
        SERVICE: inventory
    restart: unless-stopped
    environment:
      PG_DSN: postgres://app:app@postgres:5432/inventory?sslmode=disable
      KAFKA_BROKERS: redpanda:9092
      KAFKA_CLIENT_ID: inventory
      OTEL_SERVICE_NAME: inventory
      OTEL_EXPORTER_OTLP_ENDPOINT: otel-collector:4317
      OTEL_ENABLED: "true"
      LOG_LEVEL: info
      LOG_FORMAT: json
      ORDERS_EVENTS_TOPIC: orders.events
    depends_on:
      postgres:
        condition: service_healthy
      redpanda:
        condition: service_healthy
```

Add `inventory` to the Postgres init SQL (`deploy/postgres/init.sql`):

```sql
CREATE DATABASE inventory;
GRANT ALL PRIVILEGES ON DATABASE inventory TO app;
```

---

## 9. Add to CI matrix

In `.github/workflows/ci.yml`, add `inventory` to the `build-images` strategy matrix:

```yaml
    strategy:
      matrix:
        service: [gateway, orders, payments, notifications, inventory]
```

---

## 10. Write an integration test

Use testcontainers-go to spin up real Postgres and Redpanda. Model after `examples/orders/orders_test.go`:

```go
func TestReserveItem_Integration(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    ctx := context.Background()

    pgContainer, err := tcpostgres.Run(ctx, "postgres:16-alpine", ...)
    // run migrations, seed data
    // produce a ReserveItemCommand to Redpanda
    // start the app
    // assert the item quantity is updated in the DB
}
```

Run with:

```bash
task test    # go test -race ./...  (Docker required)
```

---

## Reference: orders service

The complete reference implementation lives in `examples/orders/`. Key files:

| File | What it shows |
|---|---|
| `orders.go` | `NewApp` manual DI wiring |
| `internal/app/create_order.go` | Command handler + `DecorateCreateOrderHandler` |
| `internal/transport/consumer.go` | Kafka consumer wrapping `inbox.ProcessOnce` |
| `internal/migrations/` | goose embed pattern |
| `orders_test.go` | Full integration test with testcontainers |
