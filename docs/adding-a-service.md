# Adding a New Service

This guide walks through adding a new service (the running example is `shipping`) on the `platform/servicekit` harness. The `examples/payments` service is the canonical reference — it is also the template the scaffolder copies. Read its source alongside this guide.

> Every ` ```go ` code block in this document is extracted and **compiled** by `scripts/doc-test.sh` (`just doc-test`, blocking in CI), so the snippets cannot drift from the real platform API. Blocks marked ` ```go nocompile ` are parse-checked only (they reference files the guide has not created yet).

---

## 0. Fast path: `just new-service`

```bash
just new-service shipping
```

This copies `examples/payments` → `examples/shipping`, renames files, packages, topics, consumer groups, and tables (`payments`→`shipping`, `Payments`→`Shipping`), and marks every shared-proto reference with a `TODO(new-service)` comment. The generated service **builds as-is** (it still consumes the demo `orders.events` topic) so you iterate from green. The script prints a manual wiring checklist — the same one reproduced in §9.

The rest of this guide explains what each generated piece is, in the order you would write them by hand.

---

## 1. Define the protobuf contract

Create your event and command types under `proto/<domain>/v1/`:

```
proto/
└── shipping/
    └── v1/
        └── shipping.proto
```

Then generate (buf + sqlc + oapi + mocks):

```bash
just gen        # or just `buf generate` for protos alone
```

Generated Go types appear in `gen/proto/shipping/v1/` (committed). `buf breaking` runs blocking in CI — an intentional contract break means a new `v2` proto package, not a weakened gate.

Until your own protos exist, the scaffold consumes the demo `orders/v1` events; this guide does the same so its code compiles.

---

## 2. Service directory structure

```
examples/shipping/
├── cmd/
│   └── shipping/
│       └── main.go          # servicekit.Main one-liner
├── internal/
│   ├── migrations/
│   │   ├── migrations.go    # go:embed sql
│   │   └── sql/             # goose SQL files (00001_init.sql, …)
│   ├── store/
│   │   ├── sqlc.yaml
│   │   ├── queries/         # .sql query files
│   │   └── gen/             # sqlc output (committed)
│   ├── domain/
│   │   └── shipment/        # THE business layer (conventions.md §9):
│   │       ├── codes.go     #   SHIPPING_* apperr codes, registered in init()
│   │       ├── repository.go#   Repository interface (consumer-side)
│   │       ├── pg.go        #   Postgres impl over sqlc (ambient tx via pg.FromContext)
│   │       └── service.go   #   Service owning every business rule
│   ├── app/
│   │   └── arrange_shipment.go  # cqrs command — THIN adapter over the Service + Decorate
│   └── transport/
│       └── consumer.go      # consume.Typed event handlers — decode + dispatch only
├── migrations_export.go     # exposes Migrations fs.FS for cmd/migrate
├── shipping.go              # Config + NewApp / Start / Stop / Closer
└── shipping_test.go         # integration test (testcontainers)
```

Every service gets the same `internal/domain/<aggregate>` layer, even when it is
a few lines (`examples/notifications`) — the examples are templates, and a copied
template with a ready seam beats one where the first business rule lands in a
Kafka handler. Rules, rationale and the ambient-transaction invariant:
[`docs/conventions.md`](conventions.md) §9.

---

## 3. Write migrations

Goose SQL files live in `internal/migrations/sql/` — note the `sql/` subdirectory; it is the `migrationsDir` argument passed to `servicekit.New`. Every consumer service needs its domain tables plus the three platform tables (`outbox` **with the `topic` column**, `inbox`, `audit_log`):

```sql
-- +goose Up
create table shipments (
    id         uuid        primary key,
    order_id   text        not null,
    status     text        not null,
    created_at timestamptz not null default now()
);

create table outbox (
    id             uuid primary key,
    topic          text        not null,
    aggregate_type text        not null,
    aggregate_id   text        not null,
    event_type     text        not null,
    payload        bytea       not null,
    headers        jsonb       not null default '{}'::jsonb,
    created_at     timestamptz not null default now(),
    published_at   timestamptz
);

create index outbox_unpublished_idx on outbox (created_at) where published_at is null;
create index outbox_published_at_idx on outbox (published_at) where published_at is not null;

create table inbox (
    consumer     text        not null,
    message_id   text        not null,
    processed_at timestamptz not null default now(),
    primary key (consumer, message_id)
);

create index inbox_processed_at_idx on inbox (processed_at);

create table audit_log (
    id         bigserial   primary key,
    actor      text        not null,
    action     text        not null,
    subject    text        not null,
    metadata   jsonb       not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key (id)
);

-- +goose Down
drop table audit_log;
drop table inbox;
drop table outbox;
drop table shipments;
```

(Compare `examples/payments/internal/migrations/sql/` for the exact reference, including the cleanup indexes.) Embed the directory:

```go
// Package migrations embeds the SQL migration files for the shipping service.
package migrations

import "embed"

// FS contains all goose migration files for the shipping service.
//
//go:embed sql
var FS embed.FS
```

`servicekit.New(ctx, cfg, migrations.FS, "sql")` applies them at startup (advisory-locked, idempotent) while `MIGRATE_ON_START=true` (the default). In production set `MIGRATE_ON_START=false` and run `cmd/migrate` as a pre-deploy job — see `docs/operations.md` § Database migrations.

Migration SQL is linted by squawk (`just lint-sql`, blocking `sql-lint` CI job).

---

## 4. Write sqlc queries and generate

Copy `examples/payments/internal/store/sqlc.yaml` (it points `schema:` at `../migrations/sql`), write `.sql` queries in `store/queries/`, then:

```bash
just gen     # runs sqlc for every sqlc.yaml in the repo
```

Commit the generated `store/gen/` output.

---

## 5. Config

Embed `servicekit.Config` and add your service-specific fields with `caarlos0/env` tags. `config.Load[T]()` parses the environment; if your config implements `Validate() error`, it is called after parsing and a non-nil error fails startup (use it for cross-field invariants).

The base config already carries everything the harness needs: `PG_DSN`/`PG_MIGRATE_URL`, `KAFKA_BROKERS`, `ADMIN_HTTP_ADDR`, `MIGRATE_ON_START`, `DRAIN_GRACE`, topic provisioning (`TOPIC_PARTITIONS`/`TOPIC_RF`/`TOPIC_RETENTION`/`ENSURE_TOPICS`), `SERDE_SR_URL`, inbox/audit retention, `PYROSCOPE_ADDR`. See `platform/servicekit/config.go` — every field is documented there.

---

## 6. The service file: domain layer, handler, transport, wiring

The complete pattern in one compiling file. In the real layout the domain layer lives in `internal/domain/shipment/`, the handler in `internal/app/`, the consumer in `internal/transport/`, and the embed in `internal/migrations/` (see §2) — they are inlined here so the example is self-contained. The real reference is `examples/payments` (same shape, split into those packages).

```go
// Package shipping demonstrates the full servicekit wiring pattern with the
// uniform domain layering: consume an event with consume.Typed
// (inbox-deduped), delegate to a domain Service that writes a row and
// enqueues a follow-up event in the same transaction (outbox), publish via
// the relay.
package shipping

import (
	"context"
	"embed"
	"fmt"
	"time"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/config"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/messaging/consume"
	"go-boilerplate/platform/messaging/kafka"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/run"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/servicekit"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	// TODO(new-service): replace the demo orders/v1 events with your own
	// proto/shipping/v1 messages once `just gen` has produced them.
	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// In the real layout this embed lives in internal/migrations (see §3).
//
//go:embed sql
var migrationsFS embed.FS

// Versioned event types: "<domain>.<Message>.v1" — carried in the
// "event-type" record header, dispatched on by consume.Typed.
const (
	orderCreatedEventType      = "orders.OrderCreated.v1"
	shipmentArrangedEventType  = "shipping.ShipmentArranged.v1"
)

// Config aggregates all configuration for the shipping service.
// Topic envs are named after the TOPIC, never the service (conventions §8):
// the env for orders.events is ORDERS_EVENTS_TOPIC in every service that
// touches it — not SHIPPING_EVENTS_TOPIC.
type Config struct {
	servicekit.Config
	EventsTopic string `env:"ORDERS_EVENTS_TOPIC" envDefault:"orders.events"`
}

// ─── Domain layer — in the real layout: internal/domain/shipment ────────────

// CodeInvalidOrderID is a SHIPPING_* error code owned by this service,
// registered with the platform registry at init (duplicate registration
// panics at startup). Permanent: no retry can fix a malformed id — consumers
// short-circuit it to the DLT. After adding codes, wire the service into
// cmd/errgen and run `just errgen` (checklist, §10).
const CodeInvalidOrderID = "SHIPPING_INVALID_ORDER_ID"

func init() {
	apperr.Register(CodeInvalidOrderID, 400, true, "invalid order id {order_id}", "order_id")
}

// Shipment is the domain view of a shipment row.
type Shipment struct {
	ID      uuid.UUID
	OrderID string
	Status  string
}

// Repository is the shipment persistence port, defined CONSUMER-SIDE
// (conventions.md §9): the Service declares what it needs; the Postgres
// adapter below satisfies it.
type Repository interface {
	Insert(ctx context.Context, s Shipment) error
}

// EventPublisher is the outbox port the Service enqueues domain events
// through; *outbox.Repository implements it directly.
type EventPublisher interface {
	Enqueue(ctx context.Context, msg outbox.Message) error
}

// PgRepository is the Postgres Repository. Every method resolves its query
// surface via pg.FromContext, so the SAME code joins the ambient transaction
// (inbox.ProcessOnce, cqrs.Transaction, or pg.RunInTx) when one is active —
// that is what makes "shipment row + outbox event commit together" hold.
type PgRepository struct{ pool *pg.Pool }

// NewPgRepository builds the Postgres repository over pool.
func NewPgRepository(pool *pg.Pool) *PgRepository { return &PgRepository{pool: pool} }

// Insert writes the shipment row. Real services use their sqlc-generated
// store here: gen.New(pg.FromContext(ctx, r.pool)).InsertShipment(ctx, …).
func (r *PgRepository) Insert(ctx context.Context, s Shipment) error {
	db := pg.FromContext(ctx, r.pool)
	if _, err := db.Exec(ctx,
		`insert into shipments (id, order_id, status) values ($1, $2, $3)`,
		s.ID, s.OrderID, s.Status,
	); err != nil {
		return fmt.Errorf("shipping: insert shipment: %w", err)
	}
	return nil
}

// Service owns the shipping business rules. Entry points (cqrs handler,
// Kafka transport) are thin adapters over its methods — logic shared by two
// entry points lives here, never in one handler calling another ("cmd never
// calls cmd"). Inject platform/clock.Clock here IF a rule reads "now" (see
// payments' Service); arranging a shipment does not.
type Service struct {
	repo   Repository
	events EventPublisher
}

// NewService builds the shipping domain service.
func NewService(repo Repository, events EventPublisher) *Service {
	return &Service{repo: repo, events: events}
}

// Arrange writes the shipment row and enqueues the follow-up event. Run it
// under a transaction (the inbox.ProcessOnce ambient tx in production) so
// row and event commit atomically — both writes resolve their DBTX from ctx.
func (s *Service) Arrange(ctx context.Context, orderID string) (uuid.UUID, error) {
	if _, err := uuid.Parse(orderID); err != nil {
		return uuid.Nil, apperr.Wrap(err, CodeInvalidOrderID).WithParam("order_id", orderID)
	}

	shipmentID := uuid.New()
	if err := s.repo.Insert(ctx, Shipment{ID: shipmentID, OrderID: orderID, Status: "arranged"}); err != nil {
		return uuid.Nil, err
	}

	// TODO(new-service): marshal your own shipping/v1 event here.
	payload, err := proto.Marshal(&ordersv1.OrderCreated{OrderId: orderID})
	if err != nil {
		return uuid.Nil, fmt.Errorf("shipping: marshal event: %w", err)
	}

	// Topic is the explicit destination; correlation-id/causation-id headers
	// are stamped automatically from ctx (consume.Typed put the chain
	// lineage there).
	if err := s.events.Enqueue(ctx, outbox.Message{
		ID:            uuid.New(),
		Topic:         "shipping.events",
		AggregateType: "shipment",
		AggregateID:   orderID,
		EventType:     shipmentArrangedEventType,
		Payload:       payload,
	}); err != nil {
		return uuid.Nil, fmt.Errorf("shipping: enqueue event: %w", err)
	}
	return shipmentID, nil
}

// ─── Application layer — in the real layout: internal/app ───────────────────

// ArrangeShipment is the command handled when an OrderCreated event arrives.
// Struct-tag validation is enforced by the pipeline's Validation behavior; a
// tag failure becomes a permanent VALIDATION_FAILED apperr (straight to the
// DLT under retry/DLT consumers).
type ArrangeShipment struct {
	OrderID string `validate:"required"`
}

// ArrangeShipmentResult is the command result.
type ArrangeShipmentResult struct{ ShipmentID string }

// ArrangeShipmentHandler is a THIN adapter over the domain service: it maps
// the command to Service.Arrange and nothing else. It runs inside the inbox
// transaction opened by consume.Typed, so the row + outbox event + inbox
// dedup marker commit atomically.
func ArrangeShipmentHandler(svc *Service) cqrs.HandlerFunc[ArrangeShipment, ArrangeShipmentResult] {
	return func(ctx context.Context, cmd ArrangeShipment) (ArrangeShipmentResult, error) {
		id, err := svc.Arrange(ctx, cmd.OrderID)
		if err != nil {
			return ArrangeShipmentResult{}, err
		}
		return ArrangeShipmentResult{ShipmentID: id.String()}, nil
	}
}

// DecorateArrangeShipmentHandler applies the standard behavior pipeline.
// StandardPipeline assembles Tracing → Logging → Metrics → Validation in the
// canonical order; WithTransaction is OMITTED because inbox.ProcessOnce
// already owns the transaction (see cqrs.Pipeline.WithTransaction godoc).
func DecorateArrangeShipmentHandler(
	h cqrs.HandlerFunc[ArrangeShipment, ArrangeShipmentResult],
	auditStore audit.Store,
) cqrs.HandlerFunc[ArrangeShipment, ArrangeShipmentResult] {
	return cqrs.StandardPipeline[ArrangeShipment, ArrangeShipmentResult]("ArrangeShipment").
		Use(audit.Audit[ArrangeShipment, ArrangeShipmentResult](auditStore, "shipment:arrange",
			func(cmd ArrangeShipment) string { return cmd.OrderID })).
		Decorate(h)
}

// ─── Transport — in the real layout: internal/transport ─────────────────────

// NewEventHandler is the Kafka transport: consume.Typed decodes the record
// (Schema Registry wire format when a serde is injected, raw protobuf
// otherwise), dispatches on the event-type header, deduplicates via the
// inbox, installs principal + correlation/causation ids into ctx, and runs
// the handler inside the inbox transaction. Unknown event types are skipped,
// never errored (forward compatibility). The transport stays decode +
// dispatch ONLY — business branching belongs in the Service.
func NewEventHandler(
	sp *pg.ShardedPool,
	handler cqrs.HandlerFunc[ArrangeShipment, ArrangeShipmentResult],
	opts ...consume.Option,
) kafka.HandlerFunc {
	// consume.New takes the sharded pool so each record is routed to its shard
	// by the Kafka record key (Tier-3, ADR-0019); at M=1 (PG_SHARDS unset) it is
	// the single pool — byte-identical to the unsharded path.
	return consume.New(sp, "shipping", opts...).Handler(
		consume.Typed(orderCreatedEventType, func(ctx context.Context, evt *ordersv1.OrderCreated) error {
			_, err := handler(ctx, ArrangeShipment{OrderID: evt.GetOrderId()})
			return err
		}),
	)
}

// ─── Wiring — in the real layout: shipping.go at the service root ───────────

// App holds all wired components for the shipping service.
type App struct {
	svc *servicekit.Service
}

// NewApp wires the service: servicekit harness (logger, telemetry, pg pool +
// migrations, kafka client/producer, health, admin server), topics, schema
// registration, outbox relay, consumer, audit cleanup.
func NewApp(ctx context.Context) (*App, error) {
	cfg, err := config.Load[Config]()
	if err != nil {
		return nil, err
	}

	svc, err := servicekit.New(ctx, cfg.Config, migrationsFS, "sql")
	if err != nil {
		return nil, err
	}

	// Provision topics this service touches (no-op when ENSURE_TOPICS=false;
	// AddConsumer ensures its own source + DLT topics too).
	if err := svc.EnsureTopics(ctx, cfg.EventsTopic, "shipping.events"); err != nil {
		return nil, err
	}

	// Schema Registry registration — no-op when SERDE_SR_URL is unset,
	// fail-fast when it is set. Register every consumed AND produced type.
	if err := svc.RegisterSchema(ctx, cfg.EventsTopic, orderCreatedEventType, &ordersv1.OrderCreated{}); err != nil {
		return nil, err
	}
	// TODO(new-service): register your own produced event type for
	// "shipping.events" here.

	// Outbox relay + cleaner. Single-active (advisory-lock leader) by
	// default: per-aggregate event order is preserved across replicas.
	outboxRepo := outbox.NewRepository(svc.Pool())
	if err := svc.AddOutboxRelay(svc.DefaultOutboxPublisher(), outbox.RelayConfig{
		PollInterval: 200 * time.Millisecond,
	}); err != nil {
		return nil, err
	}

	// Domain service over its ports (Postgres repository + outbox), then the
	// thin handler adapter + behavior pipeline.
	auditStore := audit.NewPgStore(svc.Pool())
	domain := NewService(NewPgRepository(svc.Pool()), outboxRepo)
	handler := DecorateArrangeShipmentHandler(ArrangeShipmentHandler(domain), auditStore)
	var consumeOpts []consume.Option
	if sd := svc.Serde(); sd != nil {
		consumeOpts = append(consumeOpts, consume.WithSerde(sd))
	}
	evtHandler := NewEventHandler(svc.Shards(), handler, consumeOpts...)

	// Consumer with in-process retry + poison-DLT (3 attempts → <topic>.DLT).
	// For tiered retry topics instead, see AddConsumerWithRetry (§8).
	if err := svc.AddConsumer(ctx, "shipping", []string{cfg.EventsTopic}, evtHandler); err != nil {
		return nil, err
	}

	// audit_log retention cleanup (defaults: 90 days, every 6 h).
	svc.AddAuditCleanup(auditStore, cfg.AuditCleanupInterval, cfg.AuditRetention)

	return &App{svc: svc}, nil
}

// Start launches the registered goroutines (consumers, relay, cleaners).
// Non-blocking. Admin-server bind failure is fatal by default.
func (a *App) Start() error { return a.svc.Start() }

// Stop tears down in readiness-first order (readyz→503, DRAIN_GRACE, then
// consumers, then clients). servicekit.Main calls this via the Closer — do
// NOT call Stop after run.Run has already closed the Closer.
func (a *App) Stop(ctx context.Context) error { return a.svc.Stop(ctx) }

// Closer exposes the teardown stack for run.Run / servicekit.Main.
func (a *App) Closer() *run.Closer { return a.svc.Closer() }
```

Other `Add*` hooks available before `Start`:

| Hook | Use |
|---|---|
| `AddConsumer(ctx, group, topics, handler)` | standard consumer: in-process retry ×3 → `<topic>.DLT` |
| `AddConsumerWithRetry(ctx, group, topics, handler, policy)` | tiered retry topics (§7) |
| `AddOutboxRelay(publisher, cfg)` | outbox relay + retention cleaner |
| `AddWorker(name, fn)` | any background goroutine (e.g. the orders unpaid-watcher) |
| `AddHTTPServer(name, srv)` | public HTTP server under the drain-gate lifecycle (the gateway uses this) |
| `AddAuditCleanup(store, interval, retention)` | audit_log retention |

HTTP-only services skip Kafka/Postgres entirely with `servicekit.New(ctx, cfg, nil, "", servicekit.WithoutKafka(), servicekit.WithoutPG())` — see `cmd/skeleton/main.go` for the minimal runnable example.

---

## 7. main.go

`servicekit.Main` owns the process: automaxprocs, build, start, signal wait, Closer teardown, exit codes.

```go nocompile
// Command shipping is the shipping service entry point.
package main

import (
	"context"

	"go-boilerplate/examples/shipping"
	"go-boilerplate/platform/servicekit"
)

func main() {
	servicekit.Main(func(ctx context.Context) (servicekit.App, error) {
		return shipping.NewApp(ctx)
	})
}
```

---

## 8. Retry opt-in (tiered retry topics)

`AddConsumer` retries in-process and dead-letters poison records — failures block the partition for the duration of the attempts. Consumers with strict latency/throughput needs should use tiered retry topics instead:

```go nocompile
// import "go-boilerplate/platform/messaging/retry"
policy := retry.DefaultPolicy() // FastAttempts: 1; tiers: 5s, 30s, 5m → DLT
err := svc.AddConsumerWithRetry(ctx, "shipping", []string{cfg.EventsTopic}, evtHandler, policy)
```

Flow: `FastAttempts` in-process attempts → escalate to `<topic>.retry.0`, `.retry.1`, … (index-named; the delay travels in the `retry-due-at` header, so tiers can be retuned without renaming topics) → after the last tier, `<topic>.DLT`.

**ORDERING WARNING:** tiered retry breaks per-key ordering — a failed record jumps the queue while later records for the same key flow on. Either make the handler reorder-safe or set `policy.KeyParkingWindow` (best-effort, in-memory key parking; see the `platform/messaging/retry` package documentation for the full trade-off).

---

## 9. Topic provisioning and serde

`EnsureTopics` (and every `AddConsumer*` call) creates missing topics from the service config:

| Env | Default | Meaning |
|---|---|---|
| `TOPIC_PARTITIONS` | `6` | partitions for created topics (bounds consumer-group parallelism) |
| `TOPIC_RF` | `1` | replication factor — local single-broker only; production ≥ 3 |
| `TOPIC_RETENTION` | `168h` | `retention.ms` on created topics; keep `INBOX_RETENTION ≥ TOPIC_RETENTION` (startup WARN otherwise) |
| `ENSURE_TOPICS` | `true` | set `false` in production — manage topics as IaC |
| `SERDE_SR_URL` | _(unset)_ | opt-in Confluent Schema Registry wire format; set on both producer and consumer sides of a topic, or neither |

---

## 10. Integration checklist (shared files the scaffold does NOT touch)

- [ ] **Protos:** define `proto/shipping/v1`, run `just gen`, replace the demo `orders/v1` references (`grep 'TODO(new-service)' examples/shipping`).
- [ ] **CI matrix:** add `shipping` to `jobs.build-images.strategy.matrix.service` in `.github/workflows/ci.yml`.
- [ ] **Compose:** add a `shipping` block under the `apps` profile in `docker-compose.yml` (copy the payments block: new `PG_DSN` database, `KAFKA_BROKERS`, topic env vars, `GOMEMLIMIT`, resource limits).
- [ ] **Database:** add `CREATE DATABASE shipping; GRANT ALL PRIVILEGES ON DATABASE shipping TO app;` to `deploy/postgres/init.sql`.
- [ ] **Env:** document new variables in `.env.example`.
- [ ] **justfile:** add `shipping` to the hardcoded `build-images` recipe.
- [ ] **cmd/migrate:** add a `migrations_export.go` (copy from payments) and register `shipping.Migrations` in the `services` map in `cmd/migrate/main.go`.
- [ ] **Error codes (cmd/errgen):** if the service registers `SHIPPING_*` apperr codes, blank-import `go-boilerplate/examples/shipping` in `cmd/errgen/main.go`, run `just errgen`, and commit the regenerated `docs/errors.md` (CI regenerates and diffs it, blocking). Registry rules: [`docs/conventions.md`](conventions.md) §10.
- [ ] **i18n catalogs:** only when the new codes surface on localized HTTP responses — embed `catalog/en.toml` (+ translations) next to the codes package and merge into the bundle via `i18n.Bundle.Load` (pattern: `examples/gateway/internal/apperrs`). Consumer-only codes need no catalog ([`docs/conventions.md`](conventions.md) §12).

---

## 11. Tests

Write both lanes — see [`docs/testing.md`](testing.md) for the full guide:

- **Fast lane (`-short`, no Docker):** unit-test the handler with `fakes.Publisher`/`fakes.Cache`, and drive the full decode→dispatch→handler transport pipeline with `fakes.Broker` + `consume.WithoutInbox()` (see `examples/payments/internal/transport/consumer_test.go`).
- **Full lane:** integration test with `pgtest.NewDSN(t)` + `kafkatest.NewRedpanda(t)`, `t.Setenv` the config, `NewApp` → `Start` → produce → poll-assert (see `examples/payments/payments_test.go`).

```bash
just test-unit          # fast lane
just test-integration   # full lane (Docker)
```

---

## Reference: payments service

The complete template lives in `examples/payments/`. Key files:

| File | What it shows |
|---|---|
| `payments.go` | `Config` + `NewApp` servicekit wiring (domain service over its ports) |
| `cmd/payments/main.go` | `servicekit.Main` entry point |
| `internal/domain/payment/` | the domain layer: consumer-side `Repository` + pg implementation (ambient tx), `Service.Process` (decision rule, injected `platform/clock`, outbox enqueue) |
| `internal/app/process_payment.go` | thin command-handler adapter + `Decorate*` pipeline |
| `internal/transport/consumer.go` | `consume.Typed` event handler (decode + dispatch only) |
| `internal/migrations/` | goose `sql/` embed pattern |
| `migrations_export.go` | migration export for `cmd/migrate` |
| `payments_test.go` | integration test with testcontainers |

The orders service (`examples/orders/`) additionally shows `AddConsumerWithRetry` (tiered retry), a second consumer, `AddWorker` (unpaid-order deadline watcher), and a fuller domain layer (`internal/domain/order`: table-driven state machine, `ORDERS_*` codes). The gateway (`examples/gateway/`) shows `AddHTTPServer`, the read-model projection, its `GATEWAY_*` codes + i18n catalogs (`internal/apperrs`), and edge validation.
