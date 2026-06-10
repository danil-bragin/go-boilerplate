# Messaging (Sub-project 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
> **Integration-heavy:** franz-go / buf / schema-registry APIs are version-sensitive. Where this plan gives code, treat it as the intended shape; VERIFY exact signatures with `go doc <pkg>` and adapt minimally to compile while preserving behavior. Report every adaptation. Tests use the **Redpanda** testcontainers module (Kafka API + built-in Schema Registry in one container) — Docker required.

**Goal:** Event transport layer: a franz-go Kafka producer + consumer-group wrapper, a protobuf+schema-registry serde, a Kafka `Publisher` that drains the SP2 transactional outbox to Kafka, and an idempotent-consumer (inbox) + dead-letter-topic story for at-least-once delivery.

**Architecture:** `platform/kafka` wraps franz-go (`kgo`) for produce/consume with cooperative-sticky groups, otel instrumentation (`kotel`), and graceful close via `run.Closer`. `platform/serde` provides protobuf (+ Confluent Schema Registry via `franz-go/pkg/sr`) encode/decode behind an interface. `proto/` holds event contracts compiled by `buf`. The Kafka `Publisher` (implements `outbox.Publisher` from SP2) keys records by aggregate id and writes `Message.Payload` (already-serialized bytes) + headers; `outbox.Relay` stays transport-agnostic. `platform/inbox` records processed message ids in the same DB tx as the side-effect (dedup → effectively-once over at-least-once). A consumer wrapper bounds retries and routes poison messages to `<topic>.DLT`.

**Tech Stack:** `github.com/twmb/franz-go` (`pkg/kgo`, `pkg/sr`, `plugin/kotel`) · `github.com/twmb/franz-go/pkg/kadm` (admin/topic create) · `buf` + `protoc-gen-go` · `google.golang.org/protobuf` · `testcontainers-go/modules/redpanda` · testify.

**Depends on (done):** `platform/config`, `platform/log`, `platform/run`, `platform/telemetry`, `platform/pg` (inbox uses DBTX/RunInTx), `platform/outbox` (Publisher interface, Message).

**Out of scope:** CQRS behaviors (SP4), cache/blob/auth (SP5), example services wiring (SP6), compose/CI (SP7).

---

## File Structure
```
go-boilerplate/
├── buf.yaml  buf.gen.yaml
├── proto/
│   └── events/v1/
│       ├── envelope.proto        # EventEnvelope (id, type, aggregate, occurred_at, payload bytes)
│       └── orders.proto          # example: OrderCreated
├── gen/proto/events/v1/          # buf output (committed): *.pb.go
├── platform/
│   ├── kafka/
│   │   ├── config.go             # Brokers, GroupID, etc. (caarlos0/env tags)
│   │   ├── client.go             # NewClient(*kgo.Client) factory, kotel, close
│   │   ├── producer.go           # Producer: Produce(ctx, Record) sync; health
│   │   ├── consumer.go           # Consumer group: Run(ctx, HandlerFunc) cooperative-sticky
│   │   ├── admin.go              # EnsureTopics via kadm
│   │   ├── record.go             # Record type (topic,key,value,headers)
│   │   ├── kafkatest/            # redpanda testcontainers helper
│   │   └── *_test.go
│   ├── serde/
│   │   ├── serde.go              # Serializer/Deserializer interfaces
│   │   ├── protobuf.go           # protobuf (+ optional schema-registry) impl
│   │   └── *_test.go
│   ├── inbox/
│   │   ├── migrations/00001_inbox.sql
│   │   ├── inbox.go              # ProcessOnce(ctx, msgID, fn) dedup within tx
│   │   └── inbox_test.go
│   └── outboxkafka/
│       ├── publisher.go          # KafkaPublisher implements outbox.Publisher
│       └── publisher_test.go     # integration: enqueue -> relay -> consume
└── (buf CLI + protoc-gen-go installed for codegen; generated code committed)
```

Boundary: `platform/kafka`,`serde`,`inbox` import only platform + franz-go/proto. `platform/outboxkafka` bridges outbox↔kafka (separate pkg so `platform/outbox` stays broker-free). Nothing imports `cmd/`/`examples/`.

---

## Task 1 — buf + proto scaffolding + generated Go
**Files:** `buf.yaml`, `buf.gen.yaml`, `proto/events/v1/envelope.proto`, `proto/events/v1/orders.proto`, generated `gen/proto/...` (committed).

- [ ] **Step 1:** Install tools if missing: `buf` (`go install github.com/bufbuild/buf/cmd/buf@latest`), `protoc-gen-go` (`go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`). Ensure `$(go env GOPATH)/bin` on PATH.
- [ ] **Step 2:** `proto/events/v1/envelope.proto` — package `events.v1`, `go_package = "go-boilerplate/gen/proto/events/v1;eventsv1"`. Message `EventEnvelope { string id=1; string type=2; string aggregate_type=3; string aggregate_id=4; google.protobuf.Timestamp occurred_at=5; bytes payload=6; map<string,string> headers=7; }`.
- [ ] **Step 3:** `proto/events/v1/orders.proto` — `OrderCreated { string order_id=1; string customer_id=2; int64 amount_cents=3; string currency=4; }`.
- [ ] **Step 4:** `buf.yaml` (v2: `version: v2`, modules dir `proto`, lint+breaking defaults) and `buf.gen.yaml` (v2: plugin `protoc-gen-go`, out `gen/proto`, `paths=source_relative`). Run `buf lint` then `buf generate`. Commit generated `*.pb.go`.
- [ ] **Step 5:** `go get google.golang.org/protobuf@latest`, `go mod tidy`, `go build ./gen/...`. Verify generated types compile. **Commit** `feat(proto): buf setup + event envelope and orders example contracts`.

> Verify buf v2 config schema with `buf --version` and the buf docs if `buf lint`/`generate` error on the config; adapt config to the installed buf major version. Generated files carry `// Code generated` headers (lint-skipped).

---

## Task 2 — platform/kafka producer + client + admin
**Files:** `config.go`, `client.go`, `producer.go`, `admin.go`, `record.go`, `kafkatest/kafkatest.go`, tests.

- [ ] **Step 1:** `kafkatest.NewRedpanda(t)` → starts `testcontainers-go/modules/redpanda`, returns broker addr (and schema-registry URL for Task 4). `go doc github.com/testcontainers/testcontainers-go/modules/redpanda` for the constructor (`redpanda.Run(ctx, "redpandadata/redpanda:latest", ...)`); `container.KafkaSeedBroker(ctx)` and `container.SchemaRegistryAddress(ctx)`.
- [ ] **Step 2:** `Config{ Brokers []string \`env:"KAFKA_BROKERS" envSeparator:","\`; ClientID string; GroupID string }` etc.
- [ ] **Step 3:** `Record{ Topic string; Key []byte; Value []byte; Headers map[string]string }`.
- [ ] **Step 4:** `client.go`: `NewClient(cfg, opts...) (*kgo.Client, error)` with `kgo.SeedBrokers`, idempotent producer (default), `kotel` tracing hook (`go doc github.com/twmb/franz-go/plugin/kotel`). `admin.go`: `EnsureTopics(ctx, client, names..., partitions, rf)` via `kadm.NewClient(cl).CreateTopics`.
- [ ] **Step 5:** `producer.go`: `Producer` wrapping `*kgo.Client`; `Produce(ctx, Record) error` synchronous (`cl.ProduceSync(...).FirstErr()`), maps Headers→`kgo.RecordHeader`. `Close(ctx)` flushes+closes. Health: `Ping(ctx)`.
- [ ] **Step 6 (test):** `TestProducer_ProduceAndConsumeRoundTrip` — Redpanda container, EnsureTopics, produce a record, then a raw `kgo` consumer reads it back, assert key/value/headers. **Commit** `feat(kafka): franz-go client, producer, admin topic ensure`.

> franz-go API anchors to verify: `kgo.NewClient`, `kgo.SeedBrokers`, `kgo.ProduceSync`, `kgo.Record{Topic,Key,Value,Headers}`, `kadm.NewClient(...).CreateTopics(ctx, partitions, rf, nil, names...)`. Adapt to the resolved version; report.

---

## Task 3 — platform/kafka consumer group
**Files:** `consumer.go`, test.
- [ ] **Step 1:** `HandlerFunc func(ctx, Record) error`. `Consumer` wrapping a `*kgo.Client` configured with `kgo.ConsumerGroup(groupID)`, `kgo.ConsumeTopics(...)`, `kgo.Balancers(kgo.CooperativeStickyBalancer())`, manual commit after successful handle (`kgo.DisableAutoCommit` + `cl.CommitRecords`).
- [ ] **Step 2:** `Run(ctx, HandlerFunc) error` — poll loop (`cl.PollFetches(ctx)`), for each record call handler; on success commit; on handler error, do NOT commit (record reprocessed) — DLQ/retry handled in Task 7's wrapper. Respect ctx cancel; return ctx.Err() on shutdown.
- [ ] **Step 3 (test):** `TestConsumer_GroupConsumesCommitted` — produce N, run consumer in goroutine, collect via handler into a thread-safe slice, assert all N received; cancel ctx to stop; restart a new consumer in same group and assert no redelivery (offsets committed). **Commit** `feat(kafka): cooperative-sticky consumer group with manual commit`.

---

## Task 4 — platform/serde protobuf + schema registry
**Files:** `serde.go`, `protobuf.go`, test.
- [ ] **Step 1:** Interfaces: `Serializer interface { Encode(msg proto.Message) ([]byte, error) }`, `Deserializer interface { Decode(data []byte, into proto.Message) error }`.
- [ ] **Step 2:** `protobuf.go`: a plain protobuf impl (`proto.Marshal`/`proto.Unmarshal`) AND a schema-registry-aware impl using `franz-go/pkg/sr` (`sr.NewClient`, `sr.Serde`) that wire-prefixes the SR schema id. Provide `NewProtobuf()` (no SR) and `NewSchemaRegistry(srURL, ...)` constructors. Keep SR optional so non-SR setups work.
- [ ] **Step 3 (test):** `TestProtobufSerde_RoundTrip` — encode an `eventsv1.OrderCreated`, decode back, assert equality (`proto.Equal`). `TestSchemaRegistrySerde_RoundTrip` — uses Redpanda's Schema Registry URL from kafkatest; register+encode+decode, assert round-trip. (Skip the SR test with `t.Skip` only if the SR client API can't be reconciled — but prefer making it work; report.) **Commit** `feat(serde): protobuf and schema-registry serializers`.

> `go doc github.com/twmb/franz-go/pkg/sr` for `Serde`/`Client` API; adapt.

---

## Task 5 — KafkaPublisher draining the outbox
**Files:** `platform/outboxkafka/publisher.go`, integration test.
- [ ] **Step 1:** `KafkaPublisher{ producer *kafka.Producer; topicFor func(outbox.Message) string }` implementing `outbox.Publisher`: `Publish(ctx, msg)` → `producer.Produce(ctx, kafka.Record{ Topic: topicFor(msg), Key: []byte(msg.AggregateID), Value: msg.Payload, Headers: merge(msg.Headers-json→map, {"event-type":msg.EventType,"message-id":msg.ID}) })`. Default `topicFor` = `msg.AggregateType` (document; gateway/services override). Headers come from `msg.Headers` (JSON object) decoded to map[string]string + standard headers.
- [ ] **Step 2 (integration test):** `TestKafkaPublisher_DrainsOutboxToKafka` — spin Redpanda + Postgres (both containers), migrate outbox schema, EnsureTopics, build `outbox.NewRelay(pool, NewKafkaPublisher(producer,...), cfg)`, `Enqueue` 3 messages in tx, `relay.ProcessBatch`, then consume from Kafka and assert the 3 payloads/keys arrived with correct headers, and the outbox rows are marked published. **Commit** `feat(outboxkafka): Kafka publisher draining the transactional outbox`.

---

## Task 6 — platform/inbox idempotent consumer
**Files:** `migrations/00001_inbox.sql`, `inbox.go`, test.
- [ ] **Step 1:** migration `inbox(message_id text primary key, consumer text not null, processed_at timestamptz not null default now())` (PK on (message_id) or (consumer,message_id) — use composite `primary key (consumer, message_id)` so different consumers can each process the same message once).
- [ ] **Step 2:** `inbox.go`: `ProcessOnce(ctx, pool, consumer, messageID string, fn func(ctx) error) (bool, error)` — runs inside `pg.RunInTx`: try `insert into inbox(...) values(...) on conflict do nothing`; if 0 rows affected → already processed → return (false, nil) WITHOUT running fn; else run fn within the same tx (so the side effect + the inbox marker commit atomically), return (true, fn-err). On fn error the tx rolls back (both the insert and side effect) → message reprocessed later. Use `gen`-free direct SQL via `pg.FromContext` (or a tiny sqlc query — prefer direct SQL to avoid another sqlc package; use `tag, _ := pg.FromContext(ctx,pool).Exec(...)` and check `tag.RowsAffected()`).
- [ ] **Step 3 (test):** `TestInbox_ProcessOnce_DedupesByConsumer` — Postgres + migrate; call ProcessOnce twice with same (consumer,id): first runs fn (processed=true), second returns processed=false and does NOT run fn. `TestInbox_FnErrorRollsBackMarker` — fn returns error → marker not persisted → a later ProcessOnce runs fn again. `TestInbox_DifferentConsumersEachProcessOnce`. **Commit** `feat(inbox): idempotent-consumer dedup within transaction`.

---

## Task 7 — DLQ / bounded-retry consumer wrapper
**Files:** `platform/kafka/dlq.go`, test.
- [ ] **Step 1:** `WithRetry(handler HandlerFunc, opts RetryOpts) HandlerFunc` where `RetryOpts{ MaxAttempts int; Producer *Producer; DLTTopic func(topic string) string }` (default `<topic>.DLT`). Wrap: on handler error, increment an attempt counter carried in a record header (`x-attempts`); if attempts < MaxAttempts, return the error (so the consumer does not commit → reprocessed) OR (simpler, stateless) immediately route to DLT after MaxAttempts. Decision: track attempts via header on re-produce to a retry topic is complex; for SP3 implement the simpler **bounded in-handler retry then DLT**: the wrapper retries the handler up to MaxAttempts in-process (with small backoff, ctx-aware); if still failing, produce the record (value+headers + `x-error`,`x-attempts`) to the DLT and return nil (commit, message parked in DLT). Document that in-process retry blocks the partition during retries (acceptable for SP3; stateless retry-topics deferred).
- [ ] **Step 2 (test):** `TestWithRetry_RoutesToDLTAfterMaxAttempts` — Redpanda; a handler that always errors; wrap with MaxAttempts=2 + DLT producer; feed one record; assert handler called 2×, then a record appears on `<topic>.DLT` with `x-error`/`x-attempts` headers, and the wrapped handler returns nil (commit). `TestWithRetry_SucceedsWithinAttempts` — handler fails once then succeeds → no DLT record, returns nil. **Commit** `feat(kafka): bounded-retry consumer wrapper with dead-letter routing`.

---

## Task 8 — final verify + review
- [ ] Whole-repo: `go build ./...`, `go test -race ./...`, `go vet`, `gofmt -l`, `golangci-lint run`, coverage on new packages. Generated `gen/proto` excluded from lint by header.
- [ ] Document at-least-once + DLT conventions in package docs. Confirm `outbox.Relay` still transport-agnostic (no kafka import in `platform/outbox`).

---

## Self-review checklist
- Outbox stays broker-free (kafka bridge in `platform/outboxkafka`). ✅
- Effectively-once = outbox (producer) + inbox (consumer) over at-least-once Kafka. ✅
- Cooperative-sticky + manual commit (no loss on rebalance). ✅
- DLT for poison messages. ✅
- Deferred: stateless retry-topics, exactly-once `GroupTransactSession`, CDC (documented for later).
