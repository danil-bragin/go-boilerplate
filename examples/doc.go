// Package examples contains a self-contained event-driven CQRS choreography
// demo built entirely on the platform packages.
//
// # Demo flow
//
//	client → POST /orders (gateway, REST)
//	  gateway → publish CreateOrderCommand → topic "orders.commands"  (returns 202 + order_id)
//	orders svc → consume orders.commands (inbox dedup)
//	  → write orders row (status="created") + outbox OrderCreated → topic "orders.events"
//	payments svc → consume orders.events (OrderCreated, inbox dedup)
//	  → write payments row (status="processed") + outbox PaymentProcessed → topic "payments.events"
//	notifications svc → consume payments.events (PaymentProcessed, inbox dedup) → notify (log)
//	gateway projection → consume orders.events + payments.events → upsert orders_read
//	client → GET /orders/{id} → status: created → paid
//
// # Services
//
//   - examples/gateway   — HTTP edge: POST /orders (command publish) + GET /orders/{id} (read model)
//   - examples/orders    — CreateOrderCommand consumer with transactional outbox
//   - examples/payments  — OrderCreated consumer with transactional outbox
//   - examples/notifications — PaymentProcessed consumer (terminal, no outbox)
//
// # Running locally
//
// Prerequisites: Docker (Redpanda + Postgres).
//
//	# Run all integration tests (starts containers automatically):
//	go test ./examples/...
//
//	# Run only the end-to-end test:
//	go test ./examples/e2e/...
//
//	# Skip long integration tests:
//	go test -short ./examples/...
//
// # Architecture notes
//
//   - DB-per-service: each service owns its own database schema.
//   - Inbox dedup: every consumer wraps processing in inbox.ProcessOnce so
//     redelivered messages are ignored.
//   - Transactional outbox: orders and payments write events in the same
//     transaction as their domain rows; the outboxkafka relay drains the outbox
//     asynchronously.
//   - The platform/ directory must never import examples/ — run
//     go list -deps go-boilerplate/platform/... to verify.
package examples
