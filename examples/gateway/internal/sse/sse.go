// Package sse streams order-status changes to clients over Server-Sent
// Events (GET /v1/orders/{id}/events).
//
// # Transport
//
// The projection (the only writer of orders_read) calls Streamer.Notify
// after every committed status write; Notify re-reads the row's CURRENT
// status and PUBLISHes it on the Redis channel "orders:status:<id>".
// Re-reading (instead of publishing the event's implied status) makes the
// payload authoritative: an event that lost the terminal-precedence race
// (0 rows updated) publishes the actual winning status, never a stale one.
//
// Each connected client subscribes to its order's channel. With no Redis
// configured the stream degrades gracefully to polling the projection store
// every poll interval — same events, higher latency.
//
// # Event format
//
// Every event carries a monotonically increasing id derived from the status
// ordinal (pending=0 → created=1 → terminal=2), so Last-Event-ID resume is
// trivial: on reconnect the current status is sent only when its ordinal is
// GREATER than the id the client already saw. data is {"status":"<status>"}.
// A ":hb" comment line is written every heartbeat interval so intermediaries
// do not reap idle connections.
//
// # Ownership
//
// The stream applies the SAME read-path rules as GET /v1/orders/{id}:
// missing principal → 401; unknown order OR non-owner without the admin
// role → the same 404 (no existence oracle).
package sse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/web/httpx"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/rueidis"

	storegen "go-boilerplate/examples/gateway/internal/store/gen"
)

const (
	// DefaultHeartbeat is how often a comment line keeps the stream alive.
	DefaultHeartbeat = 15 * time.Second
	// DefaultPollInterval is the store-polling cadence used when no Redis
	// client is available (graceful degradation).
	DefaultPollInterval = 2 * time.Second
)

// channelFor returns the Redis pub/sub channel for one order's status feed.
func channelFor(orderID string) string { return "orders:status:" + orderID }

// statusOrdinal maps a read-model status to its monotone position in the
// order lifecycle: pending(0) → created(1) → exactly one terminal(2).
// Terminal statuses share an ordinal — they are mutually exclusive (first
// terminal event wins in the projection), so per-order ids stay monotone.
func statusOrdinal(status string) int {
	switch status {
	case "pending":
		return 0
	case "created":
		return 1
	case "paid", "payment_failed", "payment_timeout":
		return 2
	default:
		return -1 // unknown: never sent (forward compatibility)
	}
}

// Streamer publishes order-status notifications (projection side) and serves
// the SSE endpoint (edge side). Both sides share the Redis client and the
// projection store; client may be nil — Notify becomes a no-op and Stream
// falls back to polling.
type Streamer struct {
	client       rueidis.Client // nil = degrade to polling
	pool         *pg.Pool
	logger       *slog.Logger
	authDisabled bool
	heartbeat    time.Duration
	pollInterval time.Duration

	// shutdown ends every active stream promptly on server shutdown —
	// without it, open SSE connections would hold http.Server.Shutdown
	// hostage for the whole teardown budget. Wire Streamer.Shutdown into
	// httpserver.Server.OnShutdown.
	shutdown     chan struct{}
	shutdownOnce sync.Once
}

// Option configures a Streamer.
type Option func(*Streamer)

// WithHeartbeat overrides the keep-alive comment interval.
func WithHeartbeat(d time.Duration) Option {
	return func(s *Streamer) {
		if d > 0 {
			s.heartbeat = d
		}
	}
}

// WithPollInterval overrides the no-Redis polling cadence.
func WithPollInterval(d time.Duration) Option {
	return func(s *Streamer) {
		if d > 0 {
			s.pollInterval = d
		}
	}
}

// New builds a Streamer. client may be nil (no Redis): Notify is then a
// no-op and Stream serves status changes by polling the projection store.
func New(client rueidis.Client, pool *pg.Pool, logger *slog.Logger, authDisabled bool, opts ...Option) *Streamer {
	s := &Streamer{
		client:       client,
		pool:         pool,
		logger:       logger,
		authDisabled: authDisabled,
		heartbeat:    DefaultHeartbeat,
		pollInterval: DefaultPollInterval,
		shutdown:     make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Shutdown ends every active stream. Register it with the public HTTP
// server's OnShutdown hook so streams close as soon as graceful shutdown
// begins (clients reconnect to another instance and resume via
// Last-Event-ID). Idempotent.
func (s *Streamer) Shutdown() {
	s.shutdownOnce.Do(func() { close(s.shutdown) })
}

// Notify publishes the order's CURRENT read-model status to the order's
// Redis channel. Call it AFTER the projection transaction committed (e.g.
// from consume.OnCommitted). Best-effort: failures are logged and never
// propagate — SSE is a notification layer, not a system of record (polling
// readers and reconnects re-read the store anyway).
func (s *Streamer) Notify(ctx context.Context, orderID string) {
	if s.client == nil {
		return
	}
	status, _, err := s.currentStatus(ctx, orderID)
	if err != nil {
		s.logger.Warn("sse: notify: read current status failed", "order_id", orderID, "error", err)
		return
	}
	cmd := s.client.B().Publish().Channel(channelFor(orderID)).Message(status).Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		s.logger.Warn("sse: notify: publish failed", "order_id", orderID, "error", err)
	}
}

// errNotFound marks an order that does not exist in the read model.
var errNotFound = errors.New("sse: order not found")

// currentStatus reads the order's status and owner from the projection store.
func (s *Streamer) currentStatus(ctx context.Context, orderID string) (status, customerID string, err error) {
	id, err := uuid.Parse(orderID)
	if err != nil {
		return "", "", errNotFound
	}
	row, err := storegen.New(pg.FromContextRead(ctx, s.pool)).GetOrderView(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", errNotFound
		}
		return "", "", fmt.Errorf("sse: get order view: %w", err)
	}
	return row.Status, row.CustomerID, nil
}

// Stream handles GET /v1/orders/{id}/events. See the package doc for the
// protocol. The handler runs until the client disconnects (or the server
// shuts down); mount it on a route group WITHOUT http.TimeoutHandler.
func (s *Streamer) Stream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orderID := chi.URLParam(r, "id")

	status, owner, err := s.currentStatus(ctx, orderID)
	if err != nil && !errors.Is(err, errNotFound) {
		writeProblem(w, http.StatusInternalServerError, "internal error")
		return
	}
	notFound := errors.Is(err, errNotFound)

	// Ownership — same semantics as GET /v1/orders/{id}: non-admin
	// principals only see their own orders, and a foreign order returns the
	// SAME 404 as a nonexistent one (no existence oracle).
	if !s.authDisabled {
		p, ok := auth.From(ctx)
		if !ok {
			writeProblem(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if !slices.Contains(p.Roles, attachments.AdminRole) && (notFound || owner != p.Subject) {
			notFound = true
		}
	}
	if notFound {
		writeProblem(w, http.StatusNotFound, "order "+orderID+" not found")
		return
	}

	// Subscribe BEFORE sending the initial snapshot so a transition landing
	// in between is delivered by the subscription, not lost.
	updates := make(chan string, 8)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.feedUpdates(streamCtx, orderID, updates)

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // intermediaries: do not buffer
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	// Long-lived stream: lift the server's WriteTimeout for this response.
	// ErrNotSupported only means the stream is bounded by HTTP_WRITE_TIMEOUT
	// and the client transparently reconnects (Last-Event-ID resume).
	_ = rc.SetWriteDeadline(time.Time{})

	// lastSent is the ordinal the CLIENT knows: the Last-Event-ID header on
	// reconnect, else -1 (nothing). The initial snapshot is sent only when
	// it is newer than what the client already saw.
	lastSent := -1
	if v, err := strconv.Atoi(r.Header.Get("Last-Event-ID")); err == nil {
		lastSent = v
	}
	send := func(status string) bool {
		ord := statusOrdinal(status)
		if ord <= lastSent {
			return true
		}
		lastSent = ord
		if _, err := fmt.Fprintf(w, "id: %d\ndata: {\"status\":%q}\n\n", ord, status); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !send(status) {
		return
	}

	heartbeat := time.NewTicker(s.heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done(): // client closed
			return
		case <-s.shutdown: // server draining: close so the client reconnects
			return
		case status := <-updates:
			if !send(status) {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": hb\n\n"); err != nil {
				return
			}
			if rc.Flush() != nil {
				return
			}
		}
	}
}

// feedUpdates delivers status strings into updates until ctx is cancelled:
// via Redis pub/sub when a client is configured, else by polling the store.
// A pub/sub subscription that dies mid-stream falls back to polling instead
// of going silent.
func (s *Streamer) feedUpdates(ctx context.Context, orderID string, updates chan<- string) {
	if s.client != nil {
		sub := s.client.B().Subscribe().Channel(channelFor(orderID)).Build()
		err := s.client.Receive(ctx, sub, func(msg rueidis.PubSubMessage) {
			select {
			case updates <- msg.Message:
			case <-ctx.Done():
			}
		})
		if ctx.Err() != nil {
			return
		}
		s.logger.Warn("sse: subscription lost, falling back to store polling",
			"order_id", orderID, "error", err)
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			status, _, err := s.currentStatus(ctx, orderID)
			if err != nil {
				continue // transient read failure or row gone: try next tick
			}
			select {
			case updates <- status:
			case <-ctx.Done():
				return
			}
		}
	}
}

// writeProblem emits the same RFC7807 problem+json shape as the generated
// API error paths.
func writeProblem(w http.ResponseWriter, status int, detail string) {
	httpx.WriteProblem(w, httpx.Problem{
		Status: status,
		Title:  http.StatusText(status),
		Detail: detail,
	})
}
