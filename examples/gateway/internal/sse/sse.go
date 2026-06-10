// Package sse streams order-status changes to clients over Server-Sent
// Events (GET /v1/orders/{id}/events).
//
// # Transport
//
// The projection (the only writer of orders_read) calls Streamer.Notify
// after every committed status write; Notify re-reads the row's CURRENT
// status and PUBLISHes {"order_id":"…","status":"…"} on the single Redis
// broadcast channel "orders:status". Re-reading (instead of publishing the
// event's implied status) makes the payload authoritative: an event that
// lost the terminal-precedence race (0 rows updated) publishes the actual
// winning status, never a stale one.
//
// Each Streamer replica holds exactly ONE Redis subscription to that
// broadcast channel (a dedicated connection, started lazily with the first
// stream) and fans messages out to an in-process registry of open streams.
// Streams therefore cost ZERO Redis connections each — the replica total is
// one dedicated subscriber connection plus the client's normal pooled
// connections for PUBLISH and reads. The old design (one dedicated
// connection per stream) hit rueidis' blocking-pool ceiling at ~1024
// streams per replica and redis maxclients globally.
//
// A single plain channel is used deliberately: PSUBSCRIBE (pattern
// subscribe) is restricted on some managed offerings (e.g. ElastiCache
// Serverless), and per-order channels would put the subscription count back
// on the scaling path. At extreme scale (broadcast volume saturating one
// subscriber connection), shard the broadcast across hash-named channels —
// "orders:status:{N}" with N = hash(order_id) % shards, each replica
// subscribing to all shards — documented here as the next step, not built.
//
// # Gaps
//
// If the subscriber connection drops, the Streamer resubscribes (bounded
// backoff) and, after every (re)subscribe ack, pushes a refresh signal to
// ALL registered streams: each re-reads its order's current status from the
// projection store, so anything published during the gap is recovered —
// the gap is bounded by the resubscribe backoff. As a second safety net
// while Redis stays unreachable, every stream also re-reads the store on a
// slow safety poll (30× the poll interval) so a stream can never silently
// stall. With no Redis configured at all, streams degrade gracefully to
// polling the projection store every poll interval — same events, higher
// latency.
//
// # Event format
//
// Every event carries a monotonically increasing id derived from the status
// ordinal (pending=0 → created=1 → terminal=2), so Last-Event-ID resume is
// trivial: on reconnect the current status is sent only when its ordinal is
// GREATER than the id the client already saw. data is {"status":"<status>"}.
// A ":hb" comment line is written every heartbeat interval so intermediaries
// do not reap idle connections. Per-stream delivery coalesces to the highest
// ordinal seen — a slow client may skip intermediate statuses but never
// observes a regression.
//
// # Ownership
//
// The stream applies the SAME read-path rules as GET /v1/orders/{id}:
// missing principal → 401; unknown order OR non-owner without the admin
// role → the same 404 (no existence oracle).
package sse

import (
	"context"
	"encoding/json"
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
	// client is available (graceful degradation). With Redis configured the
	// safety poll runs at safetyPollMultiplier× this interval instead.
	DefaultPollInterval = 2 * time.Second

	// broadcastChannel is the single Redis pub/sub channel carrying every
	// order's status updates (see the package doc for why one channel).
	broadcastChannel = "orders:status"

	// resubscribeBackoff bounds how fast the shared subscriber retries after
	// a connection drop — repeated gaps cannot hot-loop SUBSCRIBE, and the
	// refresh-on-resubscribe gap is at most this long plus the ack RTT.
	resubscribeBackoff = 500 * time.Millisecond

	// safetyPollMultiplier × pollInterval is the slow per-stream store
	// re-read cadence used while Redis IS configured: a last-resort net for
	// the window where the subscriber is down and stays down (refresh on
	// resubscribe covers transient gaps much faster).
	safetyPollMultiplier = 30

	// subscribeWait bounds how long a new stream waits for the shared
	// subscriber to be live before sending its snapshot anyway (the
	// resubscribe refresh + safety poll cover anything missed after that —
	// delayed, never lost).
	subscribeWait = 2 * time.Second
)

// statusUpdate is the broadcast payload published by Notify.
type statusUpdate struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

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

// slot is one stream's coalescing mailbox. Producers (the shared
// subscriber's dispatch and refresh broadcast) never block: deliver keeps
// only the highest-ordinal status (ordinals are monotone, so the latest
// always wins — keeping the newest ARRIVAL instead could let a stale
// pre-gap publish overwrite a terminal status), and refresh is a sticky
// flag telling the stream to re-read the store. notify (cap 1) wakes the
// stream's select loop.
type slot struct {
	mu      sync.Mutex
	status  string // highest-ordinal undelivered status ("" = none)
	refresh bool   // store re-read requested (possible missed broadcast)
	notify  chan struct{}
}

func newSlot() *slot { return &slot{notify: make(chan struct{}, 1)} }

// deliver coalesces status into the slot (highest ordinal wins) and wakes
// the stream. Never blocks.
func (sl *slot) deliver(status string) {
	sl.mu.Lock()
	if statusOrdinal(status) > statusOrdinal(sl.status) {
		sl.status = status
	}
	sl.mu.Unlock()
	sl.wake()
}

// markRefresh asks the stream to re-read the store. It does NOT clear any
// pending status — and deliver never clears refresh: a publish whose store
// read predates a write missed during the gap may arrive after the
// resubscribe, and dropping the refresh on its account would lose the
// missed (possibly terminal) status.
func (sl *slot) markRefresh() {
	sl.mu.Lock()
	sl.refresh = true
	sl.mu.Unlock()
	sl.wake()
}

func (sl *slot) wake() {
	select {
	case sl.notify <- struct{}{}:
	default: // already signalled — the pending wake covers this update too
	}
}

// take drains the slot.
func (sl *slot) take() (status string, refresh bool) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	status, refresh = sl.status, sl.refresh
	sl.status, sl.refresh = "", false
	return status, refresh
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

	// registry maps order id → the open streams watching it. The shared
	// subscriber fans broadcast messages out through it; resubscribes push a
	// refresh to every slot.
	regMu    sync.Mutex
	registry map[string]map[*slot]struct{}

	// Shared-subscriber lifecycle: started lazily by the first Redis-mode
	// stream (a Streamer built only for Notify — e.g. the standalone
	// projection — never pays for it), stopped by Shutdown via subCtx.
	subMu      sync.Mutex
	subStarted bool
	subCtx     context.Context
	subCancel  context.CancelFunc
	subWG      sync.WaitGroup

	// liveCh is closed while the broadcast subscription is acknowledged and
	// replaced with a fresh open channel on every drop — streams wait on it
	// (bounded) before sending their snapshot.
	liveMu sync.Mutex
	liveCh chan struct{}

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

// WithPollInterval overrides the no-Redis polling cadence (and thereby the
// Redis-mode safety poll, which runs at safetyPollMultiplier× this value).
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
	subCtx, subCancel := context.WithCancel(context.Background())
	s := &Streamer{
		client:       client,
		pool:         pool,
		logger:       logger,
		authDisabled: authDisabled,
		heartbeat:    DefaultHeartbeat,
		pollInterval: DefaultPollInterval,
		registry:     make(map[string]map[*slot]struct{}),
		subCtx:       subCtx,
		subCancel:    subCancel,
		liveCh:       make(chan struct{}),
		shutdown:     make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Shutdown ends every active stream and stops the shared subscriber.
// Register it with the public HTTP server's OnShutdown hook so streams close
// as soon as graceful shutdown begins (clients reconnect to another instance
// and resume via Last-Event-ID). Idempotent; returns after the subscriber
// goroutine has exited.
func (s *Streamer) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.shutdown)
		s.subMu.Lock()
		s.subCancel()
		s.subMu.Unlock()
		s.subWG.Wait()
	})
}

// Notify publishes the order's CURRENT read-model status as
// {"order_id":…,"status":…} on the broadcast channel. Call it AFTER the
// projection transaction committed (e.g. from consume.OnCommitted).
// Best-effort: failures are logged and never propagate — SSE is a
// notification layer, not a system of record (the refresh/safety-poll paths
// and reconnects re-read the store anyway).
func (s *Streamer) Notify(ctx context.Context, orderID string) {
	if s.client == nil {
		return
	}
	status, _, err := s.currentStatus(ctx, orderID)
	if err != nil {
		s.logger.Warn("sse: notify: read current status failed", "order_id", orderID, "error", err)
		return
	}
	payload, err := json.Marshal(statusUpdate{OrderID: orderID, Status: status})
	if err != nil {
		s.logger.Warn("sse: notify: marshal failed", "order_id", orderID, "error", err)
		return
	}
	cmd := s.client.B().Publish().Channel(broadcastChannel).Message(string(payload)).Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		s.logger.Warn("sse: notify: publish failed", "order_id", orderID, "error", err)
	}
}

// register adds a stream's slot to the order's fan-out set.
func (s *Streamer) register(orderID string, sl *slot) {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	set := s.registry[orderID]
	if set == nil {
		set = make(map[*slot]struct{})
		s.registry[orderID] = set
	}
	set[sl] = struct{}{}
}

// unregister removes a stream's slot, dropping the order's set when empty.
func (s *Streamer) unregister(orderID string, sl *slot) {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	set := s.registry[orderID]
	delete(set, sl)
	if len(set) == 0 {
		delete(s.registry, orderID)
	}
}

// dispatch fans one broadcast message out to the streams watching its order.
func (s *Streamer) dispatch(payload string) {
	var upd statusUpdate
	if err := json.Unmarshal([]byte(payload), &upd); err != nil || upd.OrderID == "" {
		return // malformed broadcast — ignore (forward compatibility)
	}
	s.regMu.Lock()
	defer s.regMu.Unlock()
	for sl := range s.registry[upd.OrderID] {
		sl.deliver(upd.Status) // never blocks (coalescing mailbox)
	}
}

// broadcastRefresh tells EVERY registered stream to re-read the store —
// the equivalent of a cache InvalidateAll after a subscription gap.
func (s *Streamer) broadcastRefresh() {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	for _, set := range s.registry {
		for sl := range set {
			sl.markRefresh()
		}
	}
}

// subscriberLive returns the channel that is closed while the broadcast
// subscription is acknowledged.
func (s *Streamer) subscriberLive() <-chan struct{} {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	return s.liveCh
}

func (s *Streamer) markLive() {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	select {
	case <-s.liveCh: // already closed
	default:
		close(s.liveCh)
	}
}

func (s *Streamer) markDown() {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	select {
	case <-s.liveCh:
		s.liveCh = make(chan struct{})
	default: // never went live — keep the existing open channel
	}
}

// ensureSubscriber starts the shared subscriber goroutine once. Safe against
// concurrent streams and against Shutdown: the check and the cancel both run
// under subMu, so the goroutine is either covered by Shutdown's Wait or
// never started.
func (s *Streamer) ensureSubscriber() {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	if s.subStarted || s.subCtx.Err() != nil {
		return
	}
	s.subStarted = true
	s.subWG.Add(1)
	go s.runSubscriber()
}

// runSubscriber is the replica's single subscribe/resubscribe loop —
// the same shape as the cache invalidation subscriber: any return from a
// live subscription is a gap, never a clean stop (those are the subCtx /
// ErrClosing cases), and every gap ends with a refresh broadcast once the
// new subscription is acknowledged.
func (s *Streamer) runSubscriber() {
	defer s.subWG.Done()
	for {
		err := s.subscribeOnce()
		if s.subCtx.Err() != nil || errors.Is(err, rueidis.ErrClosing) {
			return
		}
		s.logger.Warn("sse: broadcast subscription lost, resubscribing", "error", err)
		select {
		case <-s.subCtx.Done():
			return
		case <-time.After(resubscribeBackoff):
		}
	}
}

// subscribeOnce holds one dedicated connection with a live SUBSCRIBE until
// it drops. Do returns only after Redis acknowledged the SUBSCRIBE, so
// markLive (and the streams' snapshot reads gated on it) cannot precede a
// live subscription.
func (s *Streamer) subscribeOnce() error {
	cc, release := s.client.Dedicate()
	defer release()
	hooksDone := cc.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) { s.dispatch(msg.Message) },
	})
	if err := cc.Do(s.subCtx, cc.B().Subscribe().Channel(broadcastChannel).Build()).Error(); err != nil {
		return err
	}
	s.markLive()
	defer s.markDown()
	// Every (re)subscribe is a potential gap: broadcasts published while no
	// subscription was live are gone, so every registered stream re-reads
	// the store. (On the very first subscribe this is a no-op or a cheap
	// duplicate read — streams gate their snapshot on liveness anyway.)
	s.broadcastRefresh()
	select {
	case err := <-hooksDone: // connection lost
		return err
	case <-s.subCtx.Done():
		return s.subCtx.Err()
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
// No per-stream goroutine and no per-stream Redis connection: updates arrive
// through the stream's registry slot, fed by the replica's one subscriber.
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

	// Register in the fan-out registry and WAIT (bounded) for the shared
	// subscription to be live BEFORE reading the snapshot we send first: any
	// transition committed before the subscription was live is captured by
	// the re-read, and any transition after it arrives via the slot — no
	// window in which a terminal status (which has no subsequent event to
	// paper over a loss) can be missed.
	var sl *slot
	var slotNotify <-chan struct{} // nil in poll mode: never fires
	if s.client != nil {
		s.ensureSubscriber()
		sl = newSlot()
		slotNotify = sl.notify
		s.register(orderID, sl)
		defer s.unregister(orderID, sl)

		select {
		case <-s.subscriberLive():
		case <-time.After(subscribeWait):
			// Subscriber slow or Redis down: proceed — the resubscribe
			// refresh and the safety poll recover anything missed (delayed,
			// never lost).
		case <-ctx.Done():
			return
		}
	}

	// Authoritative snapshot, re-read AFTER registration + liveness (the
	// pre-auth read above only established ownership). Poll mode is "live"
	// immediately — the next poll tick re-reads anyway.
	if cur, _, err := s.currentStatus(ctx, orderID); err == nil {
		status = cur
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // intermediaries: do not buffer

	rc := http.NewResponseController(w)
	// Rolling write deadline instead of the server-wide WriteTimeout: each
	// write must complete within 2×heartbeat or the stream is torn down. A
	// client that stops reading (slowloris — this route is exempt from
	// http.TimeoutHandler by design) therefore cannot pin the goroutine past
	// one deadline window, and Shutdown is never held hostage by a blocked
	// write. ErrNotSupported only means the stream stays bounded by
	// HTTP_WRITE_TIMEOUT and the client transparently reconnects.
	armWriteDeadline := func() { _ = rc.SetWriteDeadline(time.Now().Add(2 * s.heartbeat)) }
	armWriteDeadline()
	w.WriteHeader(http.StatusOK)

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
		armWriteDeadline()
		if _, err := fmt.Fprintf(w, "id: %d\ndata: {\"status\":%q}\n\n", ord, status); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !send(status) {
		return
	}

	// Poll cadence: the no-Redis fallback polls at the configured interval;
	// with Redis the slot delivers updates and the poll degrades to the slow
	// safety net (see package doc).
	pollEvery := s.pollInterval
	if s.client != nil {
		pollEvery *= safetyPollMultiplier
	}
	poll := time.NewTicker(pollEvery)
	defer poll.Stop()
	heartbeat := time.NewTicker(s.heartbeat)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done(): // client closed
			return
		case <-s.shutdown: // server draining: close so the client reconnects
			return
		case <-slotNotify:
			st, refresh := sl.take()
			if refresh {
				// Possible missed broadcast: the store is authoritative and
				// at least as new as anything missed. On a read failure keep
				// whatever the slot held; the safety poll retries the store.
				if cur, _, err := s.currentStatus(ctx, orderID); err == nil && statusOrdinal(cur) > statusOrdinal(st) {
					st = cur
				}
			}
			if st != "" && !send(st) {
				return
			}
		case <-poll.C:
			st, _, err := s.currentStatus(ctx, orderID)
			if err != nil {
				continue // transient read failure or row gone: try next tick
			}
			if !send(st) {
				return
			}
		case <-heartbeat.C:
			armWriteDeadline()
			if _, err := fmt.Fprint(w, ": hb\n\n"); err != nil {
				return
			}
			if rc.Flush() != nil {
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
