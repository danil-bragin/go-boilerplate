package outbox

import (
	"context"
	"fmt"
	"time"

	"go-boilerplate/platform/messaging/outbox/gen"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RelayConfig configures the polling relay.
//
// RetentionAge is the maximum age of a published outbox row before it is
// eligible for deletion. CleanupInterval controls how often Cleaner.RunCleanup
// fires. Both are used when constructing a Cleaner alongside the Relay.
//
// Defaults: RetentionAge 24h, CleanupInterval 1h.
//
// SingleActive controls whether the relay should run in advisory-lock leader
// mode (see WithSingleActive). The flag itself does not change NewRelay
// behaviour — the option carries the lock pool — but lets harnesses such as
// servicekit decide whether to pass WithSingleActive. Default true: per-
// aggregate publish order is preserved out of the box; set
// OUTBOX_SINGLE_ACTIVE=false to let every instance publish concurrently
// (higher throughput, NO cross-instance ordering guarantee).
type RelayConfig struct {
	BatchSize       int32         `env:"OUTBOX_BATCH_SIZE"        envDefault:"100"`
	PollInterval    time.Duration `env:"OUTBOX_POLL_INTERVAL"     envDefault:"1s"`
	RetentionAge    time.Duration `env:"OUTBOX_RETENTION_AGE"    envDefault:"24h"`
	CleanupInterval time.Duration `env:"OUTBOX_CLEANUP_INTERVAL" envDefault:"1h"`
	SingleActive    bool          `env:"OUTBOX_SINGLE_ACTIVE"    envDefault:"true"`
	// Shards > 1 splits publishing across N independently leader-elected relays,
	// each owning the aggregate_ids that hash into its shard (ADR-0017). This
	// scales publish throughput ~N× while preserving per-aggregate order (a key
	// always maps to one shard). 1 (default) = a single relay, behaviour
	// unchanged. servicekit.AddOutboxRelay reads this to spawn the shard fleet.
	Shards int `env:"OUTBOX_RELAY_SHARDS" envDefault:"1"`
}

// Relay polls unpublished outbox rows and publishes them via a Publisher,
// using a three-phase design that keeps the DB transaction open only for the
// minimum time needed, dramatically reducing lock-hold time under load.
//
// Three-phase ProcessBatch:
//
//	Phase A (short tx): SELECT ... FOR UPDATE SKIP LOCKED fetches up to
//	BatchSize unpublished rows into memory and immediately commits, releasing
//	all row locks. Lock-hold time is O(one DB round-trip), not O(N·broker RTT).
//
//	Phase B (no tx): publish all fetched messages to the transport. If the
//	Publisher also implements BatchPublisher, PublishBatch is used for a
//	single broker round-trip (O(1) latency for the batch); otherwise Publish
//	is called per-record. Publishing happens entirely outside any transaction.
//
//	Phase C (short tx): UPDATE outbox SET published_at=now() WHERE id=ANY($1)
//	marks all successfully-published rows in one statement.
//
// Delivery semantics: AT-LEAST-ONCE. Duplicates are expected and safe:
//   - Crash between B and C: rows re-fetched and re-published on next poll.
//   - Two concurrent relays: after phase A commits, a second relay can claim
//     the same rows before phase C completes → both publish → inbox deduplicates
//     by Message.ID.
//
// ORDERING CAVEAT — multiple concurrent relays: SKIP LOCKED makes concurrent
// relays SAFE (no loss, duplicates deduplicated downstream), but it does NOT
// preserve per-aggregate publish order. Two relays can claim interleaved rows
// of the same aggregate and publish them out of order (and Kafka ordering is
// per produced batch anyway). If consumers rely on per-aggregate event order
// — the common case for event-carried state transfer — run the relay in
// LEADER mode via WithSingleActive: a Postgres advisory lock guarantees only
// one relay instance publishes at a time, with automatic failover when the
// leader's lock session dies. Standby instances poll for the lock every tick.
//
// OnError (set via SetOnError) is called for each ProcessBatch error during
// Run. Run never stops on a batch error; it backs off and retries.
type Relay struct {
	pool    *pg.Pool
	pub     Publisher
	cfg     RelayConfig
	onError func(error)

	// Single-active (leader) mode state — see WithSingleActive.
	leaderPool *pgxpool.Pool
	leaderConn *pgxpool.Conn

	// Sharding (see WithShard). shardCount <= 1 means unsharded: the relay
	// fetches every unpublished row and uses the bare leader lock key. When
	// shardCount > 1 the relay fetches only its shard's rows and contends a
	// shard-scoped lock key, so N shard-leaders run concurrently.
	shardCount int
	shardIndex int
	// lockSQL/unlockSQL are the advisory-lock acquire/release statements for
	// this relay's leader key (bare for unsharded, shard-scoped otherwise).
	lockSQL   string
	unlockSQL string

	metrics relayMetrics
}

// RelayOption configures optional Relay behaviour.
type RelayOption func(*Relay)

// WithSingleActive enables leader election via a Postgres advisory lock so
// that only ONE relay instance publishes at a time (preserving per-aggregate
// publish order across horizontally-scaled deployments).
//
// Mechanics:
//   - A dedicated connection is acquired from pool and holds a session-level
//     pg_try_advisory_lock(hashtext('outbox_relay:'||current_schema())).
//   - The instance that wins the lock is the leader and processes batches;
//     all others stay hot-standby and re-try the lock every PollInterval.
//   - The leader health-checks its lock connection every tick AND between
//     every two batches of a drain (one SELECT 1 per batch). If the
//     connection (and therefore the lock session) is lost, leadership is
//     dropped and every instance — including the former leader — competes
//     for the lock again. Failover takes a few PollIntervals.
//
// FENCING CAVEAT: an advisory lock is leader ELECTION, not fencing. A leader
// that loses its lock session only notices at the next health-check, so there
// is an inherent window of up to one health-check interval (one batch during
// a drain, one PollInterval otherwise) in which the old leader and a freshly
// elected one may both publish. That window produces duplicates (deduplicated
// downstream by the inbox) and, in the worst case, a one-batch per-aggregate
// ordering inversion. True fencing would require a token checked by the
// downstream system.
//
// The lock key is schema-scoped, so multiple services sharing a database
// (different schemas) elect independent leaders.
func WithSingleActive(pool *pgxpool.Pool) RelayOption {
	return func(r *Relay) { r.leaderPool = pool }
}

// WithShard restricts this relay to the aggregate_ids that hash into shard
// index of count (ADR-0017), and scopes its leader lock to that shard so N
// shard-relays can each be leader at once. count <= 1 is a no-op (unsharded).
// Use together with WithSingleActive so each shard has exactly one active
// publisher — that is what preserves per-aggregate order under sharding.
func WithShard(index, count int) RelayOption {
	return func(r *Relay) {
		if count > 1 {
			r.shardCount = count
			r.shardIndex = index
		}
	}
}

// NewRelay creates a Relay.
func NewRelay(pool *pg.Pool, pub Publisher, cfg RelayConfig, opts ...RelayOption) *Relay {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	r := &Relay{pool: pool, pub: pub, cfg: cfg, metrics: newRelayMetrics()}
	// Default to the bare (unsharded) leader-lock statements; WithShard may
	// switch them to shard-scoped keys below.
	r.lockSQL = advisoryLockSQL
	r.unlockSQL = advisoryUnlockSQL
	for _, o := range opts {
		o(r)
	}
	if r.shardCount > 1 {
		r.lockSQL = fmt.Sprintf(
			`select pg_try_advisory_lock(hashtext('outbox_relay:'||current_schema()||':shard:%d'))`, r.shardIndex,
		)
		r.unlockSQL = fmt.Sprintf(
			`select pg_advisory_unlock(hashtext('outbox_relay:'||current_schema()||':shard:%d'))`, r.shardIndex,
		)
	}
	return r
}

// SetOnError registers a callback that is called for each ProcessBatch error
// encountered during Run. It is safe to call before Run is started. Run never
// stops on a batch error — it backs off and retries regardless.
func (r *Relay) SetOnError(fn func(error)) {
	r.onError = fn
}

// fetchUnpublished claims up to BatchSize unpublished rows (FOR UPDATE SKIP
// LOCKED) within the caller's transaction. Unsharded relays fetch every row;
// sharded relays fetch only the rows whose aggregate_id hashes into their
// shard. Both queries return identical columns, mapped to Message.
func (r *Relay) fetchUnpublished(txCtx context.Context) ([]Message, error) {
	q := gen.New(pg.FromContext(txCtx, r.pool))
	if r.shardCount > 1 {
		rows, err := q.FetchUnpublishedShard(txCtx, gen.FetchUnpublishedShardParams{
			ShardCount: int64(r.shardCount),
			ShardIndex: int64(r.shardIndex),
			BatchSize:  r.cfg.BatchSize,
		})
		if err != nil {
			return nil, fmt.Errorf("outbox: fetch shard %d/%d: %w", r.shardIndex, r.shardCount, err)
		}
		msgs := make([]Message, len(rows))
		for i, row := range rows {
			msgs[i] = Message{
				ID:            row.ID,
				Topic:         row.Topic,
				AggregateType: row.AggregateType,
				AggregateID:   row.AggregateID,
				EventType:     row.EventType,
				Payload:       row.Payload,
				Headers:       []byte(row.Headers),
				CreatedAt:     row.CreatedAt.Time,
			}
		}
		return msgs, nil
	}
	rows, err := q.FetchUnpublished(txCtx, r.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("outbox: fetch: %w", err)
	}
	msgs := make([]Message, len(rows))
	for i, row := range rows {
		msgs[i] = Message{
			ID:            row.ID,
			Topic:         row.Topic,
			AggregateType: row.AggregateType,
			AggregateID:   row.AggregateID,
			EventType:     row.EventType,
			Payload:       row.Payload,
			Headers:       []byte(row.Headers),
			CreatedAt:     row.CreatedAt.Time,
		}
	}
	return msgs, nil
}

// ProcessBatch runs the three-phase A/B/C cycle once:
//
//	A) Short tx: fetch up to BatchSize unpublished rows (FOR UPDATE SKIP LOCKED)
//	   into memory and commit — locks released immediately.
//	B) No tx: publish all rows to the transport (batched if possible).
//	C) Short tx: mark all published rows in one batch UPDATE.
//
// Returns the number of rows successfully published. If publish (phase B)
// fails, phase C is skipped and all rows remain unpublished for retry.
func (r *Relay) ProcessBatch(ctx context.Context) (int, error) {
	// Phase A: short claim transaction — fetch and immediately commit.
	// SKIP LOCKED prevents concurrent relays from fetching the same rows
	// within the same phase-A window; once this tx commits the locks are
	// released. A second relay may then re-claim the same rows before phase C
	// marks them → duplicate publish → inbox deduplicates by Message.ID.
	var msgs []Message
	if err := pg.RunInTx(ctx, r.pool, func(txCtx context.Context) error {
		var err error
		msgs, err = r.fetchUnpublished(txCtx)
		return err
	}); err != nil {
		return 0, fmt.Errorf("outbox: claim phase: %w", err)
	}

	if len(msgs) == 0 {
		return 0, nil
	}

	// Phase B: publish outside any transaction.
	// Use BatchPublisher if the injected Publisher also implements it —
	// one broker round-trip (Flush) for the whole batch instead of N×ProduceSync.
	// Fall back to per-record Publish for plain Publisher implementations.
	if bp, ok := r.pub.(BatchPublisher); ok {
		if err := bp.PublishBatch(ctx, msgs); err != nil {
			r.metrics.addPublishError(ctx)
			return 0, fmt.Errorf("outbox: batch publish: %w", err)
		}
	} else {
		for _, msg := range msgs {
			if err := r.pub.Publish(ctx, msg); err != nil {
				r.metrics.addPublishError(ctx)
				return 0, fmt.Errorf("outbox: publish %s: %w", msg.ID, err)
			}
		}
	}

	// Phase C: mark all successfully-published rows in one batch statement.
	ids := make([]uuid.UUID, len(msgs))
	for i, msg := range msgs {
		ids[i] = msg.ID
	}
	q := gen.New(r.pool.Writer())
	if err := q.MarkPublishedBatch(ctx, ids); err != nil {
		return 0, fmt.Errorf("outbox: mark published batch: %w", err)
	}

	// Publish lag is observed ONLY for rows that completed the full A→B→C
	// cycle (same condition as the outbox.published counter): a row whose
	// phase C failed stays unpublished and will be observed once on the retry
	// that actually marks it. now − CreatedAt mixes the app clock with DB
	// insert time — see relayMetrics.recordPublishLag for the skew note.
	now := time.Now().UTC()
	for _, msg := range msgs {
		r.metrics.recordPublishLag(ctx, now.Sub(msg.CreatedAt))
	}

	r.metrics.addPublished(ctx, int64(len(msgs)))
	return len(msgs), nil
}

// pendingReportInterval is how often Run samples the unpublished-row count
// for the outbox.pending gauge. The count query hits the partial index on
// unpublished rows, so it stays cheap even on large outbox tables.
const pendingReportInterval = 15 * time.Second

// reportPending samples SELECT count(*) of unpublished rows and records the
// outbox.pending gauge. Errors are ignored — the gauge simply keeps its last
// value; the relay's own error path covers DB outage visibility.
func (r *Relay) reportPending(ctx context.Context) {
	var n int64
	if err := r.pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where published_at is null`).Scan(&n); err != nil {
		return
	}
	r.metrics.recordPending(ctx, n)
}

const (
	backoffBase = 100 * time.Millisecond
	backoffMax  = 30 * time.Second
)

// advisoryLockSQL acquires (non-blocking) the schema-scoped relay leader lock
// on the CURRENT SESSION. hashtext maps the textual key to the int lock space.
const advisoryLockSQL = `select pg_try_advisory_lock(hashtext('outbox_relay:'||current_schema()))`

// advisoryUnlockSQL releases the leader lock held by the current session.
const advisoryUnlockSQL = `select pg_advisory_unlock(hashtext('outbox_relay:'||current_schema()))`

// ensureLeader returns true when this relay currently holds the leader lock.
// When not yet leader it makes one non-blocking acquisition attempt; when
// already leader it health-checks the lock connection — a dead connection
// means the lock session is gone, so leadership is dropped (and re-contested
// on the next tick alongside every other instance).
func (r *Relay) ensureLeader(ctx context.Context) bool {
	if r.leaderConn != nil {
		// Health-check the dedicated lock connection. Any error ⇒ the session
		// (and the advisory lock with it) is gone server-side.
		if _, err := r.leaderConn.Exec(ctx, `select 1`); err != nil {
			r.dropLeadership(ctx, false)
			return false
		}
		return true
	}

	conn, err := r.leaderPool.Acquire(ctx)
	if err != nil {
		return false
	}
	var acquired bool
	if err := conn.QueryRow(ctx, r.lockSQL).Scan(&acquired); err != nil || !acquired {
		conn.Release()
		return false
	}
	r.leaderConn = conn
	return true
}

// dropLeadership releases the leader lock connection. When unlock is true the
// advisory lock is explicitly released so another instance can take over
// immediately (graceful shutdown). When the connection is unhealthy the
// underlying session is destroyed instead of being returned to the pool —
// returning it would keep the session (and the lock) alive inside the pool.
func (r *Relay) dropLeadership(ctx context.Context, unlock bool) {
	conn := r.leaderConn
	if conn == nil {
		return
	}
	r.leaderConn = nil
	if unlock {
		if _, err := conn.Exec(ctx, r.unlockSQL); err == nil {
			conn.Release()
			return
		}
	}
	// Unlock failed or connection unhealthy: kill the session so the server
	// releases the lock, instead of parking a lock-holding conn in the pool.
	_ = conn.Hijack().Close(ctx)
}

// drain repeatedly processes batches while full batches come back — there may
// be more backlog waiting. It stops on a partial batch (backlog empty), an
// error, context cancellation, or — in single-active mode — as soon as
// leadership is lost: re-verifying the advisory lock between batches (one
// cheap SELECT 1 per batch) bounds the dual-publish window after a lost lock
// to a single batch instead of an unbounded backlog drain.
func (r *Relay) drain(ctx context.Context) error {
	for {
		n, err := r.ProcessBatch(ctx)
		if err != nil || n < int(r.cfg.BatchSize) || ctx.Err() != nil {
			return err
		}
		if r.leaderPool != nil && !r.ensureLeader(ctx) {
			return nil
		}
	}
}

// Run polls until ctx is canceled. On every tick it DRAINS the backlog:
// ProcessBatch is repeated as long as it returns a full batch, so a burst of
// N rows is published in ceil(N/BatchSize) consecutive batches within one
// tick instead of being capped at BatchSize rows per PollInterval. A partial
// batch ends the drain loop until the next tick (no busy loop when idle).
//
// On ProcessBatch errors it calls OnError (if set), ends the drain loop, and
// applies a simple capped exponential backoff before the next attempt
// (resets on success). Intended to be launched in a goroutine; returns
// ctx.Err() on cancellation.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	// Graceful shutdown in single-active mode: explicitly release the leader
	// lock so a standby can take over immediately. The release runs on a
	// short detached context because ctx is already cancelled at that point.
	if r.leaderPool != nil {
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			r.dropLeadership(releaseCtx, true)
		}()
	}

	// Backlog gauge: sample the unpublished-row count now and every
	// pendingReportInterval until ctx is cancelled.
	pendingDone := make(chan struct{})
	go func() {
		defer close(pendingDone)
		r.reportPending(ctx)
		pt := time.NewTicker(pendingReportInterval)
		defer pt.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-pt.C:
				r.reportPending(ctx)
			}
		}
	}()
	defer func() { <-pendingDone }()

	var consecutiveFailures int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Single-active mode: only the advisory-lock leader processes.
			// Standbys (and a leader whose lock session died) skip the tick
			// and re-contest the lock on the next one.
			if r.leaderPool != nil && !r.ensureLeader(ctx) {
				continue
			}
			err := r.drain(ctx)
			if err != nil {
				if r.onError != nil {
					r.onError(err)
				}
				consecutiveFailures++
				backoff := backoffBase
				for i := 1; i < consecutiveFailures; i++ {
					backoff *= 2
					if backoff > backoffMax {
						backoff = backoffMax
						break
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
			} else {
				consecutiveFailures = 0
			}
		}
	}
}
