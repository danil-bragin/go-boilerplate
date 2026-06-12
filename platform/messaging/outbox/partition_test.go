package outbox_test

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is a settable clock for exercising partition rotation
// deterministically without sleeping.
type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }

// hist is a base instant far from real "now" so the historical partitions
// these tests create never overlap rows other tests in the shared DB insert
// with a DEFAULT now() (those land in the DEFAULT partition).
var hist = time.Date(2031, 3, 1, 12, 30, 0, 0, time.UTC)

const testInterval = time.Hour

// dropGeneratedPartitions removes every outbox_p% partition so the shared DB
// is clean before and after a partition test.
func dropGeneratedPartitions(t *testing.T, pool *pg.Pool) {
	t.Helper()
	ctx := context.Background()
	rows, err := pool.Reader().Query(ctx, `
		select c.relname from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		join pg_class p on p.oid = i.inhparent
		where p.relname = 'outbox' and c.relname like 'outbox\_p%'`)
	require.NoError(t, err)
	var names []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	for _, n := range names {
		_, _ = pool.Writer().Exec(ctx, "alter table outbox detach partition "+n)
		_, _ = pool.Writer().Exec(ctx, "drop table if exists "+n)
	}
}

func insertAt(t *testing.T, pool *pg.Pool, id uuid.UUID, createdAt time.Time, published bool) {
	t.Helper()
	ctx := context.Background()
	if published {
		_, err := pool.Writer().Exec(ctx,
			`insert into outbox (id, topic, aggregate_type, aggregate_id, event_type, payload, headers, created_at, published_at)
			 values ($1,'orders.events','order','x','Test','{}','{}',$2,$2)`, id, createdAt)
		require.NoError(t, err)
		return
	}
	_, err := pool.Writer().Exec(ctx,
		`insert into outbox (id, topic, aggregate_type, aggregate_id, event_type, payload, headers, created_at)
		 values ($1,'orders.events','order','x','Test','{}','{}',$2)`, id, createdAt)
	require.NoError(t, err)
}

func tableOf(t *testing.T, pool *pg.Pool, id uuid.UUID) string {
	t.Helper()
	var rel string
	require.NoError(t, pool.Reader().QueryRow(context.Background(),
		`select tableoid::regclass::text from outbox where id=$1`, id).Scan(&rel))
	return rel
}

func newPartitionManager(pool *pg.Pool, now time.Time) *outbox.PartitionManager {
	return outbox.NewPartitionManager(pool,
		outbox.PartitionConfig{Interval: testInterval, Retention: 3 * testInterval, Lookahead: 2},
		outbox.WithClock(&fakeClock{t: now}))
}

func TestPartition_EnsureCreatesWindowsAndRoutesRows(t *testing.T) {
	pool := newPoolWithSchema(t)
	dropGeneratedPartitions(t, pool)
	t.Cleanup(func() { dropGeneratedPartitions(t, pool) })
	ctx := context.Background()

	pm := newPartitionManager(pool, hist)
	require.NoError(t, pm.EnsurePartitions(ctx, hist))

	// Lookahead 2 → windows for 12:00, 13:00, 14:00.
	id := uuid.New()
	insertAt(t, pool, id, hist, false) // hist=12:30 → 12:00 window
	assert.Equal(t, "outbox_p20310301t120000", tableOf(t, pool, id),
		"row must route into its time-range partition, not DEFAULT")

	// A row two windows ahead lands in the pre-created 14:00 partition.
	idAhead := uuid.New()
	insertAt(t, pool, idAhead, hist.Add(2*testInterval+10*time.Minute), false)
	assert.Equal(t, "outbox_p20310301t140000", tableOf(t, pool, idAhead))
}

func TestPartition_EnsureIsIdempotent(t *testing.T) {
	pool := newPoolWithSchema(t)
	dropGeneratedPartitions(t, pool)
	t.Cleanup(func() { dropGeneratedPartitions(t, pool) })
	ctx := context.Background()

	pm := newPartitionManager(pool, hist)
	require.NoError(t, pm.EnsurePartitions(ctx, hist))
	require.NoError(t, pm.EnsurePartitions(ctx, hist), "re-running must not error (CREATE IF NOT EXISTS)")
}

func TestPartition_DropExpiredRemovesOldEmptyPartitions(t *testing.T) {
	pool := newPoolWithSchema(t)
	dropGeneratedPartitions(t, pool)
	t.Cleanup(func() { dropGeneratedPartitions(t, pool) })
	ctx := context.Background()

	// Create old partitions around `hist`.
	require.NoError(t, newPartitionManager(pool, hist).EnsurePartitions(ctx, hist))
	// A published (drainable) row in the oldest window.
	insertAt(t, pool, uuid.New(), hist, true)

	// Advance well past retention (3 intervals). Now = hist + 10 intervals.
	later := hist.Add(10 * testInterval)
	pmLater := newPartitionManager(pool, later)
	require.NoError(t, pmLater.DropExpired(ctx, later))

	var remaining int
	require.NoError(t, pool.Reader().QueryRow(ctx, `
		select count(*) from pg_inherits i join pg_class c on c.oid=i.inhrelid
		join pg_class p on p.oid=i.inhparent
		where p.relname='outbox' and c.relname like 'outbox\_p20310301%'`).Scan(&remaining))
	assert.Zero(t, remaining, "all hist-era partitions are past retention and empty → dropped")
}

// The headline safety invariant: a partition past retention that still holds an
// unpublished row must NOT be dropped (a lagging relay must never lose events).
func TestPartition_NeverDropsPartitionWithUnpublishedRows(t *testing.T) {
	pool := newPoolWithSchema(t)
	dropGeneratedPartitions(t, pool)
	t.Cleanup(func() { dropGeneratedPartitions(t, pool) })
	ctx := context.Background()

	require.NoError(t, newPartitionManager(pool, hist).EnsurePartitions(ctx, hist))
	stranded := uuid.New()
	insertAt(t, pool, stranded, hist, false) // UNPUBLISHED, in the oldest window

	var skipErr error
	later := hist.Add(10 * testInterval)
	pm := outbox.NewPartitionManager(pool,
		outbox.PartitionConfig{Interval: testInterval, Retention: 3 * testInterval, Lookahead: 2},
		outbox.WithClock(&fakeClock{t: later}),
		outbox.WithPartitionOnError(func(e error) { skipErr = e }))

	require.NoError(t, pm.DropExpired(ctx, later), "skipping is not an error")

	// The partition (and the stranded row) must still be there.
	assert.Equal(t, "outbox_p20310301t120000", tableOf(t, pool, stranded),
		"partition with an unpublished row must survive retention")
	require.Error(t, skipErr)
	assert.Contains(t, skipErr.Error(), "unpublished")
}

// The relay's unpublished scan must span partitions transparently.
func TestPartition_RelayScanSpansPartitionBoundary(t *testing.T) {
	pool := newPoolWithSchema(t)
	dropGeneratedPartitions(t, pool)
	t.Cleanup(func() { dropGeneratedPartitions(t, pool) })
	ctx := context.Background()

	require.NoError(t, newPartitionManager(pool, hist).EnsurePartitions(ctx, hist))
	a, b := uuid.New(), uuid.New()
	insertAt(t, pool, a, hist, false)                   // 12:00 window
	insertAt(t, pool, b, hist.Add(testInterval), false) // 13:00 window

	var n int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where published_at is null and id in ($1,$2)`, a, b).Scan(&n))
	assert.Equal(t, 2, n, "FOR UPDATE SKIP LOCKED scan sees unpublished rows across partitions")
}
