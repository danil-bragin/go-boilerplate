package outbox_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// insertPublishedAt inserts an outbox row with an explicit published_at (for
// testing cleanup without going through the full relay cycle).
func insertPublishedAt(t *testing.T, pool *pg.Pool, id uuid.UUID, publishedAt *time.Time) {
	t.Helper()
	ctx := context.Background()
	if publishedAt == nil {
		_, err := pool.Writer().Exec(
			ctx,
			`insert into outbox (id, topic, aggregate_type, aggregate_id, event_type, payload, headers)
			 values ($1, 'orders.events', 'order', 'x', 'Test', '{}', '{}')`,
			id,
		)
		require.NoError(t, err)
	} else {
		_, err := pool.Writer().Exec(
			ctx,
			`insert into outbox (id, topic, aggregate_type, aggregate_id, event_type, payload, headers, published_at)
			 values ($1, 'orders.events', 'order', 'x', 'Test', '{}', '{}', $2)`,
			id, *publishedAt,
		)
		require.NoError(t, err)
	}
}

// TestCleaner_DeletesOldPublishedRowsOnly verifies that Cleanup removes
// published rows older than the retention window, and leaves behind:
//   - unpublished rows (regardless of age)
//   - recently-published rows (within retention window)
func TestCleaner_DeletesOldPublishedRowsOnly(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()

	oldPublished := uuid.New()
	recentPublished := uuid.New()
	unpublished := uuid.New()

	twoHoursAgo := time.Now().UTC().Add(-2 * time.Hour)
	thirtyMinsAgo := time.Now().UTC().Add(-30 * time.Minute)

	insertPublishedAt(t, pool, oldPublished, &twoHoursAgo)      // old published → should be deleted
	insertPublishedAt(t, pool, recentPublished, &thirtyMinsAgo) // recent published → kept
	insertPublishedAt(t, pool, unpublished, nil)                // unpublished → kept

	cleaner := outbox.NewCleaner(pool)
	deleted, err := cleaner.Cleanup(ctx, 1*time.Hour)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted, "only the one old-published row should be deleted")

	// old published row must be gone
	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where id = $1`, oldPublished).Scan(&count))
	require.Equal(t, 0, count, "old published row must be deleted")

	// recent published row must remain
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where id = $1`, recentPublished).Scan(&count))
	require.Equal(t, 1, count, "recently published row must remain")

	// unpublished row must remain
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where id = $1`, unpublished).Scan(&count))
	require.Equal(t, 1, count, "unpublished row must remain")
}

// TestCleaner_RunCleanup_TickerLoopCancels verifies that RunCleanup terminates
// promptly when the context is cancelled.
func TestCleaner_RunCleanup_TickerLoopCancels(t *testing.T) {
	pool := newPoolWithSchema(t)

	cleaner := outbox.NewCleaner(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- cleaner.RunCleanup(ctx, 20*time.Millisecond, 24*time.Hour)
	}()

	err := <-done
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"RunCleanup must return ctx.Err() on cancellation")
}

// explainPlan runs EXPLAIN for q in a transaction with sequential scans
// disabled, returning the plan text. Disabling seqscan forces the planner to
// pick an index when one is usable — on tiny test tables it would otherwise
// always prefer a sequential scan, hiding a missing index.
func explainPlan(t *testing.T, pool *pg.Pool, q string) string {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Writer().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `set local enable_seqscan = off`)
	require.NoError(t, err)

	rows, err := tx.Query(ctx, "explain "+q)
	require.NoError(t, err)
	defer rows.Close()

	var planLines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		planLines = append(planLines, line)
	}
	require.NoError(t, rows.Err())
	plan := strings.Join(planLines, "\n")
	return plan
}

// TestCleanup_DeleteUsesPublishedAtIndex asserts the retention cleanup DELETE
// is served by the partial index on published rows instead of a full-table
// scan (which previously ran hourly against the whole outbox).
func TestCleanup_DeleteUsesPublishedAtIndex(t *testing.T) {
	pool := newPoolWithSchema(t)

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		insertPublishedAt(t, pool, uuid.New(), &now)
	}

	plan := explainPlan(t, pool,
		`delete from outbox where published_at is not null and published_at < now() - interval '24 hours'`)
	require.Contains(t, plan, "outbox_published_at_idx",
		"retention DELETE must use the partial published_at index, got plan:\n%s", plan)
}
