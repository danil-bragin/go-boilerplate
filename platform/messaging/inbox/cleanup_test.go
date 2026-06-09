package inbox_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/inbox"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestInboxCleanup_DeletesOldProcessedRowsOnly verifies that Cleanup removes
// rows whose processed_at is older than the retention window, and keeps recent rows.
func TestInboxCleanup_DeletesOldProcessedRowsOnly(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()

	twoHoursAgo := time.Now().UTC().Add(-2 * time.Hour)
	thirtyMinsAgo := time.Now().UTC().Add(-30 * time.Minute)

	// Insert rows directly with backdated processed_at.
	_, err := pool.Writer().Exec(ctx,
		`insert into inbox (consumer, message_id, processed_at) values ($1, $2, $3)`,
		"c1", "old-msg", twoHoursAgo)
	require.NoError(t, err)

	_, err = pool.Writer().Exec(ctx,
		`insert into inbox (consumer, message_id, processed_at) values ($1, $2, $3)`,
		"c1", "recent-msg", thirtyMinsAgo)
	require.NoError(t, err)

	deleted, err := inbox.Cleanup(ctx, pool, 1*time.Hour)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted, "only the old processed row should be deleted")

	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from inbox where message_id = 'old-msg'`).Scan(&count))
	require.Equal(t, 0, count, "old row must be deleted")

	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from inbox where message_id = 'recent-msg'`).Scan(&count))
	require.Equal(t, 1, count, "recent row must remain")
}

// TestInboxRunCleanup_TickerLoopCancels verifies RunCleanup exits on context cancellation.
func TestInboxRunCleanup_TickerLoopCancels(t *testing.T) {
	pool := newPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- inbox.RunCleanup(ctx, pool, 20*time.Millisecond, 24*time.Hour)
	}()

	err := <-done
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"RunCleanup must return ctx.Err() on cancellation")
}

// TestInboxCleanup_DeleteUsesProcessedAtIndex asserts the inbox retention
// DELETE is served by the processed_at index instead of a full-table scan.
func TestInboxCleanup_DeleteUsesProcessedAtIndex(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := pool.Writer().Exec(ctx,
			`insert into inbox (consumer, message_id, processed_at) values ('c', $1, now())`,
			uuid.NewString())
		require.NoError(t, err)
	}

	tx, err := pool.Writer().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `set local enable_seqscan = off`)
	require.NoError(t, err)

	rows, err := tx.Query(ctx,
		`explain delete from inbox where processed_at < now() - interval '24 hours'`)
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
	require.Contains(t, plan, "inbox_processed_at_idx",
		"inbox retention DELETE must use the processed_at index, got plan:\n%s", plan)
}
