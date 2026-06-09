package inbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/inbox"
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
