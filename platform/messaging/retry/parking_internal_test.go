package retry

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka"

	"github.com/stretchr/testify/require"
)

type nopProducer struct{}

func (nopProducer) Produce(context.Context, kafka.Record) error { return nil }

// TestPark_LazySweepBoundsMapSize (white-box) verifies the parked map is
// actually swept: after parkSweepEvery insertions with an already-expired
// window, expired entries are removed instead of accumulating forever.
func TestPark_LazySweepBoundsMapSize(t *testing.T) {
	esc := NewEscalator(nopProducer{}, DefaultPolicy(), WithKeyParking(time.Nanosecond))
	ctx := context.Background()

	for i := 0; i <= parkSweepEvery; i++ {
		_, err := esc.Escalate(ctx, "base", kafka.Record{
			Topic: "base",
			Key:   []byte("K" + strconv.Itoa(i)),
			Value: []byte("v"),
		}, errors.New("x"))
		require.NoError(t, err)
		time.Sleep(time.Microsecond) // let the nanosecond window expire
	}

	esc.parkMu.Lock()
	size := len(esc.parked)
	esc.parkMu.Unlock()
	require.LessOrEqual(t, size, 2,
		"sweep at parkSweepEvery insertions must remove expired entries (got %d)", size)
}
