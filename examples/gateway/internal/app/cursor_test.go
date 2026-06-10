package app

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCursorRoundTrip pins the keyset-cursor codec: the (created_at,
// order_id) position must survive encode → decode exactly, including
// sub-second precision (losing nanoseconds would skip or repeat rows at
// page boundaries).
func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 6, 10, 12, 34, 56, 789000123, time.UTC)
	id := uuid.New()

	gotAt, gotID, err := decodeCursor(encodeCursor(at, id))
	require.NoError(t, err)
	assert.True(t, at.Equal(gotAt), "created_at must round-trip exactly: %v != %v", at, gotAt)
	assert.Equal(t, id, gotID)
}

// TestDecodeCursor_MalformedInputs pins that every malformed cursor maps to
// ErrInvalidCursor (→ HTTP 400), never a 500: the cursor is opaque and
// client-supplied, so arbitrary garbage is expected traffic.
func TestDecodeCursor_MalformedInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		cursor string
	}{
		{"not base64", "!!!not-base64!!!"},
		{"no separator", b64("just-some-text")},
		{"bad timestamp", b64("yesterday|" + uuid.NewString())},
		{"bad uuid", b64(time.Now().UTC().Format(time.RFC3339Nano) + "|not-a-uuid")},
		{"empty payload", b64("")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := decodeCursor(tt.cursor)
			assert.ErrorIs(t, err, ErrInvalidCursor)
		})
	}
}

func b64(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}
