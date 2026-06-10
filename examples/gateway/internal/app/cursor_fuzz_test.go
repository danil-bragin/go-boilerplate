package app

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// FuzzCursorDecode hammers the opaque keyset-cursor codec with arbitrary
// client-supplied strings. The cursor arrives straight from a query
// parameter, so any byte sequence is expected traffic. Invariants:
//
//  1. decodeCursor never panics.
//  2. Every failure is ErrInvalidCursor (maps to HTTP 400, never a 500).
//  3. Anything that decodes successfully survives a re-encode → re-decode
//     round-trip exactly (a lossy codec would skip or repeat rows at page
//     boundaries).
func FuzzCursorDecode(f *testing.F) {
	// Seeds from cursor_test.go: one valid cursor and the malformed corpus.
	f.Add(encodeCursor(time.Date(2026, 6, 10, 12, 34, 56, 789000123, time.UTC), uuid.New()))
	f.Add("!!!not-base64!!!")
	f.Add(b64("just-some-text"))
	f.Add(b64("yesterday|" + uuid.NewString()))
	f.Add(b64(time.Now().UTC().Format(time.RFC3339Nano) + "|not-a-uuid"))
	f.Add(b64(""))
	f.Add("")
	f.Add(b64("|"))
	f.Add(b64("2026-06-10T12:34:56Z|" + uuid.NewString() + "|extra"))

	f.Fuzz(func(t *testing.T, cursor string) {
		at, id, err := decodeCursor(cursor)
		if err != nil {
			if !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("decodeCursor(%q) returned non-ErrInvalidCursor error: %v", cursor, err)
			}
			return
		}

		// Round-trip: re-encoding the decoded position must decode to the
		// same instant and id.
		at2, id2, err := decodeCursor(encodeCursor(at, id))
		if err != nil {
			t.Fatalf("re-encoded cursor failed to decode: %v (from %q → %v %v)", err, cursor, at, id)
		}
		if !at.Equal(at2) || id != id2 {
			t.Fatalf("cursor round-trip mismatch: (%v, %v) != (%v, %v) from %q", at, id, at2, id2, cursor)
		}
	})
}
