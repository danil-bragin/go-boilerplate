package ledger

import (
	"fmt"

	"go-boilerplate/platform/money"
)

// mustAdd sums two SAME-ASSET money values. Callers guarantee the assets match
// (entries are keyed/checked by asset upstream), so a cross-asset error here is
// not a runtime condition but a corrupted invariant — a programming bug — and
// is surfaced loudly rather than threaded through every accumulator as a dead
// error path.
func mustAdd(a, b money.Money) money.Money {
	sum, err := a.Add(b)
	if err != nil {
		panic(fmt.Sprintf("ledger: same-asset add failed (bug): %v", err))
	}
	return sum
}

// mustZero returns the zero of a REGISTERED asset. Account assets are validated
// at registration, so an unknown-asset error here is a corrupted invariant.
func mustZero(asset string) money.Money {
	z, err := money.Zero(asset)
	if err != nil {
		panic(fmt.Sprintf("ledger: zero of unregistered asset %q (bug): %v", asset, err))
	}
	return z
}
