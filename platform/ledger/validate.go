package ledger

import "go-boilerplate/platform/money"

// Validate checks the posting is well-formed and balances WITHOUT consulting
// any account (a pure, storage-independent check): it has an idempotency key
// and at least one entry, every amount is strictly positive, and within each
// asset the signed sum of entries (debit +, credit −) is exactly zero.
//
// It does NOT check that referenced accounts exist or that an entry's asset
// matches its account — those need account knowledge and are enforced by the
// Store at Post time.
func (p Posting) Validate() error {
	if p.IdempotencyKey == "" {
		return ErrNoIdempotencyKey
	}
	if len(p.Entries) == 0 {
		return ErrEmptyPosting
	}
	// net[asset] accumulates the signed sum; a posting balances iff every asset
	// nets to zero.
	net := make(map[string]money.Money, 2)
	for _, e := range p.Entries {
		if !e.Amount.IsPositive() {
			return ErrNonPositiveAmount
		}
		signed := e.Amount
		if e.Direction == Credit {
			signed = e.Amount.Neg()
		}
		asset := e.Amount.Asset()
		cur, ok := net[asset]
		if !ok {
			net[asset] = signed
			continue
		}
		net[asset] = mustAdd(cur, signed) // same asset by construction
	}
	for _, n := range net {
		if !n.IsZero() {
			return ErrUnbalanced
		}
	}
	return nil
}
