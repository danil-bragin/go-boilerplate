package ledger

import "errors"

// Sentinel errors. Validation errors are returned by Posting.Validate and Post;
// lookup errors by Balance.
var (
	// ErrNoIdempotencyKey is a posting with an empty IdempotencyKey.
	ErrNoIdempotencyKey = errors.New("ledger: posting has no idempotency key")
	// ErrEmptyPosting is a posting with no entries.
	ErrEmptyPosting = errors.New("ledger: posting has no entries")
	// ErrNonPositiveAmount is an entry whose amount is not strictly positive
	// (amounts are magnitudes; the Direction carries the sign).
	ErrNonPositiveAmount = errors.New("ledger: entry amount must be positive")
	// ErrUnbalanced is a posting whose debits and credits do not net to zero
	// within some asset.
	ErrUnbalanced = errors.New("ledger: posting does not balance per asset")
	// ErrUnknownAccount is a reference to an account that is not registered.
	ErrUnknownAccount = errors.New("ledger: unknown account")
	// ErrAccountAssetMismatch is an entry whose amount asset differs from the
	// referenced account's asset.
	ErrAccountAssetMismatch = errors.New("ledger: entry asset does not match account asset")
)
