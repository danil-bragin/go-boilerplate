// Package ledger is an append-only, double-entry ledger for services that own
// balances and must guarantee money is neither created nor destroyed.
//
// # When to use a ledger
//
// Most services do NOT need this. An e-commerce/PSP flow that records an amount
// per row (see examples/orders, examples/payments) is fine — money moves
// through an external processor and the row is a record, not an account. Reach
// for a ledger only when YOUR service is the system of record for balances
// (wallets, accounts, internal credits) and must answer "what is the balance"
// and "do all movements sum to zero" with certainty.
//
// # Model
//
// Every movement is a Posting: a set of Entries that MUST balance — within each
// asset, the sum of debits equals the sum of credits. An Entry is one leg
// (account, direction, amount); the amount is a magnitude (money.Money, always
// positive) and the Direction (Debit/Credit) carries the sign. Each Account has
// a single asset and a Normal side; its balance is DERIVED from its entries
// (never stored as the source of truth):
//
//	balance = Σ(entries on the Normal side) − Σ(entries on the opposite side)
//
// Because every posting balances per asset, the signed sum of all account
// balances is exactly zero per asset — money is conserved by construction.
//
// # Guarantees
//
//   - Double-entry: an unbalanced Posting is rejected before any write.
//   - Append-only: entries and transactions are never updated or deleted.
//   - Idempotent: a Posting carries an IdempotencyKey; re-posting the same key
//     is a no-op, so at-least-once delivery cannot double-apply.
//
// Money values use platform/money, so precision is exact and never lost.
package ledger

import (
	"context"

	"go-boilerplate/platform/money"
)

// Side is a double-entry direction (for an Entry) or an account's normal
// balance side (for an Account).
type Side int

const (
	// Debit is the left side of the ledger.
	Debit Side = iota
	// Credit is the right side of the ledger.
	Credit
)

// String renders the side as "debit"/"credit" (also the DB token).
func (s Side) String() string {
	switch s {
	case Debit:
		return "debit"
	case Credit:
		return "credit"
	default:
		return "unknown"
	}
}

// opposite returns the other side.
func (s Side) opposite() Side {
	if s == Debit {
		return Credit
	}
	return Debit
}

// Account is a single-asset account. Its balance is derived from its entries;
// Normal is the side on which the balance increases (Debit for asset/expense
// accounts, Credit for liability/equity/revenue accounts).
type Account struct {
	ID     string
	Asset  string
	Normal Side
}

// Entry is one leg of a Posting: a movement of Amount on an account, in a
// Direction. Amount is a magnitude — always positive; the Direction carries the
// sign. Its asset must match the account's asset.
type Entry struct {
	AccountID string
	Direction Side
	Amount    money.Money
}

// Posting is an atomic, balanced set of entries applied together. IdempotencyKey
// makes a Post exactly-once: re-posting the same key is a no-op.
type Posting struct {
	IdempotencyKey string
	Entries        []Entry
}

// Store records balanced postings append-only and answers account balances.
// Implementations are idempotent on IdempotencyKey and atomic per Posting.
type Store interface {
	// Post validates and records a balanced posting. Re-posting an already-seen
	// IdempotencyKey is a no-op (returns nil). An unbalanced or malformed
	// posting is rejected with no write.
	Post(ctx context.Context, p Posting) error

	// Balance returns the derived balance of an account (Σ normal-side −
	// Σ opposite-side), in the account's asset.
	Balance(ctx context.Context, accountID string) (money.Money, error)
}
