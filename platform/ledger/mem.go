package ledger

import (
	"context"
	"fmt"
	"sync"

	"go-boilerplate/platform/money"
)

// MemStore is an in-memory Store for tests and examples. It is safe for
// concurrent use. Accounts must be registered before they are referenced.
type MemStore struct {
	mu       sync.Mutex
	accounts map[string]Account
	entries  []Entry
	seen     map[string]struct{} // idempotency keys already applied
}

// NewMemStore builds an empty in-memory ledger.
func NewMemStore() *MemStore {
	return &MemStore{
		accounts: make(map[string]Account),
		seen:     make(map[string]struct{}),
	}
}

// RegisterAccount adds an account. The asset must be a registered money asset;
// re-registering the same id is an error.
func (m *MemStore) RegisterAccount(a Account) error {
	if _, ok := money.Lookup(a.Asset); !ok {
		return fmt.Errorf("ledger: account %s: unknown asset %q", a.ID, a.Asset)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.accounts[a.ID]; exists {
		return fmt.Errorf("ledger: account %s already registered", a.ID)
	}
	m.accounts[a.ID] = a
	return nil
}

// Post validates and applies a balanced posting; a repeated IdempotencyKey is a
// no-op. Accounts are checked (existence + asset match) before any mutation, so
// a rejected posting leaves the ledger untouched.
func (m *MemStore) Post(_ context.Context, p Posting) error {
	if err := p.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, done := m.seen[p.IdempotencyKey]; done {
		return nil
	}
	for _, e := range p.Entries {
		a, ok := m.accounts[e.AccountID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrUnknownAccount, e.AccountID)
		}
		if a.Asset != e.Amount.Asset() {
			return fmt.Errorf("%w: account %s is %s, entry is %s", ErrAccountAssetMismatch, e.AccountID, a.Asset, e.Amount.Asset())
		}
	}
	m.seen[p.IdempotencyKey] = struct{}{}
	m.entries = append(m.entries, p.Entries...)
	return nil
}

// Balance returns the account's derived balance (Σ normal-side − Σ opposite).
func (m *MemStore) Balance(_ context.Context, accountID string) (money.Money, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.accounts[accountID]
	if !ok {
		return money.Money{}, fmt.Errorf("%w: %s", ErrUnknownAccount, accountID)
	}
	bal := mustZero(a.Asset)
	for _, e := range m.entries {
		if e.AccountID != accountID {
			continue
		}
		delta := e.Amount
		if e.Direction != a.Normal {
			delta = e.Amount.Neg()
		}
		bal = mustAdd(bal, delta)
	}
	return bal, nil
}

// compile-time conformance.
var _ Store = (*MemStore)(nil)
