package ledger

import (
	"context"
	"errors"
	"testing"

	"go-boilerplate/platform/money"
)

func usd(s string) money.Money { return money.MustParse(s, "USD") }

func eur(s string) money.Money { return money.MustParse(s, "EUR") }

// --- Side ---

func TestSide_String(t *testing.T) {
	if Debit.String() != "debit" || Credit.String() != "credit" || Side(99).String() != "unknown" {
		t.Fatal("Side.String wrong")
	}
	if Debit.opposite() != Credit || Credit.opposite() != Debit {
		t.Fatal("opposite wrong")
	}
}

// --- Posting.Validate (pure) ---

func TestValidate_Balanced(t *testing.T) {
	p := Posting{IdempotencyKey: "k1", Entries: []Entry{
		{AccountID: "cash", Direction: Debit, Amount: usd("100.00")},
		{AccountID: "wallet", Direction: Credit, Amount: usd("100.00")},
	}}
	if err := p.Validate(); err != nil {
		t.Fatalf("balanced posting rejected: %v", err)
	}
}

func TestValidate_MultiAssetBalanced(t *testing.T) {
	// An FX posting: each asset balances independently.
	p := Posting{IdempotencyKey: "fx1", Entries: []Entry{
		{AccountID: "usd_out", Direction: Credit, Amount: usd("100.00")},
		{AccountID: "usd_clr", Direction: Debit, Amount: usd("100.00")},
		{AccountID: "eur_in", Direction: Debit, Amount: eur("92.00")},
		{AccountID: "eur_clr", Direction: Credit, Amount: eur("92.00")},
	}}
	if err := p.Validate(); err != nil {
		t.Fatalf("multi-asset balanced rejected: %v", err)
	}
}

func TestValidate_Errors(t *testing.T) {
	cases := []struct {
		name string
		p    Posting
		want error
	}{
		{"no key", Posting{Entries: []Entry{{AccountID: "a", Direction: Debit, Amount: usd("1")}}}, ErrNoIdempotencyKey},
		{"empty", Posting{IdempotencyKey: "k"}, ErrEmptyPosting},
		{"zero amount", Posting{IdempotencyKey: "k", Entries: []Entry{
			{AccountID: "a", Direction: Debit, Amount: usd("0")},
		}}, ErrNonPositiveAmount},
		{"negative amount", Posting{IdempotencyKey: "k", Entries: []Entry{
			{AccountID: "a", Direction: Debit, Amount: usd("-5")},
		}}, ErrNonPositiveAmount},
		{"unbalanced", Posting{IdempotencyKey: "k", Entries: []Entry{
			{AccountID: "a", Direction: Debit, Amount: usd("100")},
			{AccountID: "b", Direction: Credit, Amount: usd("99")},
		}}, ErrUnbalanced},
		{"one asset balances, other does not", Posting{IdempotencyKey: "k", Entries: []Entry{
			{AccountID: "a", Direction: Debit, Amount: usd("100")},
			{AccountID: "b", Direction: Credit, Amount: usd("100")},
			{AccountID: "c", Direction: Debit, Amount: eur("5")},
		}}, ErrUnbalanced},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.p.Validate(); !errors.Is(err, c.want) {
				t.Fatalf("want %v, got %v", c.want, err)
			}
		})
	}
}

// --- MemStore ---

func newLedger(t *testing.T) *MemStore {
	t.Helper()
	l := NewMemStore()
	// cash is a debit-normal asset account; wallet is a credit-normal liability.
	mustRegister(t, l, Account{ID: "cash", Asset: "USD", Normal: Debit})
	mustRegister(t, l, Account{ID: "wallet", Asset: "USD", Normal: Credit})
	return l
}

func mustRegister(t *testing.T, l *MemStore, a Account) {
	t.Helper()
	if err := l.RegisterAccount(a); err != nil {
		t.Fatalf("register %s: %v", a.ID, err)
	}
}

func TestMemStore_RegisterAccount_Errors(t *testing.T) {
	l := NewMemStore()
	if err := l.RegisterAccount(Account{ID: "x", Asset: "ZZZ", Normal: Debit}); err == nil {
		t.Fatal("unknown asset must be rejected")
	}
	mustRegister(t, l, Account{ID: "x", Asset: "USD", Normal: Debit})
	if err := l.RegisterAccount(Account{ID: "x", Asset: "USD", Normal: Debit}); err == nil {
		t.Fatal("duplicate id must be rejected")
	}
}

func TestMemStore_PostAndBalance(t *testing.T) {
	l := newLedger(t)
	ctx := context.Background()
	// Deposit $100: debit cash, credit wallet.
	deposit := Posting{IdempotencyKey: "dep-1", Entries: []Entry{
		{AccountID: "cash", Direction: Debit, Amount: usd("100.00")},
		{AccountID: "wallet", Direction: Credit, Amount: usd("100.00")},
	}}
	if err := l.Post(ctx, deposit); err != nil {
		t.Fatal(err)
	}
	assertBalance(t, l, "cash", "100.00")   // debit-normal, debited
	assertBalance(t, l, "wallet", "100.00") // credit-normal, credited

	// Withdraw $30: credit cash, debit wallet.
	if err := l.Post(ctx, Posting{IdempotencyKey: "wd-1", Entries: []Entry{
		{AccountID: "cash", Direction: Credit, Amount: usd("30.00")},
		{AccountID: "wallet", Direction: Debit, Amount: usd("30.00")},
	}}); err != nil {
		t.Fatal(err)
	}
	assertBalance(t, l, "cash", "70.00")
	assertBalance(t, l, "wallet", "70.00")
}

func TestMemStore_Idempotent(t *testing.T) {
	l := newLedger(t)
	ctx := context.Background()
	p := Posting{IdempotencyKey: "dep-1", Entries: []Entry{
		{AccountID: "cash", Direction: Debit, Amount: usd("100.00")},
		{AccountID: "wallet", Direction: Credit, Amount: usd("100.00")},
	}}
	for range 3 { // at-least-once delivery: same key applied thrice
		if err := l.Post(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	assertBalance(t, l, "cash", "100.00") // applied exactly once
}

func TestMemStore_Post_Errors(t *testing.T) {
	l := newLedger(t)
	ctx := context.Background()
	// validate error propagates
	if err := l.Post(ctx, Posting{IdempotencyKey: "", Entries: []Entry{
		{AccountID: "cash", Direction: Debit, Amount: usd("1")},
		{AccountID: "wallet", Direction: Credit, Amount: usd("1")},
	}}); !errors.Is(err, ErrNoIdempotencyKey) {
		t.Fatalf("want ErrNoIdempotencyKey, got %v", err)
	}
	// unknown account
	if err := l.Post(ctx, Posting{IdempotencyKey: "k", Entries: []Entry{
		{AccountID: "cash", Direction: Debit, Amount: usd("1")},
		{AccountID: "ghost", Direction: Credit, Amount: usd("1")},
	}}); !errors.Is(err, ErrUnknownAccount) {
		t.Fatalf("want ErrUnknownAccount, got %v", err)
	}
	// asset mismatch (wallet is USD, entry is EUR) — also balances in EUR so it
	// passes Validate and is caught by the store's account check.
	mustRegister(t, l, Account{ID: "ewallet", Asset: "EUR", Normal: Credit})
	if err := l.Post(ctx, Posting{IdempotencyKey: "k2", Entries: []Entry{
		{AccountID: "cash", Direction: Debit, Amount: eur("1")},
		{AccountID: "ewallet", Direction: Credit, Amount: eur("1")},
	}}); !errors.Is(err, ErrAccountAssetMismatch) {
		t.Fatalf("want ErrAccountAssetMismatch, got %v", err)
	}
	// a rejected posting left the ledger untouched
	assertBalance(t, l, "cash", "0")
}

func TestMemStore_Balance_UnknownAccount(t *testing.T) {
	l := newLedger(t)
	if _, err := l.Balance(context.Background(), "ghost"); !errors.Is(err, ErrUnknownAccount) {
		t.Fatalf("want ErrUnknownAccount, got %v", err)
	}
}

// TestMemStore_Conservation pins the core invariant: after any sequence of
// balanced postings, the signed sum of every account's balance is zero per
// asset — money is neither created nor destroyed.
func TestMemStore_Conservation(t *testing.T) {
	l := newLedger(t)
	ctx := context.Background()
	for i, a := range []string{"100.00", "33.33", "0.01", "7.50"} {
		key := "p" + string(rune('a'+i))
		if err := l.Post(ctx, Posting{IdempotencyKey: key, Entries: []Entry{
			{AccountID: "cash", Direction: Debit, Amount: usd(a)},
			{AccountID: "wallet", Direction: Credit, Amount: usd(a)},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	// cash is debit-normal (+), wallet is credit-normal; express both as a
	// debit-signed sum: cash balance − wallet balance must be zero.
	cash, _ := l.Balance(ctx, "cash")
	wallet, _ := l.Balance(ctx, "wallet")
	net, err := cash.Sub(wallet)
	if err != nil {
		t.Fatal(err)
	}
	if !net.IsZero() {
		t.Fatalf("conservation violated: cash %s, wallet %s, net %s", cash, wallet, net)
	}
}

func assertBalance(t *testing.T, l *MemStore, account, want string) {
	t.Helper()
	got, err := l.Balance(context.Background(), account)
	if err != nil {
		t.Fatalf("balance %s: %v", account, err)
	}
	if !got.Equal(usd(want)) {
		t.Fatalf("balance %s = %s, want %s", account, got, want)
	}
}

// --- invariant guards (white-box): the must-helpers panic on the "impossible"
// cross-asset / unknown-asset cases that signal a corrupted invariant. ---

func TestMustAdd(t *testing.T) {
	if got := mustAdd(usd("1.50"), usd("2.50")); !got.Equal(usd("4.00")) {
		t.Fatalf("mustAdd = %s", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("mustAdd cross-asset must panic")
		}
	}()
	_ = mustAdd(usd("1"), eur("1"))
}

func TestMustZero(t *testing.T) {
	if got := mustZero("USD"); !got.IsZero() {
		t.Fatalf("mustZero(USD) = %s", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("mustZero of unregistered asset must panic")
		}
	}()
	_ = mustZero("ZZZ")
}
