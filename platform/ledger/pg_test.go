package ledger_test

import (
	"context"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/ledger"
	"go-boilerplate/platform/money"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"
)

func newPgStore(t *testing.T) (*ledger.PgStore, *pg.Pool) {
	t.Helper()
	dsn := pgtest.NewDSN(t)
	ctx := context.Background()
	if err := pg.Migrate(ctx, dsn, ledger.Migrations, "migrations"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	store := ledger.NewPgStore(pool)
	for _, a := range []ledger.Account{
		{ID: "cash", Asset: "USD", Normal: ledger.Debit},
		{ID: "wallet", Asset: "USD", Normal: ledger.Credit},
	} {
		if err := store.RegisterAccount(ctx, a); err != nil {
			t.Fatalf("register %s: %v", a.ID, err)
		}
	}
	return store, pool
}

func usd(s string) money.Money { return money.MustParse(s, "USD") }

// post runs a posting inside its own transaction (the atomicity invariant).
func post(t *testing.T, pool *pg.Pool, store *ledger.PgStore, p ledger.Posting) error {
	t.Helper()
	return pg.RunInTx(context.Background(), pool, func(ctx context.Context) error {
		return store.Post(ctx, p)
	})
}

func TestPg_PostBalanceIdempotentConservation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	store, pool := newPgStore(t)
	ctx := context.Background()

	deposit := ledger.Posting{IdempotencyKey: "dep-1", Entries: []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: usd("100.00")},
		{AccountID: "wallet", Direction: ledger.Credit, Amount: usd("100.00")},
	}}
	// Apply the SAME key three times — at-least-once delivery must not double-apply.
	for range 3 {
		if err := post(t, pool, store, deposit); err != nil {
			t.Fatal(err)
		}
	}
	assertBalance(t, store, "cash", "100.00")
	assertBalance(t, store, "wallet", "100.00")

	// A withdrawal moves the balances back.
	if err := post(t, pool, store, ledger.Posting{IdempotencyKey: "wd-1", Entries: []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Credit, Amount: usd("30.00")},
		{AccountID: "wallet", Direction: ledger.Debit, Amount: usd("30.00")},
	}}); err != nil {
		t.Fatal(err)
	}
	assertBalance(t, store, "cash", "70.00")
	assertBalance(t, store, "wallet", "70.00")

	// Conservation: signed sum across accounts is zero (cash − wallet == 0).
	cash, _ := store.Balance(ctx, "cash")
	wallet, _ := store.Balance(ctx, "wallet")
	net, err := cash.Sub(wallet)
	if err != nil || !net.IsZero() {
		t.Fatalf("conservation violated: cash %s wallet %s net %s err %v", cash, wallet, net, err)
	}
}

func TestPg_Rejections(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	store, pool := newPgStore(t)

	// unbalanced -> rejected by Validate, nothing written
	if err := post(t, pool, store, ledger.Posting{IdempotencyKey: "bad-1", Entries: []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: usd("100.00")},
		{AccountID: "wallet", Direction: ledger.Credit, Amount: usd("99.00")},
	}}); err == nil {
		t.Fatal("unbalanced posting must be rejected")
	}
	// unknown account
	if err := post(t, pool, store, ledger.Posting{IdempotencyKey: "bad-2", Entries: []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: usd("1.00")},
		{AccountID: "ghost", Direction: ledger.Credit, Amount: usd("1.00")},
	}}); err == nil {
		t.Fatal("unknown account must be rejected")
	}
	assertBalance(t, store, "cash", "0")

	// unknown account balance
	if _, err := store.Balance(context.Background(), "ghost"); err == nil {
		t.Fatal("balance of unknown account must error")
	}
}

func assertBalance(t *testing.T, store *ledger.PgStore, account, want string) {
	t.Helper()
	got, err := store.Balance(context.Background(), account)
	if err != nil {
		t.Fatalf("balance %s: %v", account, err)
	}
	if !got.Equal(usd(want)) {
		t.Fatalf("balance %s = %s, want %s", account, got, want)
	}
}

// TestPg_RegisterAccount_UnknownAsset needs no database: the asset is rejected
// before any query.
func TestPg_RegisterAccount_UnknownAsset(t *testing.T) {
	store := ledger.NewPgStore(nil)
	if err := store.RegisterAccount(context.Background(), ledger.Account{ID: "x", Asset: "ZZZ", Normal: ledger.Debit}); err == nil {
		t.Fatal("unknown asset must be rejected before touching the DB")
	}
}

func TestPg_AssetMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	store, pool := newPgStore(t)
	// register a EUR liability; a balanced EUR posting that debits the USD
	// 'cash' account must be rejected on the asset check.
	if err := store.RegisterAccount(context.Background(), ledger.Account{ID: "ewallet", Asset: "EUR", Normal: ledger.Credit}); err != nil {
		t.Fatal(err)
	}
	err := post(t, pool, store, ledger.Posting{IdempotencyKey: "mm-1", Entries: []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: money.MustParse("1.00", "EUR")},
		{AccountID: "ewallet", Direction: ledger.Credit, Amount: money.MustParse("1.00", "EUR")},
	}})
	if err == nil {
		t.Fatal("entry asset not matching account asset must be rejected")
	}
}
