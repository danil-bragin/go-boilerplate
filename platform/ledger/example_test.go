package ledger_test

import (
	"context"
	"fmt"

	"go-boilerplate/platform/ledger"
	"go-boilerplate/platform/money"
)

// A wallet deposit then a partial withdrawal, with balances derived from the
// entries. cash is a debit-normal asset account; wallet is a credit-normal
// liability the platform owes the user.
func Example() {
	l := ledger.NewMemStore()
	_ = l.RegisterAccount(ledger.Account{ID: "cash", Asset: "USD", Normal: ledger.Debit})
	_ = l.RegisterAccount(ledger.Account{ID: "wallet", Asset: "USD", Normal: ledger.Credit})

	ctx := context.Background()
	usd := func(s string) money.Money { return money.MustParse(s, "USD") }

	// Deposit $100.00 — debit cash, credit the user's wallet (balanced).
	_ = l.Post(ctx, ledger.Posting{IdempotencyKey: "deposit-1", Entries: []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: usd("100.00")},
		{AccountID: "wallet", Direction: ledger.Credit, Amount: usd("100.00")},
	}})
	// Withdraw $30.00 — credit cash, debit the wallet.
	_ = l.Post(ctx, ledger.Posting{IdempotencyKey: "withdraw-1", Entries: []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Credit, Amount: usd("30.00")},
		{AccountID: "wallet", Direction: ledger.Debit, Amount: usd("30.00")},
	}})

	cash, _ := l.Balance(ctx, "cash")
	wallet, _ := l.Balance(ctx, "wallet")
	fmt.Println("cash:  ", cash.Format(money.US))
	fmt.Println("wallet:", wallet.Format(money.US))
	// Output:
	// cash:   $70.00
	// wallet: $70.00
}
