package money_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"go-boilerplate/platform/money"
	"go-boilerplate/platform/storage/pg/pgtest"
)

// TestPG_NumericRoundTrip proves a Money survives a real pgx <-> PostgreSQL
// NUMERIC round-trip losslessly: AmountValue() (a driver.Valuer) on the write
// side, ScanRow on the read side, across fiat (2dp), crypto (18dp), a
// high-scale value, and a negative — all through the jackc/pgx driver the
// boilerplate actually uses (not just database/sql).
func TestPG_NumericRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	// Canonical two-column storage shape (see sql.go): NUMERIC + TEXT.
	if _, err := conn.Exec(
		ctx,
		`CREATE TABLE money_probe (id INT PRIMARY KEY, amount NUMERIC NOT NULL, asset TEXT NOT NULL)`,
	); err != nil {
		t.Fatalf("create table: %v", err)
	}

	values := []money.Money{
		money.MustParse("12.34", "USD"),                      // fiat 2dp
		money.MustParse("1.500000000000000001", "ETH"),       // crypto 18dp, exact wei
		money.MustParse("0.000000000000000000000001", "ETH"), // sub-wei high scale
		money.MustParse("-987654.21", "USD"),                 // negative
	}

	for i, m := range values {
		// write: AmountValue() is a driver.Valuer; pgx encodes it into NUMERIC.
		if _, err := conn.Exec(
			ctx,
			`INSERT INTO money_probe (id, amount, asset) VALUES ($1, $2, $3)`,
			i, m.AmountValue(), m.Asset(),
		); err != nil {
			t.Fatalf("insert %d (%s): %v", i, m, err)
		}
		// read: scan the NUMERIC back as text + the asset, rebuild via ScanRow.
		var amountText, asset string
		if err := conn.QueryRow(
			ctx,
			`SELECT amount::text, asset FROM money_probe WHERE id=$1`, i,
		).Scan(&amountText, &asset); err != nil {
			t.Fatalf("select %d: %v", i, err)
		}
		got, err := money.ScanRow(amountText, asset)
		if err != nil {
			t.Fatalf("ScanRow %d (%q,%q): %v", i, amountText, asset, err)
		}
		if !got.Equal(m) {
			t.Fatalf("round-trip %d: stored %q -> %s, want %s", i, amountText, got, m)
		}
	}
}
