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

// TestPG_NullMoneyRoundTrip proves NullMoney maps to a nullable two-column
// shape through pgx: a present value writes NUMERIC+TEXT and reads back valid;
// an absent value writes SQL NULL+NULL and reads back invalid.
func TestPG_NullMoneyRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	if _, err := conn.Exec(
		ctx,
		`CREATE TABLE null_probe (id INT PRIMARY KEY, amount NUMERIC, asset TEXT)`,
	); err != nil {
		t.Fatalf("create table: %v", err)
	}

	cases := []struct {
		id int
		nm money.NullMoney
	}{
		{1, money.ValidMoney(money.MustParse("12.34", "USD"))},
		{2, money.NullMoney{}}, // absent -> NULL, NULL
	}
	for _, c := range cases {
		if _, err := conn.Exec(
			ctx,
			`INSERT INTO null_probe (id, amount, asset) VALUES ($1, $2, $3)`,
			c.id, c.nm.AmountValue(), c.nm.AssetValue(),
		); err != nil {
			t.Fatalf("insert %d: %v", c.id, err)
		}
		var amountSrc, assetSrc any
		if err := conn.QueryRow(
			ctx,
			`SELECT amount::text, asset FROM null_probe WHERE id=$1`, c.id,
		).Scan(&amountSrc, &assetSrc); err != nil {
			t.Fatalf("select %d: %v", c.id, err)
		}
		got, err := money.ScanNullRow(amountSrc, assetSrc)
		if err != nil {
			t.Fatalf("ScanNullRow %d: %v", c.id, err)
		}
		if got.Valid != c.nm.Valid {
			t.Fatalf("row %d: Valid=%v want %v", c.id, got.Valid, c.nm.Valid)
		}
		if c.nm.Valid && !got.Money.Equal(c.nm.Money) {
			t.Fatalf("row %d: %s != %s", c.id, got.Money, c.nm.Money)
		}
	}
}
