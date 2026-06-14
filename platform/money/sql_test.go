package money_test

import (
	"context"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/money"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
)

func TestSQL_NumericRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := context.Background()
	dsn := pgtest.NewDSN(t)
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	_, err = pool.Writer().Exec(ctx, `create table t (amount numeric not null, asset text not null)`)
	require.NoError(t, err)

	for _, s := range []struct{ amt, asset string }{
		{"12.34", "USD"},
		{"123.450000000000000001", "ETH"}, // 18dp crypto
		{"0", "JPY"},
	} {
		m := money.MustParse(s.amt, s.asset)
		_, err := pool.Writer().Exec(ctx, `insert into t(amount, asset) values ($1,$2)`, m.AmountValue(), m.Asset())
		require.NoError(t, err)
	}
	rows, err := pool.Reader().Query(ctx, `select amount::text, asset from t order by asset`)
	require.NoError(t, err)
	defer rows.Close()
	var got []money.Money
	for rows.Next() {
		var amt, asset string
		require.NoError(t, rows.Scan(&amt, &asset))
		m, err := money.ScanRow(amt, asset)
		require.NoError(t, err)
		got = append(got, m)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 3)
	// ETH 18dp survived
	var eth money.Money
	for _, m := range got {
		if m.Asset() == "ETH" {
			eth = m
		}
	}
	require.True(t, eth.Equal(money.MustParse("123.450000000000000001", "ETH")), "18dp NUMERIC round-trip lost precision: %s", eth)
}
