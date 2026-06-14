package money

import (
	"database/sql/driver"
	"fmt"

	"github.com/shopspring/decimal"
)

// AmountValue returns the amount as a driver.Valuer that PostgreSQL stores into a
// NUMERIC column with full precision. Asset() (in money.go) returns the asset
// code for the asset column.
//
// Canonical storage shape (two columns):
//
//	amount NUMERIC NOT NULL,   -- full precision, lossless (NEVER PG money/float)
//	asset  TEXT    NOT NULL
func (m Money) AmountValue() driver.Valuer { return m.amount }

// ScanRow builds a Money from the two scanned columns: amountSrc (a NUMERIC, as
// produced by the driver — a string, []byte, or decimal) and assetSrc (the asset
// text). Use after Scan(&amountSrc, &assetSrc).
func ScanRow(amountSrc any, assetSrc string) (Money, error) {
	if _, ok := Lookup(assetSrc); !ok {
		return Money{}, fmt.Errorf("money: %w: %s", ErrUnknownAsset, assetSrc)
	}
	var d decimal.Decimal
	if err := d.Scan(amountSrc); err != nil {
		return Money{}, fmt.Errorf("money: scan amount: %w", err)
	}
	return Money{amount: d, asset: assetSrc}, nil
}
