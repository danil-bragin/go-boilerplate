package money

import "database/sql/driver"

// NullMoney is an optional Money for nullable database columns and optional wire
// fields. Valid reports presence; when Valid is false the Money is the zero
// value and must not be used. It keeps Money itself always-valid and
// non-nullable — absence is explicit in the type rather than smuggled through a
// zero value.
type NullMoney struct {
	Money Money
	Valid bool
}

// ValidMoney wraps a present Money as a NullMoney.
func ValidMoney(m Money) NullMoney { return NullMoney{Money: m, Valid: true} }

// Get returns the Money and whether it is present.
func (n NullMoney) Get() (Money, bool) { return n.Money, n.Valid }

// Or returns the Money if present, otherwise def.
func (n NullMoney) Or(def Money) Money {
	if n.Valid {
		return n.Money
	}
	return def
}

// MarshalJSON emits the Money object when present, or JSON null when absent.
func (n NullMoney) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return n.Money.MarshalJSON()
}

// UnmarshalJSON parses JSON null into an absent NullMoney, or a money object
// into a present one (subject to the same bounds/asset checks as Money).
func (n *NullMoney) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*n = NullMoney{}
		return nil
	}
	var m Money
	if err := m.UnmarshalJSON(data); err != nil {
		return err
	}
	*n = NullMoney{Money: m, Valid: true}
	return nil
}

// nullValuer is a driver.Valuer that produces SQL NULL.
type nullValuer struct{}

//nolint:nilnil // (nil, nil) is the database/sql idiom for a SQL NULL value.
func (nullValuer) Value() (driver.Value, error) { return nil, nil }

// AmountValue returns a driver.Valuer for the amount column: the underlying
// NUMERIC value when present, or SQL NULL when absent.
func (n NullMoney) AmountValue() driver.Valuer {
	if !n.Valid {
		return nullValuer{}
	}
	return n.Money.AmountValue()
}

// AssetValue returns the asset-column value: the asset code when present, or
// nil (SQL NULL) when absent.
func (n NullMoney) AssetValue() any {
	if !n.Valid {
		return nil
	}
	return n.Money.Asset()
}

// ScanNullRow builds a NullMoney from the two scanned columns. When BOTH are
// SQL NULL (nil) the result is absent; when both are present it delegates to
// ScanRow; a half-null row (one nil, one not) is an inconsistency and errors.
func ScanNullRow(amountSrc, assetSrc any) (NullMoney, error) {
	amountNull, assetNull := amountSrc == nil, assetSrc == nil
	if amountNull && assetNull {
		return NullMoney{}, nil
	}
	if amountNull != assetNull {
		return NullMoney{}, codeErr(CodeInvalidAmount, "ScanNullRow").
			withDetail("half-null row: amount and asset must both be null or both be set")
	}
	asset, ok := assetSrc.(string)
	if !ok {
		return NullMoney{}, codeErr(CodeParseFailed, "ScanNullRow").
			withDetail("asset column is not text")
	}
	m, err := ScanRow(amountSrc, asset)
	if err != nil {
		return NullMoney{}, err
	}
	return NullMoney{Money: m, Valid: true}, nil
}
