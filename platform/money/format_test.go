package money

import "testing"

func TestFormat_Presets(t *testing.T) {
	cases := []struct {
		amt, asset string
		opt        FormatOptions
		want       string
	}{
		{"1234.5", "USD", US, "$1,234.50"},
		{"1234.5", "EUR", EU, "1.234,50 €"},
		{"-1234.5", "USD", FormatOptions{Symbol: "$", SymbolPos: SymbolPrefix, GroupSep: ",", GroupSize: 3, DecimalSep: ".", Negative: NegParens, Scale: ScaleAsset}, "($1,234.50)"},
		{"1.5", "ETH", CryptoFmt, "1.5 ETH"},
		{"123", "JPY", US, "¥123"}, // 0-dp asset
	}
	for _, c := range cases {
		got := MustParse(c.amt, c.asset).Format(c.opt)
		if got != c.want {
			t.Fatalf("%s %s: want %q got %q", c.amt, c.asset, c.want, got)
		}
	}
}

func TestFormat_CryptoFullScaleExact(t *testing.T) {
	m := MustParse("1234567.890123456789", "ETH")
	if got := m.Format(CryptoFmt); got != "1234567.890123456789 ETH" {
		t.Fatalf("crypto full-scale: %s", got)
	}
}

func TestFormat_DisplayRoundDoesNotMutate(t *testing.T) {
	m := MustParse("1.005", "USD")
	_ = m.Format(US) // rounds to 2dp for display
	if m.String() != "1.005 USD" {
		t.Fatal("Format must not mutate the Money value")
	}
}
