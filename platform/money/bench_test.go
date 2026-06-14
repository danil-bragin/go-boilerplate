package money_test

import (
	"encoding/json"
	"math/big"
	"testing"

	"go-boilerplate/platform/money"
)

// Benchmarks document the cost of the immutable, precision-exact design
// (precision > nanoseconds). Run: go test ./platform/money/ -bench . -benchmem -run '^$'

func BenchmarkAdd(b *testing.B) {
	x, y := money.MustParse("1234.56", "USD"), money.MustParse("78.90", "USD")
	b.ReportAllocs()
	for range b.N {
		_, _ = x.Add(y)
	}
}

func BenchmarkSub(b *testing.B) {
	x, y := money.MustParse("1234.56", "USD"), money.MustParse("78.90", "USD")
	b.ReportAllocs()
	for range b.N {
		_, _ = x.Sub(y)
	}
}

func BenchmarkMul(b *testing.B) {
	x := money.MustParse("1234.56", "USD")
	f := money.MustDec("1.0825")
	b.ReportAllocs()
	for range b.N {
		_ = x.Mul(f)
	}
}

func BenchmarkDivRound(b *testing.B) {
	x := money.MustParse("1000.00", "USD")
	d := money.MustDec("7")
	b.ReportAllocs()
	for range b.N {
		_, _ = x.DivRound(d, 2, money.HalfEven)
	}
}

func BenchmarkSplit(b *testing.B) {
	x := money.MustParse("100.00", "USD")
	b.ReportAllocs()
	for range b.N {
		_, _ = x.Split(7)
	}
}

func BenchmarkRound(b *testing.B) {
	x := money.MustParse("1234.5678", "USD")
	b.ReportAllocs()
	for range b.N {
		_ = x.Round(money.HalfEven)
	}
}

func BenchmarkParse(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_, _ = money.Parse("1234.56", "USD")
	}
}

func BenchmarkMarshalJSON(b *testing.B) {
	x := money.MustParse("1234.56", "USD")
	b.ReportAllocs()
	for range b.N {
		_, _ = json.Marshal(x)
	}
}

func BenchmarkUnmarshalJSON(b *testing.B) {
	data := []byte(`{"amount":"1234.56","asset":"USD"}`)
	b.ReportAllocs()
	for range b.N {
		var m money.Money
		_ = json.Unmarshal(data, &m)
	}
}

// --- v2 benches: format, validate, convert, large/uint256-scale ---

func bigStr(s string) *big.Int { b, _ := new(big.Int).SetString(s, 10); return b }

func BenchmarkFormat(b *testing.B) {
	m := money.MustParse("1234567.89", "USD")
	b.ReportAllocs()
	for range b.N {
		_ = m.Format(money.US)
	}
}

func BenchmarkValidate(b *testing.B) {
	m := money.MustParse("10.00", "USD")
	lo, hi := money.MustParse("1", "USD"), money.MustParse("100", "USD")
	b.ReportAllocs()
	for range b.N {
		_ = money.Validate(m, money.Positive(), money.MaxScale(), money.Between(lo, hi))
	}
}

func BenchmarkConvert(b *testing.B) {
	m := money.MustParse("100", "USD")
	r := money.Rate{From: "USD", To: "EUR", Factor: money.MustDec("0.92")}
	b.ReportAllocs()
	for range b.N {
		_, _ = m.Convert(r)
	}
}

func BenchmarkFromMinor(b *testing.B) {
	wei := bigStr("1000000000000000000")
	b.ReportAllocs()
	for range b.N {
		_, _ = money.FromMinor(wei, "ETH")
	}
}

func BenchmarkAddLarge(b *testing.B) {
	maxWei := bigStr("115792089237316195423570985008687907853269984665640564039457584007913129639935")
	x, _ := money.FromMinor(maxWei, "ETH")
	y, _ := money.FromMinor(bigStr("1"), "ETH")
	b.ReportAllocs()
	for range b.N {
		_, _ = x.Add(y)
	}
}

func BenchmarkMulLarge(b *testing.B) {
	maxWei := bigStr("115792089237316195423570985008687907853269984665640564039457584007913129639935")
	x, _ := money.FromMinor(maxWei, "ETH")
	f := money.MustDec("2")
	b.ReportAllocs()
	for range b.N {
		_ = x.Mul(f)
	}
}

func BenchmarkAllocateLarge(b *testing.B) {
	maxWei := bigStr("115792089237316195423570985008687907853269984665640564039457584007913129639935")
	x, _ := money.FromMinor(maxWei, "ETH")
	b.ReportAllocs()
	for range b.N {
		_, _ = x.Split(7)
	}
}
