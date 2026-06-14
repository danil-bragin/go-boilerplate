package money_test

import (
	"encoding/json"
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
		_ = x.DivRound(d, 2, money.HalfEven)
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
