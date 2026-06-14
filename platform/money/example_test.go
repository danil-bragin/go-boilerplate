package money_test

import (
	"fmt"

	"go-boilerplate/platform/money"
)

// Invoice: sum line items, apply a discount and VAT keeping full precision, then
// round only the final charge.
func ExampleMoney_invoice() {
	subtotal := money.MustParse("0", "USD")
	for _, l := range []money.Money{
		money.MustParse("19.99", "USD").Mul(money.DecFromInt(2)),
		money.MustParse("5.00", "USD"),
		money.MustParse("0.99", "USD").Mul(money.DecFromInt(3)),
	} {
		subtotal, _ = subtotal.Add(l)
	}
	discount := subtotal.Percent(money.MustDec("10")) // full precision, not rounded
	afterDiscount, _ := subtotal.Sub(discount)
	vat := afterDiscount.Percent(money.MustDec("20"))
	gross, _ := afterDiscount.Add(vat)
	charge := gross.Round(money.HalfEven) // round once, at the boundary

	fmt.Println("subtotal:", subtotal.Format(money.US))
	fmt.Println("charge:  ", charge.Format(money.US))
	// Output:
	// subtotal: $47.95
	// charge:   $51.79
}

// Split a bill among 3 with Fowler allocation — no cent is lost or created.
func ExampleMoney_Split() {
	total := money.MustParse("118.00", "USD")
	parts, _ := total.Split(3)
	for _, p := range parts {
		fmt.Println(p.Format(money.US))
	}
	// Output:
	// $39.34
	// $39.33
	// $39.33
}

// FX conversion at a supplied rate (money does no IO — the rate is passed in).
func ExampleMoney_Convert() {
	usd := money.MustParse("1000.00", "USD")
	eur, _ := usd.Convert(money.Rate{From: "USD", To: "EUR", Factor: money.MustDec("0.92")})
	fmt.Println(eur.Format(money.EU))
	// Output:
	// 920,00 €
}

// Crypto swap ETH→USDT with a 30bps fee; 18dp inputs, exact USDT output.
func ExampleMoney_cryptoSwap() {
	eth := money.MustParse("2.5", "ETH")
	gross, _ := money.Parse(
		eth.Mul(money.MustDec("3200.00")).Format(money.FormatOptions{DecimalSep: ".", Scale: money.ScaleFull}),
		"USDT",
	)
	fee := gross.Percent(money.MustDec("0.30"))
	net, _ := gross.Sub(fee)
	fmt.Println("gross:", gross.Format(money.CryptoFmt))
	fmt.Println("net:  ", net.Format(money.CryptoFmt))
	// Output:
	// gross: 8000 USDT
	// net:   7976 USDT
}

// Marketplace payout: split an order 85/15 conserving every cent.
func ExampleMoney_Allocate() {
	order := money.MustParse("59.99", "USD")
	parts, _ := order.Allocate(85, 15) // seller, platform
	fmt.Println("seller:  ", parts[0].Format(money.US))
	fmt.Println("platform:", parts[1].Format(money.US))
	// Output:
	// seller:   $51.00
	// platform: $8.99
}
