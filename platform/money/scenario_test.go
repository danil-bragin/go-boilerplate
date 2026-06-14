package money_test

import (
	"math/big"
	"testing"

	"go-boilerplate/platform/money"
)

// These scenario tests model realistic fintech / crypto money math end-to-end.
// They double as worked examples of the correct discipline: keep FULL precision
// through +,-,x; round ONLY at the settlement boundary; split with Allocate
// (never lose a unit); convert with an explicit rate; never use float.

// eq asserts got equals MustParse(want, asset) by VALUE (scale-agnostic).
func eq(t *testing.T, got money.Money, want, asset string) {
	t.Helper()
	if !got.Equal(money.MustParse(want, asset)) {
		t.Fatalf("want %s %s, got %s", want, asset, got)
	}
}

// Scenario 1 — e-commerce order: line items → subtotal → 10% discount → 20% VAT,
// rounding only the final charge. Full precision is kept through the chain.
func TestScenario_EcommerceOrderWithDiscountAndVAT(t *testing.T) {
	usd := "USD"
	line := func(unit string, qty int64) money.Money {
		return money.MustParse(unit, usd).Mul(money.DecFromInt(qty))
	}
	subtotal := money.MustParse("0", usd)
	for _, l := range []money.Money{line("19.99", 2), line("5.00", 1), line("0.99", 3)} {
		subtotal, _ = subtotal.Add(l)
	}
	eq(t, subtotal, "47.95", usd) // 39.98 + 5.00 + 2.97

	discount := subtotal.Percent(money.MustDec("10")) // 4.795 — full precision, NOT rounded
	afterDiscount, _ := subtotal.Sub(discount)        // 43.155
	vat := afterDiscount.Percent(money.MustDec("20")) // 8.631
	gross, _ := afterDiscount.Add(vat)                // 51.786

	charge := gross.Round(money.HalfEven) // round ONCE at the boundary → 51.79
	eq(t, charge, "51.79", usd)
	if got := charge.Format(money.US); got != "$51.79" {
		t.Fatalf("invoice display: %s", got)
	}
}

// Scenario 2 — restaurant bill + 18% tip, split among 3 (Fowler: no cent lost).
func TestScenario_BillSplitWithTip(t *testing.T) {
	usd := "USD"
	bill := money.MustParse("100.00", usd)
	tip := bill.Percent(money.MustDec("18")) // 18.00
	total, _ := bill.Add(tip)                // 118.00
	parts, err := total.Split(3)
	if err != nil {
		t.Fatal(err)
	}
	// 118.00 / 3 → 39.34, 39.33, 39.33 (earliest gets the extra cent)
	eq(t, parts[0], "39.34", usd)
	eq(t, parts[1], "39.33", usd)
	eq(t, parts[2], "39.33", usd)
	sum := money.MustParse("0", usd)
	for _, p := range parts {
		sum, _ = sum.Add(p)
	}
	eq(t, sum, "118.00", usd) // conserved exactly
}

// Scenario 3 — Stripe-style processor fee: 2.9% + $0.30, net to merchant.
func TestScenario_PaymentProcessorFee(t *testing.T) {
	usd := "USD"
	charge := money.MustParse("47.55", usd)
	pct := charge.Percent(money.MustDec("2.9")).Round(money.HalfEven) // 1.37895 → 1.38
	fixed := money.MustParse("0.30", usd)
	fee, _ := pct.Add(fixed)  // 1.68
	net, _ := charge.Sub(fee) // 45.87
	eq(t, fee, "1.68", usd)
	eq(t, net, "45.87", usd)
}

// Scenario 4 — FX conversion with a 50bps broker spread on the mid-rate.
func TestScenario_FXConversionWithSpread(t *testing.T) {
	mid := money.MustDec("0.92")
	amount := money.MustParse("1000.00", "USD")
	// Convert at the mid-rate, then shave a 50bps (0.5%) spread off the converted
	// amount — full precision kept, no rounding mid-chain.
	gross, err := amount.Convert(money.Rate{From: "USD", To: "EUR", Factor: mid}) // 920.00
	if err != nil {
		t.Fatal(err)
	}
	fee := gross.Percent(money.MustDec("0.5")) // 4.60
	net, _ := gross.Sub(fee)                   // 915.40
	eq(t, net, "915.40", "EUR")
}

// Scenario 5 — crypto swap ETH→USDT at a price, 30bps fee; 18dp precision.
func TestScenario_CryptoSwapWithFee(t *testing.T) {
	eth := money.MustParse("2.5", "ETH") // 18dp asset
	price := money.MustDec("3200.00")    // USDT per ETH
	// gross = qtyETH × price, re-denominated into USDT (cross-asset multiply: the
	// ETH amount scaled by the price, then taken as USDT).
	grossUSDT, err := money.Parse(eth.Mul(price).Format(money.FormatOptions{DecimalSep: ".", GroupSep: "", Scale: money.ScaleFull}), "USDT")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, grossUSDT, "8000", "USDT")                // 2.5 × 3200
	fee := grossUSDT.Percent(money.MustDec("0.30")) // 30 bps = 24 USDT
	net, _ := grossUSDT.Sub(fee)                    // 7976 USDT
	eq(t, net, "7976", "USDT")

	// gas in wei (18dp): 21000 gas * 30 gwei = 630000 gwei = 0.00063 ETH
	gweiInWei := big.NewInt(1_000_000_000) // 1 gwei = 1e9 wei
	gas := new(big.Int).Mul(big.NewInt(21000*30), gweiInWei)
	gasETH, err := money.FromMinor(gas, "ETH")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, gasETH, "0.00063", "ETH")
}

// Scenario 6 — marketplace payout: split an order 85% seller / 15% platform with
// Allocate so the cents are conserved exactly (no rounding leak).
func TestScenario_MarketplacePayoutSplit(t *testing.T) {
	usd := "USD"
	order := money.MustParse("59.99", usd)
	parts, err := order.Allocate(85, 15) // seller, platform
	if err != nil {
		t.Fatal(err)
	}
	seller, platform := parts[0], parts[1]
	recombined, _ := seller.Add(platform)
	eq(t, recombined, "59.99", usd) // conserved — platform + seller == order
	// seller ≈ 85% of 59.99 = 50.9915 → 50.99 (+remainder); platform 8.9985 → 8.99
	eq(t, seller, "51.00", usd)
	eq(t, platform, "8.99", usd)
}

// Scenario 7 — simple interest accrual (ACT/365), rounded to cents at posting.
func TestScenario_SimpleInterestAccrual(t *testing.T) {
	usd := "USD"
	principal := money.MustParse("10000.00", usd)
	// interest = principal * APR(5%) * days/365 ; keep full precision, round at post
	annual := principal.Percent(money.MustDec("5")) // 500.00 / year
	// per-day rate via explicit DivRound (division must be explicit); keep 18dp.
	daily, err := annual.DivRound(money.DecFromInt(365), 18, money.HalfEven)
	if err != nil {
		t.Fatal(err)
	}
	accrued := daily.Mul(money.DecFromInt(30)) // 500/365 ×30 = 41.0958904… (full precision)
	posted := accrued.Round(money.HalfEven)    // round once at posting → 41.10
	eq(t, posted, "41.10", usd)
}

// Scenario 8 — crypto staking reward at exact wei precision.
func TestScenario_StakingRewardWei(t *testing.T) {
	stake := money.MustParse("32", "ETH")
	reward := stake.Percent(money.MustDec("4")) // 4% APR, 1 year = 1.28 ETH
	eq(t, reward, "1.28", "ETH")
	// 1.28 ETH == 1_280_000_000_000_000_000 wei, exact
	wei, _ := new(big.Int).SetString("1280000000000000000", 10)
	fromWei, err := money.FromMinor(wei, "ETH")
	if err != nil {
		t.Fatal(err)
	}
	if !reward.Equal(fromWei) {
		t.Fatalf("reward %s != %s", reward, fromWei)
	}
}

// Scenario 9 — partial refund (negative flow): capture net = charge - refund.
func TestScenario_PartialRefund(t *testing.T) {
	usd := "USD"
	charge := money.MustParse("50.00", usd)
	refund := money.MustParse("15.00", usd)
	net, _ := charge.Sub(refund) // 35.00 captured
	eq(t, net, "35.00", usd)
	// a full over-refund goes negative (allowed): refund 60 on a 50 charge
	over, _ := charge.Sub(money.MustParse("60.00", usd))
	if !over.IsNegative() {
		t.Fatal("over-refund must be negative")
	}
	eq(t, over, "-10.00", usd)
}
