package money

import (
	"errors"
	"fmt"
	"sync"
)

// Kind classifies an asset as government-issued fiat or a crypto asset. It is a
// comparable scalar so Asset stays a comparable value type.
type Kind int

const (
	// Fiat is a government-issued currency (ISO 4217).
	Fiat Kind = iota
	// Crypto is a crypto asset / token.
	Crypto
)

// Asset is the immutable definition of a unit of value. Exponent is the number
// of decimal places in the smallest representable unit (the minor unit): a
// USD amount stores 2 decimals (cents), JPY stores 0, BHD stores 3, BTC stores
// 8 (satoshi). The exponent — never a hardcoded multiplier — drives rounding
// and integer storage. All fields are comparable so two Assets can be compared
// with == for idempotent re-registration.
type Asset struct {
	Code     string
	Exponent int32
	Kind     Kind
}

var (
	registryMu sync.RWMutex
	registry   map[string]Asset
)

func init() {
	registry = make(map[string]Asset)
	for _, a := range builtinAssets() {
		registry[a.Code] = a
	}
}

// Register adds a custom asset to the registry. It validates that the code is
// non-empty and the exponent is non-negative. Registration is idempotent: a
// re-register with an identical definition is a no-op success. A re-register
// that conflicts with an existing code (different exponent or kind) is an error
// — definitions are immutable once seeded.
func Register(a Asset) error {
	if a.Code == "" {
		return errors.New("money: register asset: empty code")
	}
	if a.Exponent < 0 {
		return fmt.Errorf("money: register asset %q: negative exponent %d", a.Code, a.Exponent)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if existing, ok := registry[a.Code]; ok && existing != a {
		return fmt.Errorf("money: register asset %q: conflicts with existing definition %+v", a.Code, existing)
	}
	registry[a.Code] = a
	return nil
}

// Lookup returns the registered asset for code and whether it was found.
func Lookup(code string) (Asset, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	a, ok := registry[code]
	return a, ok
}

// builtinAssets seeds the registry with the active ISO 4217 fiat currencies and
// a set of common crypto assets.
//
// Source: ISO 4217 (List One — currencies and funds), as maintained by SIX
// Financial Information on behalf of the ISO 4217 Maintenance Agency. Most fiat
// currencies use a minor unit of 2 decimal places. The exceptions seeded here
// match the official "minor unit" column:
//
//	0 decimals: BIF CLP DJF GNF ISK JPY KMF KRW PYG RWF UGX UYI VND VUV XAF XOF XPF
//	3 decimals: BHD IQD JOD KWD LYD OMR TND
//	4 decimals: CLF UYW
//
// Crypto exponents reflect each chain's smallest unit (e.g. BTC satoshi = 1e-8,
// ETH wei = 1e-18).
func builtinAssets() []Asset {
	exp0 := []string{
		"BIF", "CLP", "DJF", "GNF", "ISK", "JPY", "KMF", "KRW",
		"PYG", "RWF", "UGX", "UYI", "VND", "VUV", "XAF", "XOF", "XPF",
	}
	exp3 := []string{"BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND"}
	exp4 := []string{"CLF", "UYW"}

	// Active ISO 4217 fiat codes with the standard 2-decimal minor unit.
	exp2 := []string{
		"AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN",
		"BAM", "BBD", "BDT", "BGN", "BMD", "BND", "BOB", "BOV", "BRL", "BSD",
		"BTN", "BWP", "BYN", "BZD", "CAD", "CDF", "CHE", "CHF", "CHW", "CNY",
		"COP", "COU", "CRC", "CUP", "CVE", "CZK", "DKK", "DOP", "DZD", "EGP",
		"ERN", "ETB", "EUR", "FJD", "FKP", "GBP", "GEL", "GHS", "GIP", "GMD",
		"GTQ", "GYD", "HKD", "HNL", "HRK", "HTG", "HUF", "IDR", "ILS", "INR",
		"IRR", "JMD", "KES", "KGS", "KHR", "KPW", "KYD", "KZT", "LAK", "LBP",
		"LKR", "LRD", "LSL", "MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP",
		"MRU", "MUR", "MVR", "MWK", "MXN", "MXV", "MYR", "MZN", "NAD", "NGN",
		"NIO", "NOK", "NPR", "NZD", "PAB", "PEN", "PGK", "PHP", "PKR", "PLN",
		"QAR", "RON", "RSD", "RUB", "SAR", "SBD", "SCR", "SDG", "SEK", "SGD",
		"SHP", "SLE", "SOS", "SRD", "SSP", "STN", "SVC", "SYP", "SZL", "THB",
		"TJS", "TMT", "TOP", "TRY", "TTD", "TWD", "TZS", "UAH", "USD", "USN",
		"UYU", "UZS", "VED", "VES", "WST", "YER", "ZAR", "ZMW", "ZWG", "ZWL",
	}

	crypto := []Asset{
		{Code: "BTC", Exponent: 8, Kind: Crypto},
		{Code: "ETH", Exponent: 18, Kind: Crypto},
		{Code: "USDT", Exponent: 6, Kind: Crypto},
		{Code: "USDC", Exponent: 6, Kind: Crypto},
		{Code: "SOL", Exponent: 9, Kind: Crypto},
		{Code: "BNB", Exponent: 18, Kind: Crypto},
		{Code: "XRP", Exponent: 6, Kind: Crypto},
		{Code: "ADA", Exponent: 6, Kind: Crypto},
		{Code: "DOGE", Exponent: 8, Kind: Crypto},
		{Code: "LTC", Exponent: 8, Kind: Crypto},
	}

	assets := make([]Asset, 0, len(exp0)+len(exp2)+len(exp3)+len(exp4)+len(crypto))
	add := func(codes []string, exp int32) {
		for _, c := range codes {
			assets = append(assets, Asset{Code: c, Exponent: exp, Kind: Fiat})
		}
	}
	add(exp0, 0)
	add(exp2, 2)
	add(exp3, 3)
	add(exp4, 4)
	assets = append(assets, crypto...)
	return assets
}
