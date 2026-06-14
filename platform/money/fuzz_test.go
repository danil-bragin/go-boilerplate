package money

import "testing"

// FuzzParse asserts the parser never panics, that the DoS guard holds for every
// accepted value, and that an accepted Money survives a canonical-string and a
// JSON round-trip unchanged.
func FuzzParse(f *testing.F) {
	for _, s := range []string{
		"0", "12.34", "-1", "1e5", "0.000000000000000001",
		"abc", "", "  ", "1e1000000000", "0.1e-1000", "-0.00",
		"99999999999999999999999999999999", "1.000000000000000000",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		m, err := Parse(s, "USD")
		if err != nil {
			return // rejected input is acceptable; we only assert on accepted values
		}
		// guard invariant: anything accepted is within the safe bounds.
		if m.amount.NumDigits() > maxParseDigits {
			t.Fatalf("accepted over-long coefficient: %q", s)
		}
		if e := m.amount.Exponent(); e > maxParseExp || e < -maxParseExp {
			t.Fatalf("accepted out-of-bounds exponent %d: %q", e, s)
		}
		// canonical-string round-trip is stable and value-preserving.
		m2, err := Parse(m.amount.String(), "USD")
		if err != nil {
			t.Fatalf("reparse of canonical %q failed: %v", m.amount.String(), err)
		}
		if !m.Equal(m2) {
			t.Fatalf("string round-trip mismatch: %s vs %s", m, m2)
		}
		// JSON round-trip is value-preserving.
		b, err := m.MarshalJSON()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m3 Money
		if err := m3.UnmarshalJSON(b); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		if !m.Equal(m3) {
			t.Fatalf("JSON round-trip mismatch: %s vs %s", m, m3)
		}
	})
}

// FuzzSplitConserves asserts Split never loses or creates value: the parts sum
// EXACTLY to the original truncated to the asset's smallest unit (Split works at
// minor-unit granularity and drops sub-cent dust), for any sign and part count.
func FuzzSplitConserves(f *testing.F) {
	f.Add("100.00", 3)
	f.Add("0.07", 4)
	f.Add("-12.34", 5)
	f.Add("0.005", 2)
	f.Fuzz(func(t *testing.T, amt string, n int) {
		m, err := Parse(amt, "USD")
		if err != nil {
			return
		}
		if n <= 0 || n > 1000 { // Split requires n>0; cap keeps the fuzz fast
			return
		}
		parts, err := m.Split(n)
		if err != nil {
			t.Fatalf("Split(%q,%d): %v", amt, n, err)
		}
		sum := parts[0]
		for _, p := range parts[1:] {
			sum, err = sum.Add(p)
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
		}
		want := m.Truncate(2) // USD smallest unit; Split conserves at this scale
		if !sum.Equal(want) {
			t.Fatalf("Split(%q,%d): Σparts=%s != %s", amt, n, sum, want)
		}
	})
}
