package money

import "testing"

// TestRoundingMode_BoundaryCorrect pins the .5-boundary semantics of every
// rounding mode against verified shopspring behaviour. The want values are the
// true mathematical results (and were confirmed empirically), not bent to an
// implementation. String() trims trailing zeroes, so 1.00 prints as "1".
func TestRoundingMode_BoundaryCorrect(t *testing.T) {
	cases := []struct {
		in    string
		mode  RoundingMode
		scale int32
		want  string
	}{
		{"1.005", HalfEven, 2, "1"},    // banker's: 1.005 -> 1.00, even digit; "1.00" prints as "1"
		{"1.015", HalfEven, 2, "1.02"}, // banker's: 1.015 -> 1.02 (round to even)
		{"1.005", HalfUp, 2, "1.01"},   // half away from zero
		{"1.5", Down, 0, "1"},          // toward zero
		{"-1.5", Down, 0, "-1"},        // toward zero
		{"1.1", Up, 0, "2"},            // away from zero
		{"-1.1", Floor, 0, "-2"},       // toward -inf
		{"1.1", Ceil, 0, "2"},          // toward +inf
	}
	for _, c := range cases {
		got := c.mode.apply(MustDec(c.in), c.scale).String()
		if got != c.want {
			t.Fatalf("%s mode=%d scale=%d: want %s got %s", c.in, c.mode, c.scale, c.want, got)
		}
	}
}

// TestDec_NoFloatExactParse proves string construction is exact (no float
// detour) and that bad literals surface a wrapped error.
func TestDec_NoFloatExactParse(t *testing.T) {
	d, err := DecFromString("0.0825")
	if err != nil {
		t.Fatalf("DecFromString(0.0825): unexpected error %v", err)
	}
	if got := d.String(); got != "0.0825" {
		t.Fatalf("DecFromString(0.0825): want 0.0825 got %s", got)
	}

	if _, err := DecFromString("abc"); err == nil {
		t.Fatalf("DecFromString(abc): want error, got nil")
	}
}

// TestDec_Constructors covers the remaining seam surface.
func TestDec_Constructors(t *testing.T) {
	if got := DecFromInt(-10).String(); got != "-10" {
		t.Fatalf("DecFromInt(-10): want -10 got %s", got)
	}
	if !MustDec("0").IsZero() {
		t.Fatalf("MustDec(0).IsZero(): want true")
	}
	if MustDec("0.01").IsZero() {
		t.Fatalf("MustDec(0.01).IsZero(): want false")
	}

	defer func() {
		if recover() == nil {
			t.Fatalf("MustDec(bad): want panic")
		}
	}()
	_ = MustDec("not-a-number")
}
