package money

import (
	"errors"
	"testing"
)

func TestConvert_RejectsNonPositiveRate(t *testing.T) {
	usd := MustParse("100", "USD")
	for _, f := range []string{"0", "-0.92"} {
		_, err := usd.Convert(Rate{From: "USD", To: "EUR", Factor: MustDec(f)})
		if !errors.Is(err, ErrInvalidRate) {
			t.Fatalf("Convert factor %s want ErrInvalidRate, got %v", f, err)
		}
	}
}
