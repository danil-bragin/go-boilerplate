package money

import (
	"encoding/json"
	"testing"
)

func TestJSON_StringRoundTrip(t *testing.T) {
	m := MustParse("12.34", "USD")
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"amount":"12.34","asset":"USD"}` {
		t.Fatalf("marshal: %s", b)
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Equal(m) {
		t.Fatalf("round-trip: %s vs %s", back, m)
	}
}

func TestJSON_Crypto18dpExact(t *testing.T) {
	m := MustParse("123.450000000000000001", "ETH")
	b, _ := json.Marshal(m)
	var back Money
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Equal(m) {
		t.Fatalf("18dp lost precision: %s vs %s", back, m)
	}
}

func TestJSON_RejectNumberAmount(t *testing.T) {
	var m Money
	if err := json.Unmarshal([]byte(`{"amount":123.45,"asset":"USD"}`), &m); err == nil {
		t.Fatal("JSON-number amount must be rejected (float ingress)")
	}
}

func TestText_RoundTrip(t *testing.T) {
	m := MustParse("12.34", "USD")
	b, _ := m.MarshalText()
	if string(b) != "12.34 USD" {
		t.Fatalf("text: %s", b)
	}
	var back Money
	if err := back.UnmarshalText(b); err != nil {
		t.Fatal(err)
	}
	if !back.Equal(m) {
		t.Fatalf("text round-trip: %s vs %s", back, m)
	}
}
