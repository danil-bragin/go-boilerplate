package money

import (
	"testing"

	moneyv1 "go-boilerplate/gen/proto/money/v1"
)

func TestProto_RoundTrip18dp(t *testing.T) {
	m := MustParse("123.450000000000000001", "ETH")
	pb := ToProto(m)
	if pb.GetAmount() != "123.450000000000000001" || pb.GetAsset() != "ETH" {
		t.Fatalf("toproto: %+v", pb)
	}
	back, err := FromProto(pb)
	if err != nil {
		t.Fatal(err)
	}
	if !back.Equal(m) {
		t.Fatalf("round-trip lost precision: %s vs %s", back, m)
	}
}

func TestProto_NilAndBadAsset(t *testing.T) {
	if _, err := FromProto(nil); err == nil {
		t.Fatal("nil proto must error")
	}
	if _, err := FromProto(&moneyv1.Money{Amount: "1", Asset: "ZZZ"}); err == nil {
		t.Fatal("bad asset must error")
	}
}
