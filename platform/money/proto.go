package money

import (
	"errors"

	moneyv1 "go-boilerplate/gen/proto/money/v1"
)

// ToProto encodes m as a money.v1.Money (amount as a full-precision decimal
// string). The decimal library is never leaked across the wire: the amount is
// emitted via String() so arbitrary precision (18dp crypto, FX) survives.
func ToProto(m Money) *moneyv1.Money {
	return &moneyv1.Money{Amount: m.amount.String(), Asset: m.asset}
}

// FromProto decodes a money.v1.Money: it validates the asset and parses the
// exact decimal amount (no float path). A nil proto is an error.
func FromProto(pb *moneyv1.Money) (Money, error) {
	if pb == nil {
		return Money{}, errors.New("money: nil proto")
	}
	return Parse(pb.GetAmount(), pb.GetAsset())
}
