package money

import (
	"bytes"
	"encoding/json"
)

// jsonMoney is the wire shape: amount is ALWAYS a string (never a JSON number,
// which would be float-parsed and lose precision).
type jsonMoney struct {
	Amount json.RawMessage `json:"amount"`
	Asset  string          `json:"asset"`
}

// MarshalJSON emits {"amount":"123.45","asset":"USD"} with amount as a string.
func (m Money) MarshalJSON() ([]byte, error) {
	amt, err := json.Marshal(m.amount.String())
	if err != nil {
		return nil, err
	}
	asset, err := json.Marshal(m.asset)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	b.WriteString(`{"amount":`)
	b.Write(amt)
	b.WriteString(`,"asset":`)
	b.Write(asset)
	b.WriteString(`}`)
	return b.Bytes(), nil
}

// UnmarshalJSON parses {"amount":"123.45","asset":"USD"}. The amount MUST be a
// JSON string — a JSON number is rejected (no float ingress).
func (m *Money) UnmarshalJSON(data []byte) error {
	var j jsonMoney
	if err := json.Unmarshal(data, &j); err != nil {
		return codeErr(CodeParseFailed, "UnmarshalJSON").wrap(err)
	}
	var amount string
	if err := json.Unmarshal(j.Amount, &amount); err != nil {
		return codeErr(CodeParseFailed, "UnmarshalJSON").withDetail("amount must be a JSON string, not a number").wrap(err)
	}
	parsed, err := Parse(amount, j.Asset)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
