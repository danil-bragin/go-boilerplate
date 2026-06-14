package money

import "encoding/json"

// jsonMoney is the wire shape: amount is ALWAYS a string (never a JSON number,
// which would be float-parsed and lose precision).
type jsonMoney struct {
	Amount json.RawMessage `json:"amount"`
	Asset  string          `json:"asset"`
}

// MarshalJSON emits {"amount":"123.45","asset":"USD"} with amount as a string.
// The amount (a decimal string: digits, '.', '-', 'e') and the asset (a registry
// code: uppercase letters/digits) contain only JSON-safe characters, so they
// embed directly without escaping — MarshalJSON cannot fail.
func (m Money) MarshalJSON() ([]byte, error) {
	return []byte(`{"amount":"` + m.amount.String() + `","asset":"` + m.asset + `"}`), nil
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
