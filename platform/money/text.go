package money

import (
	"strings"
)

// MarshalText emits "<amount> <asset>", e.g. "123.45 USD" (for map keys, env, headers).
func (m Money) MarshalText() ([]byte, error) {
	return []byte(m.amount.String() + " " + m.asset), nil
}

// UnmarshalText parses "<amount> <asset>".
func (m *Money) UnmarshalText(text []byte) error {
	s := string(text)
	i := strings.LastIndexByte(s, ' ')
	if i < 0 {
		return codeErr(CodeParseFailed, "UnmarshalText").withDetail("bad text " + s + ", want \"<amount> <asset>\"")
	}
	parsed, err := Parse(s[:i], s[i+1:])
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
