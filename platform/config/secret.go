package config

import "log/slog"

// redacted is what every print/log path shows instead of a Secret's value.
const redacted = "[REDACTED]"

// Secret is a string config value that refuses to print itself. Use it for
// passwords, API keys, and tokens so that %v/%+v/%#v dumps of a config struct,
// slog output, and error messages never leak credentials:
//
//	type Config struct {
//	    SecretKey config.Secret `env:"S3_SECRET_KEY"`
//	}
//
// Access the raw value EXPLICITLY via Reveal() at the single call site that
// hands it to a client library. The explicit method makes credential flow
// greppable: `git grep "\.Reveal()"` lists every place a secret leaves the
// config layer.
type Secret string

// String implements fmt.Stringer — %v/%+v/%s print [REDACTED].
func (Secret) String() string { return redacted }

// GoString implements fmt.GoStringer — %#v prints [REDACTED] too.
func (Secret) GoString() string { return redacted }

// LogValue implements slog.LogValuer — structured logs carry [REDACTED].
func (Secret) LogValue() slog.Value { return slog.StringValue(redacted) }

// UnmarshalText implements encoding.TextUnmarshaler so caarlos0/env parses
// Secret fields from the environment like plain strings.
func (s *Secret) UnmarshalText(text []byte) error {
	*s = Secret(text)
	return nil
}

// MarshalText implements encoding.TextMarshaler so that serializing a config
// struct — to YAML (yaml.v3), or to JSON when MarshalJSON is somehow bypassed
// — emits [REDACTED] instead of the credential. This closes the gap the
// fmt/slog redaction left open: encoders never call String()/LogValue(), they
// call the marshaler. Returns the same redacted constant String() uses.
func (Secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }

// MarshalJSON implements json.Marshaler. encoding/json prefers MarshalJSON
// over MarshalText, so this is the belt-and-suspenders guarantee that
// json.Marshal of a struct containing a Secret never leaks the value — even
// if a future refactor drops MarshalText.
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }

// Reveal returns the raw secret value.
func (s Secret) Reveal() string { return string(s) }
