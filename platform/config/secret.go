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

// Reveal returns the raw secret value.
func (s Secret) Reveal() string { return string(s) }
