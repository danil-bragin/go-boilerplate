package kafka

import (
	"testing"

	"go-boilerplate/platform/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSASLTLSOpts_Mapping verifies the Config → kgo.Opt mapping: each SASL
// mechanism yields exactly one SASL option, TLS yields one DialTLSConfig
// option, and an unknown mechanism is a fail-closed error.
//
// kgo.Opt is an opaque function type, so the assertion is on the COUNT of
// emitted options per config (the integration test below proves the SASL
// handshake actually works end-to-end against a real broker).
func TestSASLTLSOpts_Mapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      Config
		wantOpts int
		wantErr  bool
	}{
		{
			name:     "no SASL no TLS (back-compat default)",
			cfg:      Config{},
			wantOpts: 0,
		},
		{
			name:     "PLAIN",
			cfg:      Config{SASLMechanism: SASLPlain, SASLUser: "u", SASLPassword: config.Secret("p")},
			wantOpts: 1,
		},
		{
			name:     "SCRAM-SHA-256",
			cfg:      Config{SASLMechanism: SASLScramSHA256, SASLUser: "u", SASLPassword: config.Secret("p")},
			wantOpts: 1,
		},
		{
			name:     "SCRAM-SHA-512",
			cfg:      Config{SASLMechanism: SASLScramSHA512, SASLUser: "u", SASLPassword: config.Secret("p")},
			wantOpts: 1,
		},
		{
			name:     "TLS only",
			cfg:      Config{TLSEnabled: true},
			wantOpts: 1,
		},
		{
			name:     "SCRAM + TLS",
			cfg:      Config{SASLMechanism: SASLScramSHA512, SASLUser: "u", SASLPassword: config.Secret("p"), TLSEnabled: true},
			wantOpts: 2,
		},
		{
			name:     "TLS with insecure-skip-verify",
			cfg:      Config{TLSEnabled: true, TLSInsecureSkipVerify: true},
			wantOpts: 1,
		},
		{
			name:    "unsupported mechanism fails closed",
			cfg:     Config{SASLMechanism: "GSSAPI"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts, err := saslTLSOpts(tt.cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Len(t, opts, tt.wantOpts)
		})
	}
}

// TestNewClient_SASLConfig verifies NewClient accepts a valid SASL+TLS config
// (config validation only — no connection is made at construction) and
// rejects an unsupported mechanism before any dial attempt.
func TestNewClient_SASLConfig(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(Config{
		Brokers:       []string{"localhost:9092"},
		ClientID:      "test",
		SASLMechanism: SASLScramSHA256,
		SASLUser:      "user",
		SASLPassword:  config.Secret("pass"),
		TLSEnabled:    true,
	})
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	_, err = NewClient(Config{
		Brokers:       []string{"localhost:9092"},
		SASLMechanism: "NOPE",
	})
	require.Error(t, err)
}
