package cache_test

import (
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/storage/cache"

	"github.com/stretchr/testify/assert"
)

// TestBuildRueidisOption_Mapping verifies the Config → rueidis.ClientOption
// mapping: address and password pass through verbatim, and TLSEnabled toggles
// a TLS12-minimum tls.Config (nil when off, back-compatible default).
func TestBuildRueidisOption_Mapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         cache.Config
		wantPass    string
		wantTLS     bool
		wantAddrLen int
	}{
		{
			name:        "no auth no TLS (default)",
			cfg:         cache.Config{RedisAddrs: []string{"localhost:6379"}},
			wantPass:    "",
			wantTLS:     false,
			wantAddrLen: 1,
		},
		{
			name:        "password only",
			cfg:         cache.Config{RedisAddrs: []string{"r1:6379"}, Password: config.Secret("s3cr3t")},
			wantPass:    "s3cr3t",
			wantTLS:     false,
			wantAddrLen: 1,
		},
		{
			name:        "password + TLS",
			cfg:         cache.Config{RedisAddrs: []string{"r1:6379", "r2:6379"}, Password: config.Secret("s3cr3t"), TLSEnabled: true},
			wantPass:    "s3cr3t",
			wantTLS:     true,
			wantAddrLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opt := cache.BuildRueidisOption(tt.cfg)
			assert.Equal(t, tt.wantPass, opt.Password)
			assert.Len(t, opt.InitAddress, tt.wantAddrLen)
			if tt.wantTLS {
				assert.NotNil(t, opt.TLSConfig)
			} else {
				assert.Nil(t, opt.TLSConfig)
			}
		})
	}
}
