package app_test

import (
	"testing"

	gatewayapp "go-boilerplate/examples/gateway/internal/app"

	"github.com/stretchr/testify/assert"
)

// TestOrderCacheKey_VersionedConvention pins the versioned cache-key
// convention: "<svc>:v<N>:<entity>:<id>". Bump the version segment whenever
// the cached result shape changes, so old entries become unreachable instead
// of unmarshalling into the new shape.
func TestOrderCacheKey_VersionedConvention(t *testing.T) {
	assert.Equal(t, "gw:v1:order:abc", gatewayapp.OrderCacheKey("abc"))
}
