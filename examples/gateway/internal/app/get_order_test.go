package app_test

import (
	"context"
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

// TestGetOrderHandler_InvalidIDIsNotFound pins the 404-not-500 contract for
// client-supplied ids: a non-UUID order id maps to ErrOrderNotFound BEFORE
// any database access (nil pool proves the read model is never touched —
// garbage ids must not consume a DB round-trip or leak an internal error).
func TestGetOrderHandler_InvalidIDIsNotFound(t *testing.T) {
	t.Parallel()
	handler := gatewayapp.GetOrderHandler(nil)

	for _, bad := range []string{"", "not-a-uuid", "../../../etc/passwd"} {
		_, err := handler(context.Background(), gatewayapp.GetOrder{OrderID: bad})
		assert.ErrorIs(t, err, gatewayapp.ErrOrderNotFound, "order id %q", bad)
	}
}
