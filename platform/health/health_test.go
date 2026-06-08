package health_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/health"
)

func TestReadyz_AllPassReturns200(t *testing.T) {
	h := health.New()
	h.AddReadiness("db", func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.ReadyzHandler().ServeHTTP(rec, req)

	require.Equal(t, 200, rec.Code)
}

func TestReadyz_OneFailureReturns503(t *testing.T) {
	h := health.New()
	h.AddReadiness("db", func(context.Context) error { return errors.New("down") })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.ReadyzHandler().ServeHTTP(rec, req)

	require.Equal(t, 503, rec.Code)
}

func TestLivez_AlwaysOK_UnlessNotLive(t *testing.T) {
	h := health.New()

	rec := httptest.NewRecorder()
	h.LivezHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/livez", nil))
	require.Equal(t, 200, rec.Code)

	h.SetNotLive() // shutdown flips liveness
	rec2 := httptest.NewRecorder()
	h.LivezHandler().ServeHTTP(rec2, httptest.NewRequest("GET", "/livez", nil))
	require.Equal(t, 503, rec2.Code)
}
