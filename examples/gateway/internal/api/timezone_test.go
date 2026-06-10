package api

import (
	"context"
	"testing"
	"time"

	"go-boilerplate/examples/gateway/internal/app"
	"go-boilerplate/examples/gateway/internal/apperrs"
	"go-boilerplate/platform/apperr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFormatCreatedAt_AlwaysUTCZ: the contract field is RFC 3339 UTC with
// the "Z" suffix no matter what location the time.Time carries.
func TestFormatCreatedAt_AlwaysUTCZ(t *testing.T) {
	kyiv, err := time.LoadLocation("Europe/Kyiv")
	require.NoError(t, err)

	utc := time.Date(2026, 6, 10, 12, 34, 56, 789000000, time.UTC)
	assert.Equal(t, "2026-06-10T12:34:56Z", formatCreatedAt(utc),
		"sub-second precision is truncated, Z suffix kept")

	// Same instant expressed in a non-UTC zone must render identically.
	assert.Equal(t, "2026-06-10T12:34:56Z", formatCreatedAt(utc.In(kyiv)),
		"a non-UTC location must not leak into the contract field")
}

// TestFormatCreatedAtLocal_DSTBoundary pins DST correctness around the
// America/New_York 2026-03-08 spring-forward (02:00 EST → 03:00 EDT): the
// offset must flip -05:00 → -04:00 across the gap.
func TestFormatCreatedAtLocal_DSTBoundary(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	before := time.Date(2026, 3, 8, 6, 59, 0, 0, time.UTC) // 01:59 EST
	assert.Equal(t, "2026-03-08T01:59:00-05:00", formatCreatedAtLocal(before, ny))

	after := time.Date(2026, 3, 8, 7, 30, 0, 0, time.UTC) // 03:30 EDT (02:30 never exists)
	assert.Equal(t, "2026-03-08T03:30:00-04:00", formatCreatedAtLocal(after, ny))
}

// TestParseTimezone is the accept/reject table for the X-Timezone header.
func TestParseTimezone(t *testing.T) {
	str := func(s string) *string { return &s }

	t.Run("absent header → nil location, no error", func(t *testing.T) {
		loc, err := parseTimezone(nil)
		require.NoError(t, err)
		assert.Nil(t, loc)

		loc, err = parseTimezone(str(""))
		require.NoError(t, err)
		assert.Nil(t, loc)
	})

	t.Run("valid IANA names accepted", func(t *testing.T) {
		for _, name := range []string{"Europe/Kyiv", "America/New_York", "Asia/Tokyo", "UTC"} {
			loc, err := parseTimezone(str(name))
			require.NoError(t, err, name)
			require.NotNil(t, loc, name)
			assert.Equal(t, name, loc.String())
		}
	})

	t.Run("offsets, garbage and Local rejected with GATEWAY_INVALID_TIMEZONE", func(t *testing.T) {
		for _, name := range []string{"UTC+3", "+02:00", "GMT-5", "Mars/Olympus", "not a zone", "Local"} {
			loc, err := parseTimezone(str(name))
			require.Error(t, err, name)
			assert.Nil(t, loc, name)
			assert.Equal(t, apperrs.CodeInvalidTimezone, apperr.Code(err), name)
			var ae *apperr.Error
			require.ErrorAs(t, err, &ae)
			assert.Equal(t, 400, ae.Status)
			assert.Equal(t, name, ae.Params["timezone"], "offending value must be a param")
		}
	})
}

// TestGetOrder_TimezoneHandling drives the handler with a stub view:
// created_at is always UTC Z; created_at_local appears only with a valid
// X-Timezone; an invalid one is a coded 400 rejected BEFORE the read.
func TestGetOrder_TimezoneHandling(t *testing.T) {
	created := time.Date(2026, 3, 8, 7, 30, 0, 0, time.UTC)
	reads := 0
	get := func(_ context.Context, q app.GetOrder) (app.OrderView, error) {
		reads++
		return app.OrderView{OrderID: q.OrderID, Status: "created", AmountCents: 100, Currency: "USD", CreatedAt: created}, nil
	}
	s := newTestServer(get, nil, true)
	str := func(v string) *string { return &v }

	t.Run("absent header → UTC only", func(t *testing.T) {
		resp, err := s.GetOrder(context.Background(), GetOrderRequestObject{Id: "o-1"})
		require.NoError(t, err)
		v, ok := resp.(GetOrder200JSONResponse)
		require.True(t, ok)
		assert.Equal(t, "2026-03-08T07:30:00Z", v.CreatedAt)
		assert.Nil(t, v.CreatedAtLocal)
	})

	t.Run("valid header → adds DST-correct local rendering", func(t *testing.T) {
		resp, err := s.GetOrder(context.Background(), GetOrderRequestObject{
			Id:     "o-1",
			Params: GetOrderParams{XTimezone: str("America/New_York")},
		})
		require.NoError(t, err)
		v, ok := resp.(GetOrder200JSONResponse)
		require.True(t, ok)
		assert.Equal(t, "2026-03-08T07:30:00Z", v.CreatedAt, "contract field stays UTC")
		require.NotNil(t, v.CreatedAtLocal)
		assert.Equal(t, "2026-03-08T03:30:00-04:00", *v.CreatedAtLocal)
	})

	t.Run("invalid header → 400 before the read", func(t *testing.T) {
		readsBefore := reads
		_, err := s.GetOrder(context.Background(), GetOrderRequestObject{
			Id:     "o-1",
			Params: GetOrderParams{XTimezone: str("UTC+3")},
		})
		require.Error(t, err)
		assert.Equal(t, apperrs.CodeInvalidTimezone, apperr.Code(err))
		assert.Equal(t, readsBefore, reads, "handler must reject before touching the read model")
	})
}

// TestListOrders_TimezoneHandling: list items carry created_at (UTC Z) and,
// with X-Timezone, created_at_local per item.
func TestListOrders_TimezoneHandling(t *testing.T) {
	created := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	list := func(context.Context, app.ListOrders) (app.OrderPage, error) {
		return app.OrderPage{Items: []app.OrderView{
			{OrderID: "o-1", Status: "created", AmountCents: 100, Currency: "USD", CreatedAt: created},
		}}, nil
	}
	s := newTestServer(nil, list, true)
	str := func(v string) *string { return &v }

	resp, err := s.ListOrders(context.Background(), ListOrdersRequestObject{
		Params: ListOrdersParams{XTimezone: str("Europe/Kyiv")},
	})
	require.NoError(t, err)
	page, ok := resp.(ListOrders200JSONResponse)
	require.True(t, ok)
	require.Len(t, page.Items, 1)
	assert.Equal(t, "2026-06-10T09:00:00Z", page.Items[0].CreatedAt)
	require.NotNil(t, page.Items[0].CreatedAtLocal)
	assert.Equal(t, "2026-06-10T12:00:00+03:00", *page.Items[0].CreatedAtLocal, "Kyiv is UTC+3 in June (EEST)")

	// Invalid zone → coded 400.
	_, err = s.ListOrders(context.Background(), ListOrdersRequestObject{
		Params: ListOrdersParams{XTimezone: str("garbage")},
	})
	require.Error(t, err)
	assert.Equal(t, apperrs.CodeInvalidTimezone, apperr.Code(err))
}
