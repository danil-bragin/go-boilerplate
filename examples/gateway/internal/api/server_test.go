package api

import (
	"context"
	"log/slog"
	"testing"

	"go-boilerplate/examples/gateway/internal/app"
	"go-boilerplate/examples/gateway/internal/apperrs"
	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/auth"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestServer builds a Server with stub query handlers and no pool/producer
// — only code paths that return BEFORE any I/O are exercised here.
func newTestServer(
	get cqrs.HandlerFunc[app.GetOrder, app.OrderView],
	list cqrs.HandlerFunc[app.ListOrders, app.OrderPage],
	authDisabled bool,
) *Server {
	return NewServer(nil, nil, "test-topic", slog.Default(), get, list, authDisabled)
}

// TestCreateOrder_NoPrincipal_Unauthenticated: with auth enabled and no
// principal in ctx, CreateOrder fails closed with the platform
// AUTH_UNAUTHENTICATED code (401) instead of the old untyped authError.
func TestCreateOrder_NoPrincipal_Unauthenticated(t *testing.T) {
	s := newTestServer(nil, nil, false)

	_, err := s.CreateOrder(context.Background(), CreateOrderRequestObject{
		Body: &CreateOrderRequest{CustomerId: "c1", AmountCents: 100, Currency: "USD"},
	})

	require.Error(t, err)
	assert.Equal(t, apperr.CodeAuthUnauthenticated, apperr.Code(err))
}

// TestCreateOrder_PrincipalWithoutRole_Forbidden: a principal lacking the
// "user"/"admin" role yields the platform AUTH_FORBIDDEN code (403).
func TestCreateOrder_PrincipalWithoutRole_Forbidden(t *testing.T) {
	s := newTestServer(nil, nil, false)
	ctx := auth.Into(context.Background(), auth.Principal{Subject: "mallory", Roles: []string{"viewer"}})

	_, err := s.CreateOrder(ctx, CreateOrderRequestObject{
		Body: &CreateOrderRequest{CustomerId: "c1", AmountCents: 100, Currency: "USD"},
	})

	require.Error(t, err)
	assert.Equal(t, apperr.CodeAuthForbidden, apperr.Code(err))
}

// TestGetOrder_NotFound_CodedWithParam: a missing order surfaces as the
// GATEWAY_ORDER_NOT_FOUND apperr carrying the order id as a param — the
// edge (responseErrorHandler → httpx.FromError) turns it into the 404
// problem+json; no inline Problem construction remains in the handler.
func TestGetOrder_NotFound_CodedWithParam(t *testing.T) {
	get := func(_ context.Context, q app.GetOrder) (app.OrderView, error) {
		return app.OrderView{}, app.OrderNotFound(q.OrderID)
	}
	s := newTestServer(get, nil, true)

	_, err := s.GetOrder(context.Background(), GetOrderRequestObject{Id: "o-7"})

	require.Error(t, err)
	assert.Equal(t, apperrs.CodeOrderNotFound, apperr.Code(err))
	var ae *apperr.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, 404, ae.Status)
	assert.Equal(t, "o-7", ae.Params["order_id"])
}

// TestGetOrder_NonOwner_SameNotFoundCode: ownership denial returns the SAME
// coded 404 as a nonexistent order — no existence oracle.
func TestGetOrder_NonOwner_SameNotFoundCode(t *testing.T) {
	get := func(context.Context, app.GetOrder) (app.OrderView, error) {
		return app.OrderView{OrderID: "o-9", CustomerID: "alice", Status: "created"}, nil
	}
	s := newTestServer(get, nil, false)
	ctx := auth.Into(context.Background(), auth.Principal{Subject: "bob", Roles: []string{"user"}})

	_, err := s.GetOrder(ctx, GetOrderRequestObject{Id: "o-9"})

	require.Error(t, err)
	assert.Equal(t, apperrs.CodeOrderNotFound, apperr.Code(err))
}

// TestListOrders_InvalidCursor_FlowsThroughChain: the coded app sentinel
// survives the handler's fmt.Errorf wrap, so the edge still maps it to
// GATEWAY_INVALID_CURSOR / 400.
func TestListOrders_InvalidCursor_FlowsThroughChain(t *testing.T) {
	list := func(context.Context, app.ListOrders) (app.OrderPage, error) {
		return app.OrderPage{}, app.ErrInvalidCursor
	}
	s := newTestServer(nil, list, true)

	bad := "not-a-cursor"
	_, err := s.ListOrders(context.Background(), ListOrdersRequestObject{
		Params: ListOrdersParams{Cursor: &bad},
	})

	require.Error(t, err)
	assert.Equal(t, apperrs.CodeInvalidCursor, apperr.Code(err))
	var ae *apperr.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, 400, ae.Status)
}
