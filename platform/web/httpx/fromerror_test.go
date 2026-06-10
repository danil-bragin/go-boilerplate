package httpx_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/authz"
	"go-boilerplate/platform/web/httpx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	apperr.Register("TEST_HTTPX_NOT_FOUND", 404, true, "order {order_id} not found", "order_id")
}

func TestFromError_AppErr(t *testing.T) {
	err := apperr.New("TEST_HTTPX_NOT_FOUND").WithParam("order_id", "42")

	p := httpx.FromError(err)

	assert.Equal(t, http.StatusNotFound, p.Status)
	assert.Equal(t, "TEST_HTTPX_NOT_FOUND", p.Code)
	assert.Equal(t, "order 42 not found", p.Detail)
	assert.Equal(t, map[string]any{"order_id": "42"}, p.Params)
}

func TestFromError_WrappedAppErr(t *testing.T) {
	err := fmt.Errorf("handler: %w", apperr.New("TEST_HTTPX_NOT_FOUND"))
	p := httpx.FromError(err)
	assert.Equal(t, "TEST_HTTPX_NOT_FOUND", p.Code)
	assert.Equal(t, http.StatusNotFound, p.Status)
}

func TestFromError_UnknownError_InternalWithoutLeak(t *testing.T) {
	err := errors.New("pq: connection to 10.0.0.5 refused (secret-dsn)")

	p := httpx.FromError(err)

	assert.Equal(t, http.StatusInternalServerError, p.Status)
	assert.Equal(t, apperr.CodeInternal, p.Code)
	assert.NotContains(t, p.Detail, "secret-dsn", "internal detail must not leak")
	assert.NotContains(t, p.Title, "secret-dsn")
}

func TestFromError_AuthSentinels(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantCode   string
		wantStatus int
	}{
		{"authz forbidden", authz.ErrForbidden, apperr.CodeAuthForbidden, http.StatusForbidden},
		{"wrapped authz forbidden", fmt.Errorf("x: %w", authz.ErrForbidden), apperr.CodeAuthForbidden, http.StatusForbidden},
		{"authz unauthenticated", authz.ErrUnauthenticated, apperr.CodeAuthUnauthenticated, http.StatusUnauthorized},
		{"cqrs unauthenticated", cqrs.ErrUnauthenticated, apperr.CodeAuthUnauthenticated, http.StatusUnauthorized},
		{"wrapped cqrs unauthenticated", fmt.Errorf("x: %w", cqrs.ErrUnauthenticated), apperr.CodeAuthUnauthenticated, http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := httpx.FromError(tt.err)
			assert.Equal(t, tt.wantCode, p.Code)
			assert.Equal(t, tt.wantStatus, p.Status)
		})
	}
}

func TestFromError_ValidationError(t *testing.T) {
	verr := &httpx.ValidationError{
		Fields: map[string]string{"amount_cents": "failed on 'min'"},
		Details: []httpx.FieldError{
			{Field: "amount_cents", Rule: "min", Param: "1"},
		},
	}

	p := httpx.FromError(verr)

	assert.Equal(t, http.StatusUnprocessableEntity, p.Status)
	assert.Equal(t, apperr.CodeValidationFailed, p.Code)
	assert.Equal(t, verr.Fields, p.Errors)
	require.Contains(t, p.Params, "fields")
	fields, ok := p.Params["fields"].([]map[string]any)
	require.True(t, ok, "params.fields must be a []map[string]any, got %T", p.Params["fields"])
	require.Len(t, fields, 1)
	assert.Equal(t, map[string]any{"field": "amount_cents", "rule": "min", "param": "1"}, fields[0])
}

// TestProblem_RFC9457MemberNames pins the wire shape: the standard members
// use the exact RFC 9457 names, extension members are lowercase and do not
// collide with reserved names, and empty optional members are omitted.
func TestProblem_RFC9457MemberNames(t *testing.T) {
	full := httpx.Problem{
		Type:     "https://example.com/probs/out-of-credit",
		Title:    "Out of credit",
		Status:   403,
		Detail:   "Your balance is 30",
		Instance: "/account/12345/msgs/abc",
		Code:     "TEST_CODE",
		Params:   map[string]any{"balance": 30},
		Errors:   map[string]string{"f": "bad"},
	}
	b, err := json.Marshal(full)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	wantKeys := []string{"type", "title", "status", "detail", "instance", "code", "params", "errors"}
	assert.ElementsMatch(t, wantKeys, keys(m))

	// Optional members are omitted when empty (status/title always present
	// in practice via WriteProblem defaults).
	b, err = json.Marshal(httpx.Problem{Status: 500, Title: "Internal Server Error"})
	require.NoError(t, err)
	var minimal map[string]any
	require.NoError(t, json.Unmarshal(b, &minimal))
	assert.ElementsMatch(t, []string{"status", "title"}, keys(minimal))
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestWriteError_SetsInstanceFromRequestPath(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/orders/42", http.NoBody)

	httpx.WriteError(rec, req, apperr.New("TEST_HTTPX_NOT_FOUND").WithParam("order_id", "42"))

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "/orders/42", body["instance"])
	assert.Equal(t, "TEST_HTTPX_NOT_FOUND", body["code"])
}

// Decode must populate ValidationError.Details with field/rule/param so the
// edge can emit structured validation params.
func TestDecode_ValidationErrorDetails(t *testing.T) {
	type payload struct {
		Amount int    `json:"amount_cents" validate:"min=1"`
		Name   string `json:"name"         validate:"required"`
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"amount_cents":0,"name":""}`))
	req.Header.Set("Content-Type", "application/json")

	_, err := httpx.Decode[payload](req)

	var ve *httpx.ValidationError
	require.ErrorAs(t, err, &ve)
	require.Len(t, ve.Details, 2)
	byField := map[string]httpx.FieldError{}
	for _, d := range ve.Details {
		byField[d.Field] = d
	}
	assert.Equal(t, httpx.FieldError{Field: "amount_cents", Rule: "min", Param: "1"}, byField["amount_cents"])
	assert.Equal(t, httpx.FieldError{Field: "name", Rule: "required", Param: ""}, byField["name"])
}

// TestWriteError_UsesProblemLocalizer pins the localization seam owned by
// httpx: a ProblemLocalizer installed in the request ctx overrides
// title/detail while code/params stay untouched. ok=false keeps defaults.
func TestWriteError_UsesProblemLocalizer(t *testing.T) {
	loc := httpx.ProblemLocalizer(func(code string, params map[string]any) (string, string, bool) {
		if code != "TEST_HTTPX_NOT_FOUND" {
			return "", "", false
		}
		return "Не найдено", "заказ " + params["order_id"].(string) + " не найден", true
	})

	req := httptest.NewRequest(http.MethodGet, "/orders/42", http.NoBody)
	req = req.WithContext(httpx.WithProblemLocalizer(req.Context(), loc))
	rec := httptest.NewRecorder()

	httpx.WriteError(rec, req, apperr.New("TEST_HTTPX_NOT_FOUND").WithParam("order_id", "42"))

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "Не найдено", body["title"])
	assert.Equal(t, "заказ 42 не найден", body["detail"])
	assert.Equal(t, "TEST_HTTPX_NOT_FOUND", body["code"], "code stays locale-independent")

	// Localizer that reports no translation → developer detail kept.
	miss := httpx.ProblemLocalizer(func(string, map[string]any) (string, string, bool) { return "", "", false })
	req2 := httptest.NewRequest(http.MethodGet, "/orders/42", http.NoBody)
	req2 = req2.WithContext(httpx.WithProblemLocalizer(req2.Context(), miss))
	rec2 := httptest.NewRecorder()
	httpx.WriteError(rec2, req2, apperr.New("TEST_HTTPX_NOT_FOUND").WithParam("order_id", "42"))
	var body2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &body2))
	assert.Equal(t, "order 42 not found", body2["detail"])
}
