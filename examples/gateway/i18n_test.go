package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-boilerplate/examples/gateway/internal/app"
	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/i18n"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// problemBody is the decoded problem+json shape the i18n tests assert on.
type problemBody struct {
	Title  string         `json:"title"`
	Status int            `json:"status"`
	Detail string         `json:"detail"`
	Code   string         `json:"code"`
	Params map[string]any `json:"params"`
}

// serveLocalizedError runs the gateway's i18n middleware + response error
// handler exactly as wired in NewApp (middleware installs the localizer,
// responseErrorHandler writes via httpx.WriteError) for the given error and
// Accept-Language header.
func serveLocalizedError(t *testing.T, acceptLanguage string, err error) problemBody {
	t.Helper()
	bundle, berr := newI18nBundle()
	require.NoError(t, berr)

	h := i18n.Middleware(bundle)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		responseErrorHandler(w, r, err)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-1", http.NoBody)
	if acceptLanguage != "" {
		req.Header.Set("Accept-Language", acceptLanguage)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var p problemBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	require.Equal(t, p.Status, rec.Code)
	return p
}

// TestI18n_NotFoundLocalized: Accept-Language: ru localizes title and detail
// of the 404 problem; en is the default; the machine-readable code and
// params are IDENTICAL in every locale — clients never parse detail.
func TestI18n_NotFoundLocalized(t *testing.T) {
	notFound := app.OrderNotFound("o-1")

	ru := serveLocalizedError(t, "ru", notFound)
	assert.Equal(t, "Заказ o-1 не найден.", ru.Detail)
	assert.Equal(t, "Заказ не найден", ru.Title)

	en := serveLocalizedError(t, "", notFound) // no header → default en
	assert.Equal(t, "Order o-1 was not found.", en.Detail)
	assert.Equal(t, "Order not found", en.Title)

	enQ := serveLocalizedError(t, "fr;q=0.9, en;q=0.8", notFound) // unsupported fr → en
	assert.Equal(t, en.Detail, enQ.Detail)

	// Contract members are locale-independent.
	for _, p := range []problemBody{ru, en, enQ} {
		assert.Equal(t, http.StatusNotFound, p.Status)
		assert.Equal(t, "GATEWAY_ORDER_NOT_FOUND", p.Code)
		assert.Equal(t, "o-1", p.Params["order_id"])
	}
}

// TestI18n_ValidationLocalized: a validation 400 gets the localized platform
// VALIDATION_FAILED detail and the gateway's title override, while the
// structured params.fields stay untouched in both locales.
func TestI18n_ValidationLocalized(t *testing.T) {
	verr := apperr.New(apperr.CodeValidationFailed).WithParam("fields",
		[]map[string]any{{"field": "amount_cents", "rule": "gt", "param": "0"}})

	ru := serveLocalizedError(t, "ru-RU,ru;q=0.9,en;q=0.5", verr)
	assert.Equal(t, "Одно или несколько полей заполнены неверно.", ru.Detail)
	assert.Equal(t, "Некорректный запрос", ru.Title)

	en := serveLocalizedError(t, "en-US", verr)
	assert.Equal(t, "One or more fields are invalid.", en.Detail)
	assert.Equal(t, "Invalid request", en.Title)

	for _, p := range []problemBody{ru, en} {
		assert.Equal(t, http.StatusBadRequest, p.Status)
		assert.Equal(t, "VALIDATION_FAILED", p.Code)
		fields, ok := p.Params["fields"].([]any)
		require.True(t, ok)
		require.Len(t, fields, 1)
		f := fields[0].(map[string]any)
		assert.Equal(t, "amount_cents", f["field"])
		assert.Equal(t, "gt", f["rule"])
	}
}
