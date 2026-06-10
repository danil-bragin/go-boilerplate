package i18n_test

import (
	"context"
	"embed"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/i18n"
	"go-boilerplate/platform/web/httpx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

//go:embed testdata/*.toml
var testCatalog embed.FS

func testBundle(t *testing.T) *i18n.Bundle {
	t.Helper()
	b, err := i18n.New(testCatalog, "testdata/en.toml", "testdata/ru.toml")
	require.NoError(t, err)
	return b
}

// localeCtx builds a ctx as the middleware would for the given Accept-Language.
func localeCtx(t *testing.T, b *i18n.Bundle, acceptLanguage string) context.Context {
	t.Helper()
	var got context.Context
	h := i18n.Middleware(b)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Context()
	}))
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	if acceptLanguage != "" {
		req.Header.Set("Accept-Language", acceptLanguage)
	}
	h.ServeHTTP(httptest.NewRecorder(), req)
	require.NotNil(t, got)
	return got
}

func TestMiddleware_Negotiation(t *testing.T) {
	b := testBundle(t)
	tests := []struct {
		name           string
		acceptLanguage string
		wantLocale     language.Tag
		wantGreeting   string
	}{
		{"explicit ru", "ru", language.Russian, "привет"},
		{"explicit en", "en", language.English, "hello"},
		{"unsupported falls back to en", "de", language.English, "hello"},
		{"q-weights pick ru", "en;q=0.5, ru;q=0.9", language.Russian, "привет"},
		{"region variant matches base", "ru-RU", language.Russian, "привет"},
		{"missing header defaults to en", "", language.English, "hello"},
		{"garbage header defaults to en", ";;;", language.English, "hello"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := localeCtx(t, b, tt.acceptLanguage)
			assert.Equal(t, tt.wantLocale, i18n.Locale(ctx))
			assert.Equal(t, tt.wantGreeting, i18n.T(ctx, "test.greeting", nil))
		})
	}
}

func TestT_TemplateParams(t *testing.T) {
	b := testBundle(t)
	ctx := localeCtx(t, b, "en")
	got := i18n.T(ctx, "test.named", map[string]any{"name": "Ada"})
	assert.Equal(t, "hello Ada", got)
}

func TestT_MissingKeyReturnsEmpty(t *testing.T) {
	b := testBundle(t)
	ctx := localeCtx(t, b, "en")
	assert.Empty(t, i18n.T(ctx, "test.nope", nil), "missing key → empty (caller falls back to dev msg)")
}

func TestT_NoLocalizerInCtxReturnsEmpty(t *testing.T) {
	assert.Empty(t, i18n.T(context.Background(), "test.greeting", nil))
}

func TestT_Plurals(t *testing.T) {
	b := testBundle(t)

	en := localeCtx(t, b, "en")
	assert.Equal(t, "1 item is invalid", i18n.T(en, "test.items", map[string]any{"count": 1}))
	assert.Equal(t, "3 items are invalid", i18n.T(en, "test.items", map[string]any{"count": 3}))

	ru := localeCtx(t, b, "ru")
	assert.Equal(t, "1 поле неверно", i18n.T(ru, "test.items", map[string]any{"count": 1}))
	assert.Equal(t, "3 поля неверны", i18n.T(ru, "test.items", map[string]any{"count": 3}))
}

// TestMiddleware_LocalizesProblems is the end-to-end seam check: the
// middleware installs an httpx.ProblemLocalizer, so httpx.WriteError emits
// localized title/detail while code/params stay the stable contract.
func TestMiddleware_LocalizesProblems(t *testing.T) {
	b, err := i18n.Default()
	require.NoError(t, err)

	h := i18n.Middleware(b)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, apperr.New(apperr.CodeAuthForbidden))
	}))

	for _, tt := range []struct {
		lang       string
		wantDetail string
	}{
		{"en", "You do not have permission to perform this action."},
		{"ru", "У вас нет прав для выполнения этого действия."},
	} {
		t.Run(tt.lang, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/x", http.NoBody)
			req.Header.Set("Accept-Language", tt.lang)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, http.StatusForbidden, rec.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "AUTH_FORBIDDEN", body["code"], "code is locale-independent")
			assert.Equal(t, tt.wantDetail, body["detail"])
		})
	}
}

// TestDefault_CoversPlatformCodes: the shipped en catalog must localize
// every code the platform itself registers, plus the common validation rules.
func TestDefault_CoversPlatformCodes(t *testing.T) {
	b, err := i18n.Default()
	require.NoError(t, err)
	ctx := localeCtx(t, b, "en")

	for _, code := range []string{
		apperr.CodeInternal,
		apperr.CodeValidationFailed,
		apperr.CodeAuthUnauthenticated,
		apperr.CodeAuthForbidden,
	} {
		assert.NotEmpty(t, i18n.T(ctx, code, nil), "missing en message for %s", code)
	}
	for _, rule := range []string{"required", "min", "max", "len", "email", "uuid", "oneof"} {
		assert.NotEmpty(t, i18n.T(ctx, "validation."+rule, map[string]any{"field": "f", "param": "1"}),
			"missing en message for validation.%s", rule)
	}
}
