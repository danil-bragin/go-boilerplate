package httpserver_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-boilerplate/platform/web/httpserver"

	"github.com/stretchr/testify/require"
)

// TestDeprecate_EmitsExactHeaders pins the wire format of the three
// deprecation headers: RFC 9745 Deprecation (structured-field Date — "@" +
// unix seconds), RFC 8594 Sunset (IMF-fixdate) and the successor-version
// Link relation.
func TestDeprecate_EmitsExactHeaders(t *testing.T) {
	sunset := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	h := httpserver.Deprecate(sunset, "/v2/orders")(okHandler)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/orders", http.NoBody))

	require.Equal(t, http.StatusOK, rec.Code, "deprecation is advisory — the handler must still run")
	require.Equal(t, "@1798761600", rec.Header().Get("Deprecation"),
		"RFC 9745: structured-field Date = '@' + unix seconds of the sunset instant")
	require.Equal(t, "Fri, 01 Jan 2027 00:00:00 GMT", rec.Header().Get("Sunset"),
		"RFC 8594: IMF-fixdate (http.TimeFormat) in GMT")
	require.Equal(t, `</v2/orders>; rel="successor-version"`, rec.Header().Get("Link"))
}

// TestDeprecate_NonUTCSunsetNormalized: the Sunset header must be GMT no
// matter what zone the caller's time.Time carries; Deprecation is
// zone-independent by construction (unix seconds).
func TestDeprecate_NonUTCSunsetNormalized(t *testing.T) {
	kyiv, err := time.LoadLocation("Europe/Kyiv")
	require.NoError(t, err)
	// 02:00 Kyiv (UTC+2 in winter) == 00:00 UTC.
	sunset := time.Date(2027, 1, 1, 2, 0, 0, 0, kyiv)
	h := httpserver.Deprecate(sunset, "")(okHandler)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/orders", http.NoBody))

	require.Equal(t, "@1798761600", rec.Header().Get("Deprecation"))
	require.Equal(t, "Fri, 01 Jan 2027 00:00:00 GMT", rec.Header().Get("Sunset"))
}

// TestDeprecate_NoSuccessorOmitsLink: an empty successor emits no Link header
// rather than a malformed "<>" reference.
func TestDeprecate_NoSuccessorOmitsLink(t *testing.T) {
	h := httpserver.Deprecate(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), "")(okHandler)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/orders", http.NoBody))

	_, present := rec.Result().Header["Link"]
	require.False(t, present, "no successor → no Link header")
}

// TestDeprecate_PreservesExistingLinkHeaders: the middleware ADDS its Link
// relation, never clobbering relations set by inner handlers.
func TestDeprecate_PreservesExistingLinkHeaders(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Link", `</docs>; rel="help"`)
		w.WriteHeader(http.StatusOK)
	})
	h := httpserver.Deprecate(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), "/v2/orders")(inner)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/orders", http.NoBody))

	links := rec.Result().Header.Values("Link")
	require.Contains(t, links, `</v2/orders>; rel="successor-version"`)
	require.Contains(t, links, `</docs>; rel="help"`)
}
