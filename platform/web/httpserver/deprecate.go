package httpserver

import (
	"net/http"
	"strconv"
	"time"
)

// Deprecate marks every response of the wrapped handler (typically a
// version-prefixed route group) as deprecated with a fixed removal date:
//
//   - Deprecation: @<unix>          (RFC 9745, structured-field Date)
//   - Sunset: <IMF-fixdate>         (RFC 8594)
//   - Link: <successor>; rel="successor-version"  (omitted when successor=="")
//
// Both date headers carry the SAME instant — sunset, the date the endpoint
// stops working. RFC 9745 allows the deprecation date to differ from (precede)
// the sunset date; this middleware deliberately keeps one knob because the
// only date that is a real operational commitment is removal. If you need to
// announce deprecation long before a removal date exists, mount Deprecate
// with the best-known sunset and move it LATER only (clients treat Sunset as
// a promise: it may slip further away, never closer).
//
// The middleware is purely advisory — the handler still runs and the response
// is otherwise untouched. Mount it on the OLD version's route group while the
// successor is served in parallel:
//
//	mux.Route("/v1", func(r chi.Router) {
//	    r.Use(httpserver.Deprecate(sunset, "/v2/orders"))
//	    r.Get("/orders", v1handler)
//	})
//	mux.Route("/v2", func(r chi.Router) { r.Get("/orders", v2handler) })
//
// Link is ADDed (not Set) so handler-emitted Link relations survive.
// See docs/operations.md §API evolution for the full playbook (including the
// proto vN analog for Kafka contracts).
func Deprecate(sunset time.Time, successor string) func(http.Handler) http.Handler {
	deprecation := "@" + strconv.FormatInt(sunset.Unix(), 10)
	sunsetHTTP := sunset.UTC().Format(http.TimeFormat)
	link := ""
	if successor != "" {
		link = "<" + successor + `>; rel="successor-version"`
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := w.Header()
			h.Set("Deprecation", deprecation)
			h.Set("Sunset", sunsetHTTP)
			if link != "" {
				h.Add("Link", link)
			}
			next.ServeHTTP(w, r)
		})
	}
}
