package httpserver

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// routeTagMeterName is the otel instrumentation scope for per-route HTTP
// server metrics.
const routeTagMeterName = "web.httpserver"

// unmatchedRoute is the bounded-cardinality tag used when no chi route
// matched (404s) or when the pattern is unknown (request abandoned by
// http.TimeoutHandler before the handler finished).
const unmatchedRoute = "unmatched"

// routeHolder carries the matched route pattern from RouteTag (which runs on
// the request-serving goroutine, where reading the chi RouteContext is safe)
// to outer middlewares such as AccessLog.
//
// Why not read chi.RouteContext directly in outer middlewares? Because
// http.TimeoutHandler abandons the inner goroutine on timeout: the abandoned
// goroutine keeps using the chi RouteContext (nested routers mutate it) while
// the outer middleware would read it — a data race. The holder is
// mutex-guarded, so the hand-off is safe even for abandoned requests (which
// simply report "unmatched").
type routeHolder struct {
	mu      sync.Mutex
	pattern string
}

func (h *routeHolder) set(p string) {
	h.mu.Lock()
	h.pattern = p
	h.mu.Unlock()
}

func (h *routeHolder) get() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pattern == "" {
		return unmatchedRoute
	}
	return h.pattern
}

type routeHolderKey struct{}

// withRouteHolder installs a routeHolder into ctx (done by AccessLog).
func withRouteHolder(ctx context.Context) (context.Context, *routeHolder) {
	h := &routeHolder{}
	return context.WithValue(ctx, routeHolderKey{}, h), h
}

// routeHolderFrom returns the holder installed by an outer middleware, or nil.
func routeHolderFrom(ctx context.Context) *routeHolder {
	h, _ := ctx.Value(routeHolderKey{}).(*routeHolder)
	return h
}

// RouteTag returns a post-routing middleware that tags telemetry with the
// matched chi route PATTERN (e.g. "/v1/orders/{id}") instead of the raw URL
// path. Per request it:
//
//   - renames the active server span to "METHOD pattern" (low-cardinality
//     span names — one series per route, not per URL);
//   - sets the http.route span attribute;
//   - records the http.server.duration histogram (milliseconds) with
//     {http.request.method, http.route, http.response.status_code}
//     attributes — the per-route RED metrics;
//   - publishes the pattern to outer middlewares (AccessLog's "route" field)
//     via a mutex-guarded holder.
//
// PLACEMENT: RouteTag must be the INNERMOST middleware on the chi router —
// in particular AFTER (inside) Timeout. chi fills the RouteContext during
// routing, so the pattern is read after next returns; doing that read on the
// request-serving goroutine is what keeps it race-free when
// http.TimeoutHandler abandons a slow request. httpserver.New wires the stack
// as: SecurityHeaders → RequestID → OTel → AccessLog → Recover → MaxBytes →
// Timeout → RouteTag.
//
// Construct RouteTag AFTER telemetry setup (telemetry.Setup installs the
// global meter provider the histogram is created from). Requests that match
// no route are tagged with route="unmatched" to keep cardinality bounded;
// requests cut off by TimeoutHandler also report "unmatched" in the
// access log (the 503 is written by TimeoutHandler on the outer goroutine).
func RouteTag() func(http.Handler) http.Handler {
	m := otel.Meter(routeTagMeterName)
	var duration metric.Float64Histogram
	if h, err := m.Float64Histogram(
		"http.server.duration",
		metric.WithDescription("HTTP server request duration by route pattern"),
		metric.WithUnit("ms"),
	); err == nil {
		duration = h
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			cw := &capturingWriter{ResponseWriter: w}
			next.ServeHTTP(cw, r)

			ctx := r.Context()
			route := unmatchedRoute
			// Safe: we are on the same goroutine that performed the routing.
			if rctx := chi.RouteContext(ctx); rctx != nil {
				if p := rctx.RoutePattern(); p != "" {
					route = p
				}
			}

			// Publish to outer middlewares (AccessLog) through the holder.
			if h := routeHolderFrom(ctx); h != nil {
				h.set(route)
			}

			// Rename the server span started by the OTel middleware and
			// attach the route attribute. SetName on an already-ended span
			// (timed-out request) is a safe no-op.
			span := trace.SpanFromContext(ctx)
			span.SetName(r.Method + " " + route)
			span.SetAttributes(semconv.HTTPRoute(route))

			if duration != nil {
				// When http.TimeoutHandler cut the request off, the status the
				// handler wrote went to an abandoned writer — the CLIENT got
				// the TimeoutHandler's 503. Record what the client saw, not
				// the handler's intent.
				status := cw.Status()
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					status = http.StatusServiceUnavailable
				}
				duration.Record(
					ctx,
					float64(time.Since(start))/float64(time.Millisecond),
					metric.WithAttributes(
						attribute.String("http.request.method", r.Method),
						attribute.String("http.route", route),
						attribute.Int("http.response.status_code", status),
					),
				)
			}
		})
	}
}
