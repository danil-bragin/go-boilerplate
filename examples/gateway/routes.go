package gateway

import (
	"fmt"
	"net/http"
	"net/netip"

	"go-boilerplate/examples/gateway/internal/api"
	"go-boilerplate/examples/gateway/internal/apperrs"
	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/examples/gateway/internal/sse"
	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/featureflags"
	"go-boilerplate/platform/i18n"
	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/blob"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/web/httpserver"
	"go-boilerplate/platform/web/httpx"
	"go-boilerplate/platform/web/ratelimit"

	"github.com/go-chi/chi/v5"
)

// requestErrorHandler maps request-binding errors (malformed JSON, bad params)
// from the strict handler to a coded RFC 9457 400 (GATEWAY_MALFORMED_REQUEST).
// Binding errors describe the client's own input, so the message is safe to
// echo — it travels in params.reason (AIP-193: every message variable is a
// param) and renders into the detail via the registered template.
func requestErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()
	log.From(ctx).InfoContext(
		ctx, "gateway: request binding error",
		"error", err,
		"request_id", httpserver.RequestIDFromContext(ctx),
	)
	httpx.WriteError(w, r, apperr.Wrap(err, apperrs.CodeMalformedRequest).
		WithParam("reason", err.Error()))
}

// responseErrorHandler maps handler errors from the strict handler to RFC 9457
// problem+json via httpx.FromError: coded apperr errors (GATEWAY_*, AUTH_*,
// VALIDATION_FAILED, …) keep their registered status/code/params; anything
// else is an internal failure — the real error is LOGGED (with request_id for
// correlation) and the client receives only a generic 500 INTERNAL problem.
// Internal error strings (DSNs, hosts, stack details) must never leak through
// the edge; httpx.FromError guarantees that for unknown errors.
func responseErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()
	logger := log.From(ctx)
	if p := httpx.FromError(err); p.Status >= http.StatusInternalServerError {
		logger.ErrorContext(
			ctx, "gateway: handler error",
			"error", err,
			"request_id", httpserver.RequestIDFromContext(ctx),
		)
	} else {
		logger.InfoContext(
			ctx, "gateway: request rejected",
			"error", err,
			"code", p.Code,
			"request_id", httpserver.RequestIDFromContext(ctx),
		)
	}
	httpx.WriteError(w, r, err)
}

// newI18nBundle builds the gateway's localization bundle: the platform
// defaults (en+ru for INTERNAL, VALIDATION_FAILED, AUTH_*, validation.<rule>)
// merged with the gateway's own embedded catalogs for the GATEWAY_* codes.
func newI18nBundle() (*i18n.Bundle, error) {
	b, err := i18n.Default()
	if err != nil {
		return nil, fmt.Errorf("gateway: building platform i18n bundle: %w", err)
	}
	if err := b.Load(apperrs.Catalog, apperrs.CatalogPaths...); err != nil {
		return nil, fmt.Errorf("gateway: loading gateway i18n catalogs: %w", err)
	}
	return b, nil
}

// mountI18n installs the locale-negotiation middleware on the mux. Must run
// BEFORE any route is mounted (chi middleware rule) so that every problem
// response — strict API handlers, SSE, attachments — gets localized
// title/detail via the httpx.ProblemLocalizer seam. Code and params are
// never localized: they are the machine-readable contract.
func mountI18n(mux chi.Router, bundle *i18n.Bundle) {
	mux.Use(i18n.Middleware(bundle))
}

// applyEdgeSecurity adds CORS and per-IP rate-limit middleware to the mux.
//
// CORS is deny-by-default: with GATEWAY_CORS_ORIGINS unset (the default) no
// cross-origin browser access is allowed. Set an explicit comma-separated
// origin list in production, or "*" for local dev/demo only.
//
// The rate limiter is keyed by real client IP: RemoteAddr unless the request
// arrives via a trusted proxy, in which case X-Forwarded-For is consulted.
// (The authed-tier per-principal limiter is NOT applied here — it needs the
// principal, so each route group chains it after its auth middleware; see
// newAuthedRateLimit.)
func applyEdgeSecurity(cfg Config, mux chi.Router, lim ratelimit.Limiter, trusted []netip.Prefix) {
	mux.Use(httpserver.CORS(httpserver.CORSOptions{
		AllowedOrigins: cfg.CORSOrigins,
		AllowedMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Content-Type", "Authorization", "X-Request-Id"},
	}))
	mux.Use(httpserver.RateLimitPer(lim, httpserver.ClientIPKey(trusted)))
}

// newAuthedRateLimit builds the authed-tier rate-limit middleware: a SECOND
// limiter, chained after the per-IP one (both must pass), keyed per principal
// (token subject) so one identity fanning out over many source IPs is still
// capped, while two principals behind one NAT IP get independent buckets.
// Anonymous requests fall back to the client-IP key.
//
// Returns nil when lim is nil (RATELIMIT_AUTHED_RPS=0 disables the tier).
// The middleware must run AFTER the auth middleware of its route group —
// before it, no principal is in the context and every request would take the
// IP fallback.
func newAuthedRateLimit(lim ratelimit.Limiter, trusted []netip.Prefix) func(http.Handler) http.Handler {
	if lim == nil {
		return nil
	}
	return httpserver.RateLimitPer(lim, httpserver.PrincipalKey(httpserver.ClientIPKey(trusted)))
}

// mountAPIRoutes wires the strict handler with optional auth middleware.
//
// The gateway server is built WithoutTimeout/WithoutMaxBytes (the SSE route
// must stream past both — see mountSSERoutes), so the JSON API group
// re-applies the standard per-request Timeout and body cap here with the
// same configured values.
func mountAPIRoutes(
	cfg Config,
	mux chi.Router,
	apiServer api.StrictServerInterface,
	verifier auth.Verifier,
	authedLimit func(http.Handler) http.Handler,
) {
	// Wrap handler errors: coded apperr errors keep their registered
	// status/code, others → generic 500 INTERNAL (real error logged, never
	// echoed — see responseErrorHandler).
	strictOpts := api.StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  requestErrorHandler,
		ResponseErrorHandlerFunc: responseErrorHandler,
	}
	strictHandler := api.NewStrictHandlerWithOptions(apiServer, nil, strictOpts)

	// Mount routes. When auth is enabled, apply auth middleware to all routes
	// EXCEPT /healthz: load-balancer health probes carry no credentials, and a
	// 401 on the health route makes every probe fail (half-dead pod).
	chiOpts := api.ChiServerOptions{
		BaseRouter: mux.With(
			httpserver.MaxBytes(cfg.HTTP.MaxBodyBytes),
			httpserver.Timeout(cfg.HTTP.HandlerTimeout),
		),
		// Param-binding errors (missing required query param, bad date-time,
		// non-integer limit) are raised by the generated wrapper BEFORE the
		// strict handler — without this they'd be plain-text http.Error 400s
		// instead of the coded GATEWAY_MALFORMED_REQUEST problem shape.
		ErrorHandlerFunc: requestErrorHandler,
	}
	// Middleware slice semantics (generated code): handlers are wrapped in
	// slice order, so the LAST element is the OUTERMOST — list the authed-tier
	// limiter FIRST so it runs INSIDE (after) auth and sees the principal.
	if authedLimit != nil {
		chiOpts.Middlewares = append(chiOpts.Middlewares, api.MiddlewareFunc(authedLimit))
	}
	if !cfg.AuthDisabled {
		authMiddleware := auth.Middleware(verifier)
		chiOpts.Middlewares = append(chiOpts.Middlewares, api.MiddlewareFunc(
			func(next http.Handler) http.Handler {
				authed := authMiddleware(next)
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/healthz" {
						next.ServeHTTP(w, r)
						return
					}
					authed.ServeHTTP(w, r)
				})
			},
		))
	}
	api.HandlerWithOptions(strictHandler, chiOpts)
}

// mountAttachmentRoutes mounts the attachment upload/download routes behind
// auth middleware. Does nothing when objStore or flags is nil — graceful degradation.
//
// Attachment routes are mounted behind the same auth middleware so that
// upload/download requires a valid token (same RBAC boundary as POST /v1/orders).
// Like the JSON API group they re-apply the standard Timeout and body cap
// (the server itself is built WithoutTimeout/WithoutMaxBytes for SSE).
func mountAttachmentRoutes(
	cfg Config,
	httpSrv *httpserver.Server,
	verifier auth.Verifier,
	objStore blob.ObjectStore,
	flags *featureflags.Flags,
	pool *pg.Pool,
	authedLimit func(http.Handler) http.Handler,
) {
	if objStore == nil || flags == nil {
		return
	}
	attachRouter := httpSrv.Mux().With(
		httpserver.MaxBytes(cfg.HTTP.MaxBodyBytes),
		httpserver.Timeout(cfg.HTTP.HandlerTimeout),
	)
	if !cfg.AuthDisabled {
		attachMiddleware := auth.Middleware(verifier)
		attachRouter = attachRouter.With(func(next http.Handler) http.Handler {
			return attachMiddleware(next)
		})
	}
	// Authed-tier limiter AFTER auth (chi runs With-middlewares in the order
	// added) so the principal key is available.
	if authedLimit != nil {
		attachRouter = attachRouter.With(authedLimit)
	}
	// Ownership: only the order's customer (or an admin) may touch its
	// attachments. Backed by the gateway read model (orders_read.customer_id).
	opts := []attachments.Option{}
	if pool != nil {
		opts = append(opts, attachments.WithOwnerLookup(attachments.StoreOwnerLookup(pool)))
	}
	attachments.New(objStore, flags.Bool, opts...).Mount(attachRouter)
}

// mountSSERoutes mounts GET /v1/orders/{id}/events behind the same auth
// middleware as the rest of the API.
//
// DELIBERATELY exempt from http.TimeoutHandler and MaxBytes (the server is
// built WithoutTimeout/WithoutMaxBytes; this group re-applies neither):
// TimeoutHandler buffers the whole response and cuts it at the deadline —
// fatal for an endless stream — and the body cap is moot on a GET whose body
// is never read. The streamer's own heartbeat + client-close detection bound
// the connection instead.
func mountSSERoutes(
	cfg Config,
	httpSrv *httpserver.Server,
	verifier auth.Verifier,
	streamer *sse.Streamer,
	authedLimit func(http.Handler) http.Handler,
) {
	var r chi.Router = httpSrv.Mux()
	if !cfg.AuthDisabled {
		sseMiddleware := auth.Middleware(verifier)
		r = r.With(func(next http.Handler) http.Handler {
			return sseMiddleware(next)
		})
	}
	// Authed-tier limiter AFTER auth: one token per stream OPEN (reconnect
	// storms are bounded per principal); the stream itself is long-lived and
	// consumes nothing further.
	if authedLimit != nil {
		r = r.With(authedLimit)
	}
	r.Get("/v1/orders/{id}/events", streamer.Stream)

	// End active streams as soon as graceful shutdown begins; otherwise open
	// connections would hold http.Server.Shutdown for the teardown budget.
	httpSrv.OnShutdown(streamer.Shutdown)
}
