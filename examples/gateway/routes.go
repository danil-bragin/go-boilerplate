package gateway

import (
	"net/http"
	"net/netip"

	"go-boilerplate/examples/gateway/internal/api"
	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/examples/gateway/internal/sse"
	"go-boilerplate/platform/featureflags"
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
// from the strict handler to an RFC7807 400. Binding errors describe the
// client's own input, so the message is safe to echo.
func requestErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()
	log.From(ctx).InfoContext(
		ctx, "gateway: request binding error",
		"error", err,
		"request_id", httpserver.RequestIDFromContext(ctx),
	)
	httpx.WriteProblem(w, httpx.Problem{
		Status: http.StatusBadRequest,
		Title:  "Bad Request",
		Detail: err.Error(),
	})
}

// responseErrorHandler maps handler errors from the strict handler to RFC7807
// responses. Auth errors keep their status (401/403); everything else is an
// internal failure: the real error is LOGGED (with request_id for correlation)
// and the client receives only a generic problem — internal error strings
// (DSNs, hosts, stack details) must never leak through the edge.
func responseErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	if api.WriteAuthError(w, err) {
		return
	}
	ctx := r.Context()
	log.From(ctx).ErrorContext(
		ctx, "gateway: handler error",
		"error", err,
		"request_id", httpserver.RequestIDFromContext(ctx),
	)
	httpx.WriteProblem(w, httpx.Problem{
		Status: http.StatusInternalServerError,
		Title:  "Internal Server Error",
		Detail: "internal error",
	})
}

// applyEdgeSecurity adds CORS and per-IP rate-limit middleware to the mux.
//
// CORS is deny-by-default: with GATEWAY_CORS_ORIGINS unset (the default) no
// cross-origin browser access is allowed. Set an explicit comma-separated
// origin list in production, or "*" for local dev/demo only.
//
// The rate limiter is keyed by real client IP: RemoteAddr unless the request
// arrives via a trusted proxy, in which case X-Forwarded-For is consulted.
func applyEdgeSecurity(cfg Config, mux chi.Router, lim ratelimit.Limiter, trusted []netip.Prefix) {
	mux.Use(httpserver.CORS(httpserver.CORSOptions{
		AllowedOrigins: cfg.CORSOrigins,
		AllowedMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Content-Type", "Authorization", "X-Request-Id"},
	}))
	mux.Use(httpserver.RateLimitPer(lim, httpserver.ClientIPKey(trusted)))
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
) {
	// Wrap handler errors: map authError → 403/401, others → generic RFC7807
	// (real error logged, never echoed — see responseErrorHandler).
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
	}
	if !cfg.AuthDisabled {
		authMiddleware := auth.Middleware(verifier)
		chiOpts.Middlewares = []api.MiddlewareFunc{
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
		}
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
func mountSSERoutes(cfg Config, httpSrv *httpserver.Server, verifier auth.Verifier, streamer *sse.Streamer) {
	var r chi.Router = httpSrv.Mux()
	if !cfg.AuthDisabled {
		sseMiddleware := auth.Middleware(verifier)
		r = r.With(func(next http.Handler) http.Handler {
			return sseMiddleware(next)
		})
	}
	r.Get("/v1/orders/{id}/events", streamer.Stream)

	// End active streams as soon as graceful shutdown begins; otherwise open
	// connections would hold http.Server.Shutdown for the teardown budget.
	httpSrv.OnShutdown(streamer.Shutdown)
}
