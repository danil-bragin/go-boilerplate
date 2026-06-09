package gateway

import (
	"net/http"
	"net/netip"

	"go-boilerplate/examples/gateway/internal/api"
	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/platform/featureflags"
	"go-boilerplate/platform/observability/log"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/blob"
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
	log.From(ctx).InfoContext(ctx, "gateway: request binding error",
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
	log.From(ctx).ErrorContext(ctx, "gateway: handler error",
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

	// Mount routes. When auth is enabled, apply auth middleware to all routes.
	chiOpts := api.ChiServerOptions{
		BaseRouter: mux,
	}
	if !cfg.AuthDisabled {
		authMiddleware := auth.Middleware(verifier)
		chiOpts.Middlewares = []api.MiddlewareFunc{
			func(next http.Handler) http.Handler {
				return authMiddleware(next)
			},
		}
	}
	api.HandlerWithOptions(strictHandler, chiOpts)
}

// mountAttachmentRoutes mounts the attachment upload/download routes behind
// auth middleware. Does nothing when objStore or flags is nil — graceful degradation.
//
// Attachment routes are mounted behind the same auth middleware so that
// upload/download requires a valid token (same RBAC boundary as POST /orders).
func mountAttachmentRoutes(
	cfg Config,
	httpSrv *httpserver.Server,
	verifier auth.Verifier,
	objStore blob.ObjectStore,
	flags *featureflags.Flags,
) {
	if objStore == nil || flags == nil {
		return
	}
	var attachRouter chi.Router
	if !cfg.AuthDisabled {
		attachMiddleware := auth.Middleware(verifier)
		attachRouter = httpSrv.Mux().With(func(next http.Handler) http.Handler {
			return attachMiddleware(next)
		})
	} else {
		attachRouter = httpSrv.Mux()
	}
	attachments.New(objStore, flags.Bool).Mount(attachRouter)
}
