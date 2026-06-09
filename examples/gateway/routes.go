package gateway

import (
	"net/http"
	"net/netip"

	"go-boilerplate/examples/gateway/internal/api"
	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/platform/featureflags"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/blob"
	"go-boilerplate/platform/web/httpserver"
	"go-boilerplate/platform/web/ratelimit"

	"github.com/go-chi/chi/v5"
)

// applyEdgeSecurity adds CORS and per-IP rate-limit middleware to the mux.
//
// CORS is applied only when cfg.CORSOrigins is set. For demo/local the
// default allows all origins. In production set GATEWAY_CORS_ORIGINS to
// an explicit comma-separated list and remove the "*" wildcard.
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
	// Wrap handler errors: map authError → 403/401, others → 500.
	strictOpts := api.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			if api.WriteAuthError(w, err) {
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
		},
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
// upload/download requires a valid token (same RBAC boundary as POST /v1/orders).
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
