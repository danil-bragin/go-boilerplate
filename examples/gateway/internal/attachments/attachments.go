// Package attachments provides an HTTP handler for order file attachments.
// Objects are stored in an S3-compatible object store (blob.ObjectStore) and
// the upload/download endpoints are gated by an OpenFeature boolean flag
// ("order-attachments-enabled"). When the flag is off both endpoints return 404.
//
// # Authorization
//
// Upload and Download require the principal (set by the auth middleware) to
// hold the requiredRole (default "user") or the admin role. Absent principal
// → 401; missing role → 403.
//
// # Ownership
//
// When an OwnerLookup is configured (WithOwnerLookup), the handler verifies
// that the order identified by {id} belongs to the authenticated principal:
// the principal's Subject must equal the order's customer_id from the read
// model, unless the principal holds the admin role. Non-owners receive 403;
// unknown orders 404; lookup failures 500 (fail closed). Without an
// OwnerLookup only the role gate applies (boilerplate demo mode).
package attachments

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"

	"go-boilerplate/examples/gateway/internal/apperrs"
	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/security/authz"
	"go-boilerplate/platform/storage/blob"
	"go-boilerplate/platform/web/httpx"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

const (
	defaultFlagKey    = "order-attachments-enabled"
	defaultPresignTTL = 5 * time.Minute
	defaultRole       = "user"
	maxFilenameLen    = 128
)

// AdminRole is the role that bypasses ownership checks across the gateway:
// both the attachments handler and the orders read path (api.Server) use
// this same constant, so "admin" means the same thing everywhere.
const AdminRole = "admin"

// ErrOwnerNotFound is returned by an OwnerLookup when the order does not
// exist in the read model. The handler maps it to 404.
var ErrOwnerNotFound = errors.New("attachments: order owner not found")

// OwnerLookup resolves the owner (customer_id / principal subject) of the
// order identified by orderID from the read model. Return ErrOwnerNotFound
// when the order is unknown; any other error is treated as an infrastructure
// failure (500, fail closed).
type OwnerLookup func(ctx context.Context, orderID string) (string, error)

// Option configures a Handler.
type Option func(*Handler)

// WithOwnerLookup enables the ownership check: the authenticated principal
// must own the order (Subject == customer_id) unless it holds the admin role.
func WithOwnerLookup(lookup OwnerLookup) Option {
	return func(h *Handler) { h.ownerLookup = lookup }
}

// ownerPolicy is the resource-aware authz.Policy used for the ownership
// check: resource is the order's owner subject (string). The principal is
// authorized when it IS the owner, or when it holds bypassRole.
type ownerPolicy struct {
	bypassRole string
}

// Authorize implements authz.Policy. resource must be the owner subject.
func (o ownerPolicy) Authorize(_ context.Context, p auth.Principal, action string, resource any) error {
	owner, _ := resource.(string)
	if owner != "" && p.Subject == owner {
		return nil
	}
	if slices.Contains(p.Roles, o.bypassRole) {
		return nil
	}
	return fmt.Errorf("%w: principal is not the owner for %q", authz.ErrForbidden, action)
}

// Handler handles upload and download of order attachments.
type Handler struct {
	store        blob.ObjectStore
	flagBool     func(ctx context.Context, key string, def bool) bool
	flagKey      string
	presignTTL   time.Duration
	requiredRole string
	ownerLookup  OwnerLookup
	ownership    authz.Policy
}

// New creates a Handler with the given ObjectStore and flag evaluation function.
// The flagBool func is typically featureflags.Flags.Bool — passing it as a
// function decouples the handler from the concrete Flags type, making it
// trivially testable with a closure.
func New(store blob.ObjectStore, flagBool func(context.Context, string, bool) bool, opts ...Option) *Handler {
	h := &Handler{
		store:        store,
		flagBool:     flagBool,
		flagKey:      defaultFlagKey,
		presignTTL:   defaultPresignTTL,
		requiredRole: defaultRole,
		ownership:    ownerPolicy{bypassRole: AdminRole},
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Mount registers the attachment routes on r.
//
//	POST /v1/orders/{id}/attachment         → Upload
//	GET  /v1/orders/{id}/attachment/{name}  → Download
func (h *Handler) Mount(r chi.Router) {
	r.Post("/v1/orders/{id}/attachment", h.Upload)
	r.Get("/v1/orders/{id}/attachment/{name}", h.Download)
}

// sanitizeName strips directory components from s (via path.Base) and
// validates the result. It returns ("", false) if:
//   - the raw input contains a path separator ("/" or "\"), indicating a
//     deliberate traversal attempt or injection
//   - the base result is ".", "..", or empty
//   - the result contains a path separator or any control byte (< 0x20)
//   - the result is longer than maxFilenameLen characters
//
// Rejecting path separators in the raw input is defence-in-depth: a legitimate
// filename header value should never contain "/" or "\".
func sanitizeName(s string) (string, bool) {
	// Reject raw inputs that contain path separators — a filename header value
	// should never embed directory components.
	if strings.ContainsAny(s, "/\\") {
		return "", false
	}
	base := path.Base(s)
	if base == "." || base == ".." || base == "" {
		return "", false
	}
	if strings.ContainsAny(base, "/\\") {
		return "", false
	}
	for i := range len(base) {
		if base[i] < 0x20 {
			return "", false
		}
	}
	if len(base) > maxFilenameLen {
		return "", false
	}
	return base, true
}

// hasRole reports whether p has the given role.
func hasRole(p auth.Principal, role string) bool {
	return slices.Contains(p.Roles, role)
}

// roleGate enforces the authentication + role requirements shared by Upload
// and Download. It writes the error response and returns (Principal{}, false)
// when the request is denied. The admin role always passes the gate.
func (h *Handler) roleGate(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	p, ok := auth.From(r.Context())
	if !ok {
		httpx.WriteError(w, r, authz.ErrUnauthenticated)
		return auth.Principal{}, false
	}
	if !hasRole(p, h.requiredRole) && !hasRole(p, AdminRole) {
		httpx.WriteError(w, r, apperr.Wrap(authz.ErrForbidden, apperr.CodeAuthForbidden).WithParam("role", h.requiredRole))
		return auth.Principal{}, false
	}
	return p, true
}

// checkOwnership enforces the ownership policy for the given (already
// validated) order id. No-op (allow) when no OwnerLookup is configured.
// It writes the error response and returns false when the request is denied.
func (h *Handler) checkOwnership(w http.ResponseWriter, r *http.Request, p auth.Principal, orderID string) bool {
	if h.ownerLookup == nil {
		return true
	}
	ctx := r.Context()
	owner, err := h.ownerLookup(ctx, orderID)
	if err != nil {
		if errors.Is(err, ErrOwnerNotFound) {
			httpx.WriteError(w, r, apperr.Wrap(err, apperrs.CodeOrderNotFound).WithParam("order_id", orderID))
		} else {
			// Fail closed: an unavailable read model must never grant access.
			httpx.WriteError(w, r, err) // unknown error -> 500 INTERNAL, no leak
		}
		return false
	}
	if err := h.ownership.Authorize(ctx, p, "attachment:access", owner); err != nil {
		httpx.WriteError(w, r, err) // authz.ErrForbidden -> 403 AUTH_FORBIDDEN
		return false
	}
	return true
}

// Upload stores the request body as an attachment for the order identified by
// the {id} URL parameter. The object key is "orders/<id>/<filename>" where
// filename comes from the X-Filename request header (defaulting to "file").
//
// Returns 404 when the feature flag is off, 400 on an invalid id or filename,
// 401 when the principal is absent, 403 when the principal lacks the required
// role, 201 with {"key": "<key>"} on success, or 500 on storage errors.
func (h *Handler) Upload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.flagBool(ctx, h.flagKey, false) {
		httpx.WriteError(w, r, apperr.New(apperrs.CodeAttachmentsDisabled))
		return
	}

	// Authorization: require authenticated principal with the correct role.
	p, ok := h.roleGate(w, r)
	if !ok {
		return
	}

	// Validate {id}: must be a valid UUID.
	rawID := chi.URLParam(r, "id")
	if _, err := uuid.Parse(rawID); err != nil {
		httpx.WriteError(w, r, apperr.Wrap(err, apperrs.CodeAttachmentInvalidOrderID))
		return
	}

	// Ownership: the order must belong to the principal (admin bypasses).
	if !h.checkOwnership(w, r, p, rawID) {
		return
	}

	rawFilename := r.Header.Get("X-Filename")
	if rawFilename == "" {
		rawFilename = "file"
	}
	filename, ok := sanitizeName(rawFilename)
	if !ok {
		httpx.WriteError(w, r, apperr.New(apperrs.CodeAttachmentInvalidFilename))
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	key := "orders/" + rawID + "/" + filename

	if err := h.store.Put(ctx, key, bytes.NewReader(body), int64(len(body)), contentType); err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	httpx.JSON(w, http.StatusCreated, map[string]string{"key": key})
}

// Download redirects to a pre-signed GET URL for the attachment identified by
// the {id} and {name} URL parameters. The object key is "orders/<id>/<name>".
//
// Returns 404 when the feature flag is off or the object does not exist, 400 on
// an invalid id or name, 401 when the principal is absent, 403 when the
// principal lacks the required role, 302 to the presigned URL on success, or
// 500 on storage errors.
func (h *Handler) Download(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.flagBool(ctx, h.flagKey, false) {
		httpx.WriteError(w, r, apperr.New(apperrs.CodeAttachmentsDisabled))
		return
	}

	// Authorization: require authenticated principal with the correct role.
	p, ok := h.roleGate(w, r)
	if !ok {
		return
	}

	// Validate {id}: must be a valid UUID.
	rawID := chi.URLParam(r, "id")
	if _, err := uuid.Parse(rawID); err != nil {
		httpx.WriteError(w, r, apperr.Wrap(err, apperrs.CodeAttachmentInvalidOrderID))
		return
	}

	// Ownership: the order must belong to the principal (admin bypasses).
	if !h.checkOwnership(w, r, p, rawID) {
		return
	}

	rawName := chi.URLParam(r, "name")
	name, ok := sanitizeName(rawName)
	if !ok {
		httpx.WriteError(w, r, apperr.New(apperrs.CodeAttachmentInvalidFilename))
		return
	}

	key := "orders/" + rawID + "/" + name

	exists, err := h.store.Exists(ctx, key)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if !exists {
		httpx.WriteError(w, r, apperr.New(apperrs.CodeAttachmentNotFound).WithParams(map[string]any{"order_id": rawID, "filename": name}))
		return
	}

	u, err := h.store.PresignGet(ctx, key, h.presignTTL)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	http.Redirect(w, r, u, http.StatusFound) //nolint:gosec // G710: URL is a presigned S3 URL generated by the object store, not user input
}
