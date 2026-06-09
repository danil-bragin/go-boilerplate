// Package attachments provides an HTTP handler for order file attachments.
// Objects are stored in an S3-compatible object store (blob.ObjectStore) and
// the upload/download endpoints are gated by an OpenFeature boolean flag
// ("order-attachments-enabled"). When the flag is off both endpoints return 404.
//
// # Authorization
//
// Upload and Download require the principal (set by the auth middleware) to
// hold the requiredRole (default "user"). Absent principal → 401; missing
// role → 403.
//
// # TODO(ownership)
//
// Production deployments should verify that the order identified by {id}
// belongs to the authenticated principal (compare principal Subject/claims to
// the order's owner stored in the read model), not merely check a role. This
// handler stubs authorization with a role-gate as a boilerplate demonstration.
package attachments

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/blob"
	"go-boilerplate/platform/httpx"
)

const (
	defaultFlagKey    = "order-attachments-enabled"
	defaultPresignTTL = 5 * time.Minute
	defaultRole       = "user"
	maxFilenameLen    = 128
)

// Handler handles upload and download of order attachments.
type Handler struct {
	store        blob.ObjectStore
	flagBool     func(ctx context.Context, key string, def bool) bool
	flagKey      string
	presignTTL   time.Duration
	requiredRole string
}

// New creates a Handler with the given ObjectStore and flag evaluation function.
// The flagBool func is typically featureflags.Flags.Bool — passing it as a
// function decouples the handler from the concrete Flags type, making it
// trivially testable with a closure.
func New(store blob.ObjectStore, flagBool func(context.Context, string, bool) bool) *Handler {
	return &Handler{
		store:        store,
		flagBool:     flagBool,
		flagKey:      defaultFlagKey,
		presignTTL:   defaultPresignTTL,
		requiredRole: defaultRole,
	}
}

// Mount registers the attachment routes on r.
//
//	POST /orders/{id}/attachment         → Upload
//	GET  /orders/{id}/attachment/{name}  → Download
func (h *Handler) Mount(r chi.Router) {
	r.Post("/orders/{id}/attachment", h.Upload)
	r.Get("/orders/{id}/attachment/{name}", h.Download)
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
		httpx.Error(w, http.StatusNotFound, "attachments disabled")
		return
	}

	// Authorization: require authenticated principal with the correct role.
	p, ok := auth.From(ctx)
	if !ok {
		httpx.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if !hasRole(p, h.requiredRole) {
		httpx.Error(w, http.StatusForbidden, "required role: "+h.requiredRole)
		return
	}

	// Validate {id}: must be a valid UUID.
	rawID := chi.URLParam(r, "id")
	if _, err := uuid.Parse(rawID); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid order id")
		return
	}

	rawFilename := r.Header.Get("X-Filename")
	if rawFilename == "" {
		rawFilename = "file"
	}
	filename, ok := sanitizeName(rawFilename)
	if !ok {
		httpx.Error(w, http.StatusBadRequest, "invalid filename")
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to read request body")
		return
	}

	key := "orders/" + rawID + "/" + filename

	if err := h.store.Put(ctx, key, bytes.NewReader(body), int64(len(body)), contentType); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to store attachment")
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
		httpx.Error(w, http.StatusNotFound, "attachments disabled")
		return
	}

	// Authorization: require authenticated principal with the correct role.
	p, ok := auth.From(ctx)
	if !ok {
		httpx.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if !hasRole(p, h.requiredRole) {
		httpx.Error(w, http.StatusForbidden, "required role: "+h.requiredRole)
		return
	}

	// Validate {id}: must be a valid UUID.
	rawID := chi.URLParam(r, "id")
	if _, err := uuid.Parse(rawID); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid order id")
		return
	}

	rawName := chi.URLParam(r, "name")
	name, ok := sanitizeName(rawName)
	if !ok {
		httpx.Error(w, http.StatusBadRequest, "invalid filename")
		return
	}

	key := "orders/" + rawID + "/" + name

	exists, err := h.store.Exists(ctx, key)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to check attachment existence")
		return
	}
	if !exists {
		httpx.Error(w, http.StatusNotFound, "attachment not found")
		return
	}

	u, err := h.store.PresignGet(ctx, key, h.presignTTL)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to generate download URL")
		return
	}

	http.Redirect(w, r, u, http.StatusFound)
}
