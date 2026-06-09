// Package attachments provides an HTTP handler for order file attachments.
// Objects are stored in an S3-compatible object store (blob.ObjectStore) and
// the upload/download endpoints are gated by an OpenFeature boolean flag
// ("order-attachments-enabled"). When the flag is off both endpoints return 404.
package attachments

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"go-boilerplate/platform/blob"
	"go-boilerplate/platform/httpx"
)

const (
	defaultFlagKey    = "order-attachments-enabled"
	defaultPresignTTL = 5 * time.Minute
)

// Handler handles upload and download of order attachments.
type Handler struct {
	store      blob.ObjectStore
	flagBool   func(ctx context.Context, key string, def bool) bool
	flagKey    string
	presignTTL time.Duration
}

// New creates a Handler with the given ObjectStore and flag evaluation function.
// The flagBool func is typically featureflags.Flags.Bool — passing it as a
// function decouples the handler from the concrete Flags type, making it
// trivially testable with a closure.
func New(store blob.ObjectStore, flagBool func(context.Context, string, bool) bool) *Handler {
	return &Handler{
		store:      store,
		flagBool:   flagBool,
		flagKey:    defaultFlagKey,
		presignTTL: defaultPresignTTL,
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

// Upload stores the request body as an attachment for the order identified by
// the {id} URL parameter. The object key is "orders/<id>/<filename>" where
// filename comes from the X-Filename request header (defaulting to "file").
//
// Returns 404 when the feature flag is off, 201 with {"key": "<key>"} on
// success, or 500 on storage errors.
func (h *Handler) Upload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.flagBool(ctx, h.flagKey, false) {
		httpx.Error(w, http.StatusNotFound, "attachments disabled")
		return
	}

	id := chi.URLParam(r, "id")

	filename := r.Header.Get("X-Filename")
	if filename == "" {
		filename = "file"
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

	key := "orders/" + id + "/" + filename

	if err := h.store.Put(ctx, key, bytes.NewReader(body), int64(len(body)), contentType); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to store attachment")
		return
	}

	httpx.JSON(w, http.StatusCreated, map[string]string{"key": key})
}

// Download redirects to a pre-signed GET URL for the attachment identified by
// the {id} and {name} URL parameters. The object key is "orders/<id>/<name>".
//
// Returns 404 when the feature flag is off or the object does not exist, 302 to
// the presigned URL on success, or 500 on storage errors.
func (h *Handler) Download(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.flagBool(ctx, h.flagKey, false) {
		httpx.Error(w, http.StatusNotFound, "attachments disabled")
		return
	}

	id := chi.URLParam(r, "id")
	name := chi.URLParam(r, "name")
	key := "orders/" + id + "/" + name

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
