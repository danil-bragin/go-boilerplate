package attachments_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/platform/testkit/fakes"
)

// flagOn is a flagBool func that always returns true (feature enabled).
func flagOn(_ context.Context, _ string, _ bool) bool { return true }

// flagOff is a flagBool func that always returns false (feature disabled).
func flagOff(_ context.Context, _ string, _ bool) bool { return false }

// newRouter wires h into a chi router matching the attachment routes.
func newRouter(h *attachments.Handler) chi.Router {
	r := chi.NewRouter()
	h.Mount(r)
	return r
}

// TestUpload_StoresWhenFlagOn: flag→true; POST /orders/o1/attachment with body
// "hello" + X-Filename "doc.txt" → 201, key "orders/o1/doc.txt"; the fake store
// has it (Exists returns true).
func TestUpload_StoresWhenFlagOn(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	body := strings.NewReader("hello")
	req := httptest.NewRequest(http.MethodPost, "/orders/o1/attachment", body)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Filename", "doc.txt")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusCreated, rw.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(rw.Body).Decode(&resp))
	assert.Equal(t, "orders/o1/doc.txt", resp["key"])

	ok, err := store.Exists(context.Background(), "orders/o1/doc.txt")
	require.NoError(t, err)
	require.True(t, ok, "uploaded object must be present in the store")
}

// TestDownload_RedirectsWhenFlagOn: pre-put an object; GET /orders/o1/attachment/doc.txt
// → 302 with Location = the fake presign URL.
func TestDownload_RedirectsWhenFlagOn(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	ctx := context.Background()
	content := []byte("hello")
	require.NoError(t, store.Put(ctx, "orders/o1/doc.txt", bytes.NewReader(content), int64(len(content)), "text/plain"))

	h := attachments.New(store, flagOn)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/orders/o1/attachment/doc.txt", nil)
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusFound, rw.Code)
	loc := rw.Header().Get("Location")
	assert.Equal(t, "https://fake-blob.local/orders/o1/doc.txt", loc)
}

// TestUpload_404WhenFlagOff: flag→false → 404.
func TestUpload_404WhenFlagOff(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOff)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/orders/o1/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.bin")
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusNotFound, rw.Code)
}

// TestDownload_404WhenFlagOff: flag→false → 404.
func TestDownload_404WhenFlagOff(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOff)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/orders/o1/attachment/doc.txt", nil)
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusNotFound, rw.Code)
}

// TestDownload_404WhenMissing: flag on, object absent → 404.
func TestDownload_404WhenMissing(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/orders/o1/attachment/nonexistent.bin", nil)
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusNotFound, rw.Code)
}

// TestUpload_FallbackFilenameWhenHeaderMissing: no X-Filename header → key uses
// "file" as the fallback filename.
func TestUpload_FallbackFilenameWhenHeaderMissing(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/orders/o2/attachment", strings.NewReader("bytes"))
	// Deliberately no X-Filename header.
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusCreated, rw.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(rw.Body).Decode(&resp))
	assert.Equal(t, "orders/o2/file", resp["key"])

	ok, err := store.Exists(context.Background(), "orders/o2/file")
	require.NoError(t, err)
	require.True(t, ok)
}

// TestUpload_DefaultContentType: no Content-Type header → defaults to
// "application/octet-stream" (verified by round-trip via fakes.ObjectStore).
func TestUpload_DefaultContentType(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/orders/o3/attachment", strings.NewReader("x"))
	req.Header.Set("X-Filename", "x.bin")
	// Deliberately no Content-Type.
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusCreated, rw.Code)

	rc, err := store.Get(context.Background(), "orders/o3/x.bin")
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("x"), data)
}
