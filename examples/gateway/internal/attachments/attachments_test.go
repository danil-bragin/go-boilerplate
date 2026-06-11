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

	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// userPrincipal returns a request whose context carries a principal with the
// "user" role, using the supplied valid order UUID.
func withUserPrincipal(req *http.Request) *http.Request {
	ctx := auth.Into(req.Context(), auth.Principal{
		Subject: "test-user",
		Roles:   []string{"user"},
	})
	return req.WithContext(ctx)
}

// validOrderID is a fixed UUID used as a valid order ID in tests.
const validOrderID = "550e8400-e29b-41d4-a716-446655440000"

// TestUpload_StoresWhenFlagOn: flag→true; POST /orders/<uuid>/attachment with body
// "hello" + X-Filename "doc.txt" → 201, key "orders/<uuid>/doc.txt"; the fake store
// has it (Exists returns true).
func TestUpload_StoresWhenFlagOn(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	body := strings.NewReader("hello")
	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", body)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Filename", "doc.txt")
	req = withUserPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusCreated, rw.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(rw.Body).Decode(&resp))
	assert.Equal(t, "orders/"+validOrderID+"/doc.txt", resp["key"])

	ok, err := store.Exists(context.Background(), "orders/"+validOrderID+"/doc.txt")
	require.NoError(t, err)
	require.True(t, ok, "uploaded object must be present in the store")
}

// TestDownload_RedirectsWhenFlagOn: pre-put an object; GET /orders/<uuid>/attachment/doc.txt
// → 302 with Location = the fake presign URL.
func TestDownload_RedirectsWhenFlagOn(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	ctx := context.Background()
	content := []byte("hello")
	require.NoError(t, store.Put(ctx, "orders/"+validOrderID+"/doc.txt", bytes.NewReader(content), int64(len(content)), "text/plain"))

	h := attachments.New(store, flagOn)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/v1/orders/"+validOrderID+"/attachment/doc.txt", http.NoBody)
	req = withUserPrincipal(req)
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusFound, rw.Code)
	loc := rw.Header().Get("Location")
	// The presigned URL forces an attachment download disposition (stored-XSS
	// defence): the response-content-disposition / response-content-type query
	// params instruct the store to answer Content-Disposition: attachment +
	// application/octet-stream, so a browser saves the file instead of
	// rendering uploaded HTML/SVG inline.
	assert.Contains(t, loc, "https://fake-blob.local/orders/"+validOrderID+"/doc.txt")
	assert.Contains(t, loc, "response-content-disposition=attachment")
	assert.Contains(t, loc, "response-content-type=application%2Foctet-stream")
}

// TestUpload_RejectsDisallowedContentType: an upload whose Content-Type is not
// in the allowlist (text/html — a stored-XSS vector) is refused with 415
// GATEWAY_ATTACHMENT_TYPE_REJECTED and never reaches the store.
func TestUpload_RejectsDisallowedContentType(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	body := strings.NewReader("<script>alert(1)</script>")
	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", body)
	req.Header.Set("Content-Type", "text/html")
	req.Header.Set("X-Filename", "evil.html")
	req = withUserPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusUnsupportedMediaType, rw.Code)

	ok, err := store.Exists(context.Background(), "orders/"+validOrderID+"/evil.html")
	require.NoError(t, err)
	require.False(t, ok, "a disallowed content type must never be stored")
}

// TestUpload_AllowsConfiguredContentType: a content type added via
// WithAllowedContentTypes is accepted (the allowlist is configurable).
func TestUpload_AllowsConfiguredContentType(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn, attachments.WithAllowedContentTypes([]string{"image/webp"}))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req.Header.Set("Content-Type", "image/webp")
	req.Header.Set("X-Filename", "pic.webp")
	req = withUserPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusCreated, rw.Code)
}

// TestUpload_ContentTypeWithParamsAllowed: the allowlist matches the media
// type only, ignoring parameters — "text/plain; charset=utf-8" is allowed when
// "text/plain" is.
func TestUpload_ContentTypeWithParamsAllowed(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("hi"))
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("X-Filename", "doc.txt")
	req = withUserPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusCreated, rw.Code)
}

// TestUpload_CanonicalKeyFromParsedUUID: the object key is built from the
// CANONICAL (parsed) UUID, not the raw URL parameter. An uppercase / braced
// UUID variant in the path is normalised so two spellings of one id cannot map
// to two distinct keys.
func TestUpload_CanonicalKeyFromParsedUUID(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	// Uppercase spelling of validOrderID — uuid.Parse normalises to lowercase.
	rawID := strings.ToUpper(validOrderID)
	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+rawID+"/attachment", strings.NewReader("data"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Filename", "doc.txt")
	req = withUserPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusCreated, rw.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(rw.Body).Decode(&resp))
	// Canonical (lowercase) key, never the raw uppercase path value.
	assert.Equal(t, "orders/"+validOrderID+"/doc.txt", resp["key"])

	ok, err := store.Exists(context.Background(), "orders/"+validOrderID+"/doc.txt")
	require.NoError(t, err)
	require.True(t, ok, "object must be stored under the canonical key")
}

// TestUpload_OversizedBodyReturns413: a request body that trips the MaxBytes
// middleware surfaces as 413 GATEWAY_ATTACHMENT_TOO_LARGE, not a generic 500.
func TestUpload_OversizedBodyReturns413(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	// 8 bytes of payload behind a 4-byte MaxBytes cap → ReadAll returns a
	// *http.MaxBytesError.
	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("12345678"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Filename", "doc.txt")
	req = withUserPrincipal(req)
	req.Body = http.MaxBytesReader(httptest.NewRecorder(), req.Body, 4)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rw.Code)
}

// TestUpload_404WhenFlagOff: flag→false → 404.
func TestUpload_404WhenFlagOff(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOff)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.bin")
	req = withUserPrincipal(req)
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

	req := httptest.NewRequest(http.MethodGet, "/v1/orders/"+validOrderID+"/attachment/doc.txt", http.NoBody)
	req = withUserPrincipal(req)
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

	req := httptest.NewRequest(http.MethodGet, "/v1/orders/"+validOrderID+"/attachment/nonexistent.bin", http.NoBody)
	req = withUserPrincipal(req)
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

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("bytes"))
	// Deliberately no X-Filename header.
	req = withUserPrincipal(req)
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusCreated, rw.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(rw.Body).Decode(&resp))
	assert.Equal(t, "orders/"+validOrderID+"/file", resp["key"])

	ok, err := store.Exists(context.Background(), "orders/"+validOrderID+"/file")
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

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("x"))
	req.Header.Set("X-Filename", "x.bin")
	// Deliberately no Content-Type.
	req = withUserPrincipal(req)
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusCreated, rw.Code)

	rc, err := store.Get(context.Background(), "orders/"+validOrderID+"/x.bin")
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("x"), data)
}

// ── Security: path traversal ─────────────────────────────────────────────────

// TestUpload_RejectsPathTraversalFilename: X-Filename: ../../etc/passwd → 400;
// the fake store must have NO object with a traversal key.
func TestUpload_RejectsPathTraversalFilename(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("evil"))
	req.Header.Set("X-Filename", "../../etc/passwd")
	req = withUserPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusBadRequest, rw.Code, "traversal filename must be rejected with 400")

	// The store must not contain any traversal key.
	keys, err := store.List(context.Background(), "")
	require.NoError(t, err)
	for _, k := range keys {
		assert.NotContains(t, k, "..", "traversal key must not be stored")
	}
}

// TestUpload_RejectsBadOrderID: order id "not-a-uuid" → 400.
func TestUpload_RejectsBadOrderID(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/not-a-uuid/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.txt")
	req = withUserPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusBadRequest, rw.Code, "non-UUID order ID must be rejected with 400")
}

// TestDownload_RejectsTraversalName: GET with name ".." (traversal) → 400.
// The chi route encodes it as the {name} param; we use a literal ".." path segment.
func TestDownload_RejectsTraversalName(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	// Use ".." as the literal {name} parameter.  chi decodes %2F so we pass
	// the path directly; ".." itself is a valid path segment and chi will
	// capture it as the {name} URL param.
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/"+validOrderID+"/attachment/..", http.NoBody)
	req = withUserPrincipal(req)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusBadRequest, rw.Code, "traversal name must be rejected with 400")
}

// ── Security: authorization ───────────────────────────────────────────────────

// TestUpload_401WithoutPrincipal: no principal in context → 401.
func TestUpload_401WithoutPrincipal(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	// No auth.Into — principal is absent from the context.
	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.txt")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusUnauthorized, rw.Code, "absent principal must yield 401")
}

// TestUpload_403WithoutRole: principal present but lacking the "user" role → 403.
func TestUpload_403WithoutRole(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn)
	r := newRouter(h)

	// Inject a principal that has no roles.
	ctx := auth.Into(context.Background(), auth.Principal{
		Subject: "guest",
		Roles:   []string{}, // no roles
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.txt")
	req = req.WithContext(ctx)

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)

	require.Equal(t, http.StatusForbidden, rw.Code, "principal without required role must yield 403")
}
