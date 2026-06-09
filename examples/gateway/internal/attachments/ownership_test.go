package attachments_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/stretchr/testify/require"
)

// ownerLookupFor returns an OwnerLookup that maps validOrderID → owner and
// reports ErrOwnerNotFound for anything else.
func ownerLookupFor(owner string) attachments.OwnerLookup {
	return func(_ context.Context, orderID string) (string, error) {
		if orderID == validOrderID {
			return owner, nil
		}
		return "", attachments.ErrOwnerNotFound
	}
}

func withPrincipal(req *http.Request, sub string, roles ...string) *http.Request {
	ctx := auth.Into(req.Context(), auth.Principal{Subject: sub, Roles: roles})
	return req.WithContext(ctx)
}

// TestUpload_Ownership_OwnerAllowed: principal sub == order customer_id → 201.
func TestUpload_Ownership_OwnerAllowed(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn, attachments.WithOwnerLookup(ownerLookupFor("alice")))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.txt")
	req = withPrincipal(req, "alice", "user")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusCreated, rw.Code, "order owner must be allowed to upload")
}

// TestUpload_Ownership_NonOwnerForbidden: sub != customer_id and no admin role → 403.
func TestUpload_Ownership_NonOwnerForbidden(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn, attachments.WithOwnerLookup(ownerLookupFor("alice")))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.txt")
	req = withPrincipal(req, "mallory", "user")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusForbidden, rw.Code, "non-owner must be forbidden")
}

// TestUpload_Ownership_AdminBypasses: admin role bypasses the ownership check.
func TestUpload_Ownership_AdminBypasses(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	h := attachments.New(store, flagOn, attachments.WithOwnerLookup(ownerLookupFor("alice")))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.txt")
	req = withPrincipal(req, "support-staff", "admin")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusCreated, rw.Code, "admin must bypass ownership")
}

// TestDownload_Ownership_NonOwnerForbidden mirrors the upload check on the
// read path.
func TestDownload_Ownership_NonOwnerForbidden(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	seedObject(t, store, "orders/"+validOrderID+"/file.txt")

	h := attachments.New(store, flagOn, attachments.WithOwnerLookup(ownerLookupFor("alice")))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/orders/"+validOrderID+"/attachment/file.txt", http.NoBody)
	req = withPrincipal(req, "mallory", "user")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusForbidden, rw.Code)
}

// TestDownload_Ownership_OwnerAllowed: the owner gets the presigned redirect.
func TestDownload_Ownership_OwnerAllowed(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	seedObject(t, store, "orders/"+validOrderID+"/file.txt")

	h := attachments.New(store, flagOn, attachments.WithOwnerLookup(ownerLookupFor("alice")))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/orders/"+validOrderID+"/attachment/file.txt", http.NoBody)
	req = withPrincipal(req, "alice", "user")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusFound, rw.Code)
}

// TestOwnership_UnknownOrder404: lookup reporting ErrOwnerNotFound → 404
// (the order does not exist in the read model yet).
func TestOwnership_UnknownOrder404(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	lookup := attachments.OwnerLookup(func(context.Context, string) (string, error) {
		return "", attachments.ErrOwnerNotFound
	})
	h := attachments.New(store, flagOn, attachments.WithOwnerLookup(lookup))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req = withPrincipal(req, "alice", "user")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusNotFound, rw.Code)
}

// TestOwnership_LookupError500: infrastructure failure in the lookup → 500,
// fail closed (never allow on error).
func TestOwnership_LookupError500(t *testing.T) {
	t.Parallel()

	store := fakes.NewObjectStore()
	lookup := attachments.OwnerLookup(func(context.Context, string) (string, error) {
		return "", errors.New("read model unavailable")
	})
	h := attachments.New(store, flagOn, attachments.WithOwnerLookup(lookup))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req = withPrincipal(req, "alice", "user")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusInternalServerError, rw.Code)
}

// seedObject puts a small object into the fake store so Download's existence
// check passes.
func seedObject(t *testing.T, store *fakes.ObjectStore, key string) {
	t.Helper()
	err := store.Put(context.Background(), key, strings.NewReader("x"), 1, "text/plain")
	require.NoError(t, err)
}
