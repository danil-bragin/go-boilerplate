package attachments_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go-boilerplate/examples/gateway/internal/attachments"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingAuditor captures the entries passed to RecordOutOfBand.
type recordingAuditor struct {
	mu      sync.Mutex
	entries []audit.Entry
}

func (a *recordingAuditor) RecordOutOfBand(_ context.Context, e audit.Entry) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = append(a.entries, e)
	return nil
}

func (a *recordingAuditor) byAction(action string) []audit.Entry {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []audit.Entry
	for _, e := range a.entries {
		if e.Action == action {
			out = append(out, e)
		}
	}
	return out
}

// TestAttachments_AuditsUploadSuccess: a successful upload records an
// attachment.upload audit entry attributed to the principal.
func TestAttachments_AuditsUploadSuccess(t *testing.T) {
	t.Parallel()

	aud := &recordingAuditor{}
	h := attachments.New(fakes.NewObjectStore(), flagOn,
		attachments.WithOwnerLookup(ownerLookupFor("alice")),
		attachments.WithAuditor(aud))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.txt")
	req = withPrincipal(req, "alice", "user")

	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusCreated, rw.Code)

	uploads := aud.byAction(audit.ActionAttachmentUpload)
	require.Len(t, uploads, 1, "successful upload must be audited")
	assert.Equal(t, "alice", uploads[0].Actor)
	assert.Contains(t, uploads[0].Subject, validOrderID)
}

// TestAttachments_AuditsOwnershipDenial: a non-owner upload records an
// authz.denied audit entry attributed to the denied (but authenticated)
// principal.
func TestAttachments_AuditsOwnershipDenial(t *testing.T) {
	t.Parallel()

	aud := &recordingAuditor{}
	h := attachments.New(fakes.NewObjectStore(), flagOn,
		attachments.WithOwnerLookup(ownerLookupFor("alice")),
		attachments.WithAuditor(aud))
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+validOrderID+"/attachment", strings.NewReader("data"))
	req.Header.Set("X-Filename", "file.txt")
	req = withPrincipal(req, "mallory", "user") // not the owner, no admin
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	require.Equal(t, http.StatusForbidden, rw.Code)

	denials := aud.byAction(audit.ActionAuthzDenied)
	require.Len(t, denials, 1, "ownership denial must be audited")
	assert.Equal(t, "mallory", denials[0].Actor)
	assert.Equal(t, "attachment:access", denials[0].Metadata["denied_action"])
}
