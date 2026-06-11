package auth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-boilerplate/platform/security/auth"
)

// benchOKHandler is the no-op next handler — its cost is excluded from the
// numbers we care about (the len-guard + verifier dispatch).
func benchOKHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

// trivialVerifier returns immediately without parsing — isolates the
// middleware's len-guard + dispatch cost from real jwt.Parse work, so the
// "under-cap reaches verifier" path measures only the guard overhead.
type trivialVerifier struct{}

func (trivialVerifier) Verify(_ context.Context, _ string) (auth.Principal, error) {
	return auth.Principal{Subject: "x"}, nil
}

// BenchmarkBearerTokenCheck_UnderCap measures the per-request cost of the
// length guard on the COMMON path (token within the cap → proceeds to the
// verifier). This is the overhead the cap adds to every legitimate request;
// it should be a handful of nanoseconds (one len() + compare).
func BenchmarkBearerTokenCheck_UnderCap(b *testing.B) {
	h := auth.Middleware(trivialVerifier{})(benchOKHandler())
	authz := "Bearer " + strings.Repeat("x", 900) // realistic JWT size

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", authz)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
}

// BenchmarkBearerTokenCheck_GuardOnly isolates the len-guard overhead from
// httptest request construction by reusing one pre-built request per case. The
// delta between the under-cap (reaches verifier) and over-cap (rejected by the
// guard) numbers is the actual cost the cap adds — it should be a couple of
// nanoseconds, since both paths run the same len-compare and the reject path
// SKIPS the verifier entirely.
func BenchmarkBearerTokenCheck_GuardOnly(b *testing.B) {
	cases := []struct {
		name  string
		token string
	}{
		{"under_cap_900B", strings.Repeat("x", 900)},
		{"over_cap_16KB", strings.Repeat("x", 16*1024)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			h := auth.Middleware(trivialVerifier{}, auth.WithMaxTokenBytes(8192))(benchOKHandler())
			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			req.Header.Set("Authorization", "Bearer "+tc.token)
			rec := httptest.NewRecorder()

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				h.ServeHTTP(rec, req)
			}
		})
	}
}

// BenchmarkBearerTokenCheck_OverCapRejected measures the over-cap REJECT path:
// an 8KB-over-cap token must be turned away by the len-guard WITHOUT entering
// the verifier (no jwt.Parse). Compare its ns/op against the under-cap path —
// the reject is a cheap len-compare, so it must be in the same order of
// magnitude (and never trigger the expensive parse it is defending against).
func BenchmarkBearerTokenCheck_OverCapRejected(b *testing.B) {
	h := auth.Middleware(trivialVerifier{}, auth.WithMaxTokenBytes(8192))(benchOKHandler())
	authz := "Bearer " + strings.Repeat("x", 16*1024) // 16KB, well over cap

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", authz)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
}
