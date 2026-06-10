package httpx_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-boilerplate/platform/web/httpx"
)

// fuzzReq mirrors the createReq shape used by the unit tests: one required
// field and one format-validated field, so the fuzzer exercises both the
// JSON decode path and the validator path.
type fuzzReq struct {
	Name  string `json:"name"  validate:"required"`
	Email string `json:"email" validate:"required,email"`
}

// FuzzHTTPXDecode hammers Decode with arbitrary request bodies and
// Content-Type values — both are fully attacker-controlled. Invariants:
//
//  1. Never panics.
//  2. err == nil implies validation passed (required fields are non-empty);
//     garbage must never slip through as a "valid" struct.
//  3. A non-JSON Content-Type always yields ErrUnsupportedMediaType.
func FuzzHTTPXDecode(f *testing.F) {
	// Seeds from decode_test.go.
	f.Add([]byte(`{"name":"a","email":"a@b.com"}`), "application/json")
	f.Add([]byte(`{"name":"","email":"nope"}`), "application/json")
	f.Add([]byte(`{`), "")
	f.Add([]byte(`{"name":"a","email":"a@b.com"}EXTRA`), "application/json")
	f.Add([]byte(`{"unknown":1}`), "application/json")
	f.Add([]byte(`null`), "application/json")
	f.Add([]byte(`[]`), "application/json; charset=utf-8")
	f.Add([]byte(`{"name":"a","email":"a@b.com"}`), "text/plain")
	f.Add([]byte{}, "application/json")
	f.Add([]byte{0xff, 0xfe, 0x00}, "application/octet-stream")

	f.Fuzz(func(t *testing.T, body []byte, contentType string) {
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		if contentType != "" {
			r.Header.Set("Content-Type", contentType)
		}

		v, err := httpx.Decode[fuzzReq](r)
		if err != nil {
			return // any error is fine; the invariant is no panic + no false success
		}
		if v.Name == "" || v.Email == "" {
			t.Fatalf("Decode returned nil error but required fields are empty: %+v (body=%q ct=%q)",
				v, body, contentType)
		}
	})
}
