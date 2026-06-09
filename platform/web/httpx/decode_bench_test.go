package httpx_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-boilerplate/platform/web/httpx"
)

func BenchmarkDecode(b *testing.B) {
	body := []byte(`{"name":"alice","email":"alice@example.com"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		_, _ = httpx.Decode[benchReq](r)
	}
}

type benchReq struct {
	Name  string `json:"name" validate:"required"`
	Email string `json:"email" validate:"required,email"`
}
