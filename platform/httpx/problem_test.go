package httpx_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/httpx"
)

func TestWriteProblem_SetsStatusAndContentType(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.WriteProblem(rec, httpx.Problem{
		Status: http.StatusNotFound,
		Title:  "Not Found",
		Detail: "order 42 not found",
	})

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, float64(404), body["status"])
	require.Equal(t, "order 42 not found", body["detail"])
}
