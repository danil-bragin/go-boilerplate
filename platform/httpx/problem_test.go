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

// FIX 5 — JSON() must marshal before writing status; unmarshalable value → 500.
func TestJSON_UnmarshalableValue_Returns500(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.JSON(rec, http.StatusOK, make(chan int))

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	require.NotEmpty(t, rec.Body.Bytes())
}

// FIX 5 — JSON() with a valid value must preserve status, content-type, and body.
func TestJSON_ValidValue(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.JSON(rec, http.StatusCreated, map[string]string{"key": "val"})

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "val", body["key"])
}

// FIX 5 — WriteProblem with a valid problem must preserve status and content-type.
func TestWriteProblem_ValidProblem(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.WriteProblem(rec, httpx.Problem{
		Status: http.StatusBadRequest,
		Title:  "Bad Request",
		Detail: "missing field",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	require.NotEmpty(t, rec.Body.Bytes())
}
