package httpx

import (
	"encoding/json"
	"net/http"
)

// JSON writes v as application/json with the given status.
// It marshals v before writing the status header so that encode errors
// result in a 500 instead of a committed-but-corrupt response.
func JSON(w http.ResponseWriter, status int, v any) {
	if v == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		Error(w, http.StatusInternalServerError, "failed to encode response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
