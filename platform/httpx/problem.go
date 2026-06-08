// Package httpx provides JSON request decoding+validation and RFC7807
// problem+json error responses for HTTP handlers.
package httpx

import (
	"encoding/json"
	"net/http"
)

// Problem is an RFC7807 problem detail.
type Problem struct {
	Type   string `json:"type,omitempty"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	// Errors carries field-level validation messages (optional extension).
	Errors map[string]string `json:"errors,omitempty"`
}

// WriteProblem writes p as application/problem+json with p.Status.
// It marshals p before writing the status header so that encode errors
// never produce a committed-but-empty response.
func WriteProblem(w http.ResponseWriter, p Problem) {
	if p.Status == 0 {
		p.Status = http.StatusInternalServerError
	}
	if p.Title == "" {
		p.Title = http.StatusText(p.Status)
	}
	b, err := json.Marshal(p)
	if err != nil {
		// Extremely unlikely for a plain struct; fall back to text.
		http.Error(w, p.Title, p.Status)
		return
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_, _ = w.Write(b)
}

// Error writes a minimal problem for the given status and detail.
func Error(w http.ResponseWriter, status int, detail string) {
	WriteProblem(w, Problem{Status: status, Detail: detail})
}
