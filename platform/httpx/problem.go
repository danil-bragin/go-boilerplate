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
func WriteProblem(w http.ResponseWriter, p Problem) {
	if p.Status == 0 {
		p.Status = http.StatusInternalServerError
	}
	if p.Title == "" {
		p.Title = http.StatusText(p.Status)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// Error writes a minimal problem for the given status and detail.
func Error(w http.ResponseWriter, status int, detail string) {
	WriteProblem(w, Problem{Status: status, Detail: detail})
}
