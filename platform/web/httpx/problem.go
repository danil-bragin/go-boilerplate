// Package httpx provides JSON request decoding+validation and RFC 9457
// (problem+json) error responses for HTTP handlers.
package httpx

import (
	"encoding/json"
	"errors"
	"net/http"

	"go-boilerplate/platform/apperr"
)

// Problem is an RFC 9457 problem detail.
//
// Member names follow RFC 9457 §3.1 exactly: "type", "title", "status",
// "detail" and "instance" are the standard members. "code", "params" and
// "errors" are EXTENSION members (RFC 9457 §3.2): lowercase, chosen not to
// collide with current or future standard member names. Extension members
// are the machine-readable API contract — clients must switch on Code and
// read Params; Title/Detail are human-readable and may be localized.
type Problem struct {
	Type   string `json:"type,omitempty"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	// Instance identifies this specific occurrence — the request path or a
	// request-id URN (RFC 9457 §3.1.5).
	Instance string `json:"instance,omitempty"`

	// Code is the flat UPPER_SNAKE application error code (see
	// platform/apperr). Stable API contract; never localized.
	Code string `json:"code,omitempty"`
	// Params carries the structured parameters of the error (apperr.Params):
	// every variable referenced by the human-readable message appears here so
	// clients can render their own messages (Google AIP-193 rule).
	Params map[string]any `json:"params,omitempty"`
	// Errors carries field-level validation messages (legacy extension kept
	// for backward compatibility; new clients should read Params["fields"]).
	Errors map[string]string `json:"errors,omitempty"`
}

// FromError maps err to a Problem:
//
//   - *apperr.Error in the chain → its Status/Code/Params with the rendered
//     message template as Detail (the wrapped cause is NOT exposed). The
//     platform auth sentinels (authz.ErrForbidden, authz.ErrUnauthenticated,
//     cqrs.ErrUnauthenticated) are apperr errors, so they map to
//     403 AUTH_FORBIDDEN / 401 AUTH_UNAUTHENTICATED through this branch.
//   - *ValidationError (request decode) → 422 VALIDATION_FAILED with the
//     legacy Errors field-map plus Params["fields"] = [{field, rule, param}].
//   - anything else → 500 INTERNAL with no detail (unknown errors must not
//     leak internals to clients).
//
// The caller may set Instance afterwards (WriteError does it from the
// request path).
func FromError(err error) Problem {
	var ae *apperr.Error
	if errors.As(err, &ae) {
		return Problem{
			Status: ae.Status,
			Title:  http.StatusText(ae.Status),
			Detail: ae.Message(),
			Code:   ae.Code,
			Params: ae.Params,
		}
	}

	var ve *ValidationError
	if errors.As(err, &ve) {
		p := problemForCode(apperr.CodeValidationFailed)
		// Decode-level validation keeps the historical 422 (the request was
		// well-formed JSON but semantically invalid) while command-level
		// validation uses the registered 400 via the apperr branch above.
		p.Status = http.StatusUnprocessableEntity
		p.Title = http.StatusText(p.Status)
		p.Errors = ve.Fields
		if len(ve.Details) > 0 {
			fields := make([]map[string]any, len(ve.Details))
			for i, d := range ve.Details {
				fields[i] = map[string]any{"field": d.Field, "rule": d.Rule, "param": d.Param}
			}
			p.Params = map[string]any{"fields": fields}
		}
		return p
	}

	// Unknown error: never leak err.Error() to clients.
	return Problem{
		Status: http.StatusInternalServerError,
		Title:  http.StatusText(http.StatusInternalServerError),
		Code:   apperr.CodeInternal,
	}
}

// problemForCode builds a Problem from a registered apperr code, using the
// registered status and default message.
func problemForCode(code string) Problem {
	status := http.StatusInternalServerError
	detail := ""
	if reg, ok := apperr.Lookup(code); ok {
		status = reg.Status
		detail = reg.Message
	}
	return Problem{
		Status: status,
		Title:  http.StatusText(status),
		Detail: detail,
		Code:   code,
	}
}

// WriteError maps err via FromError, fills Instance from the request path,
// and writes the problem. The standard way for handlers to answer with an
// error:
//
//	if err != nil { httpx.WriteError(w, r, err); return }
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	p := FromError(err)
	if p.Instance == "" && r != nil && r.URL != nil {
		p.Instance = r.URL.Path
	}
	WriteProblem(w, p)
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
