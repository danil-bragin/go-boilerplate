package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
)

var validate = newValidator()

func newValidator() *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled())
	v.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})
	return v
}

// MaxBodyBytes caps the JSON request body Decode will read (1 MiB).
const MaxBodyBytes int64 = 1 << 20

// ErrUnsupportedMediaType is returned by Decode when the request Content-Type
// is present and is not application/json.
var ErrUnsupportedMediaType = errors.New("httpx: unsupported media type")

// FieldError is one structured validation failure: the JSON field name (the
// json tag when present, the Go field name otherwise), the validator rule
// that failed ("required", "min", …) and the rule's parameter ("1" for
// min=1; empty for parameterless rules).
type FieldError struct {
	Field string
	Rule  string
	Param string
}

// ValidationError reports field-level validation failures. Fields is the
// legacy human-readable map (kept for backward compatibility); Details
// carries the structured per-field data used for Problem.Params["fields"].
type ValidationError struct {
	Fields  map[string]string
	Details []FieldError
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed: %v", e.Fields)
}

// Decode reads a JSON body of type T from r and validates it via struct tags.
// It rejects unknown fields. Returns *ValidationError on validation failure.
func Decode[T any](r *http.Request) (T, error) {
	var v T

	// FIX 1 — guard nil / empty body before touching it.
	if r.Body == nil || r.Body == http.NoBody {
		return v, errors.New("httpx: decode json: empty request body")
	}

	// FIX 4 — Content-Type enforcement (lenient when header is absent).
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mediaType, _, err := mime.ParseMediaType(ct)
		if err != nil || mediaType != "application/json" {
			return v, ErrUnsupportedMediaType
		}
	}

	// FIX 3 — cap body size to guard against DoS.
	limited := http.MaxBytesReader(nil, r.Body, MaxBodyBytes)
	dec := json.NewDecoder(limited)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return v, fmt.Errorf("httpx: decode json: %w", err)
	}

	// FIX 2 — reject trailing data after the JSON value.
	if dec.More() {
		return v, errors.New("httpx: decode json: unexpected trailing data after JSON value")
	}

	if err := validate.Struct(v); err != nil {
		var verrs validator.ValidationErrors
		if errors.As(err, &verrs) {
			fields := make(map[string]string, len(verrs))
			details := make([]FieldError, 0, len(verrs))
			for _, fe := range verrs {
				fields[fe.Field()] = fmt.Sprintf("failed on '%s'", fe.Tag())
				details = append(details, FieldError{Field: fe.Field(), Rule: fe.Tag(), Param: fe.Param()})
			}
			return v, &ValidationError{Fields: fields, Details: details}
		}
		return v, fmt.Errorf("httpx: validate: %w", err)
	}
	return v, nil
}

// WriteDecodeError maps a decode/validation error to an appropriate problem.
func WriteDecodeError(w http.ResponseWriter, err error) {
	var ve *ValidationError
	if errors.As(err, &ve) {
		// FromError keeps the historical 422 + Errors map and adds the
		// VALIDATION_FAILED code with Params["fields"].
		WriteProblem(w, FromError(ve))
		return
	}
	if errors.Is(err, ErrUnsupportedMediaType) {
		WriteProblem(w, Problem{
			Status: http.StatusUnsupportedMediaType,
			Title:  http.StatusText(http.StatusUnsupportedMediaType),
		})
		return
	}
	Error(w, http.StatusBadRequest, "invalid request body")
}
