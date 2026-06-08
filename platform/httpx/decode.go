package httpx

import (
	"encoding/json"
	"fmt"
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

// ValidationError reports field-level validation failures.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed: %v", e.Fields)
}

// Decode reads a JSON body of type T from r and validates it via struct tags.
// It rejects unknown fields. Returns *ValidationError on validation failure.
func Decode[T any](r *http.Request) (T, error) {
	var v T
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return v, fmt.Errorf("httpx: decode json: %w", err)
	}

	if err := validate.Struct(v); err != nil {
		var verrs validator.ValidationErrors
		if ok := asValidationErrors(err, &verrs); ok {
			fields := make(map[string]string, len(verrs))
			for _, fe := range verrs {
				fields[fe.Field()] = fmt.Sprintf("failed on '%s'", fe.Tag())
			}
			return v, &ValidationError{Fields: fields}
		}
		return v, fmt.Errorf("httpx: validate: %w", err)
	}
	return v, nil
}

func asValidationErrors(err error, target *validator.ValidationErrors) bool {
	if verrs, ok := err.(validator.ValidationErrors); ok {
		*target = verrs
		return true
	}
	return false
}

// WriteDecodeError maps a decode/validation error to an appropriate problem.
func WriteDecodeError(w http.ResponseWriter, err error) {
	var ve *ValidationError
	if asValidation(err, &ve) {
		WriteProblem(w, Problem{
			Status: http.StatusUnprocessableEntity,
			Title:  "Validation Failed",
			Errors: ve.Fields,
		})
		return
	}
	Error(w, http.StatusBadRequest, "invalid request body")
}

func asValidation(err error, target **ValidationError) bool {
	for err != nil {
		if ve, ok := err.(*ValidationError); ok {
			*target = ve
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
