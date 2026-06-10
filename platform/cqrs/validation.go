package cqrs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"go-boilerplate/platform/apperr"

	"github.com/go-playground/validator/v10"
)

// Validatable is a seam for commands that implement their own validation logic.
// If a command type implements this interface, Validation calls Validate()
// instead of falling back to struct-tag validation.
type Validatable interface {
	Validate() error
}

// validate is the shared, package-level validator instance. It is safe for
// concurrent use after construction. Field names in validation errors use
// the json tag when present (`json:"-"` and untagged fields fall back to the
// Go field name) so the structured params match the wire contract clients
// actually send.
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

// Validation returns a Behavior that validates the command before passing it
// to the next handler. If C implements Validatable, Validate() is called;
// otherwise struct-tag validation is performed (only for struct/pointer-to-struct
// types — non-struct C values are passed through without error). On failure,
// the zero R and an error are returned and next is not called.
//
// Struct-tag failures are typed: a permanent apperr VALIDATION_FAILED with
// Params["fields"] = [{field, rule, param}] (field = json tag when present,
// Go name otherwise), wrapping the raw validator.ValidationErrors. Permanence
// matters in consumers: kafka retry layers route the error straight to the
// DLT instead of burning retries on an unfixable payload. Validatable errors
// pass through as before — domains may return their own apperr codes there.
//
// Nil-pointer commands: if C is a pointer type and the command is nil, the
// validator will return an error (not a panic); callers should ensure commands
// are non-nil before dispatching.
func Validation[C, R any]() Behavior[C, R] {
	return func(next HandlerFunc[C, R]) HandlerFunc[C, R] {
		return func(ctx context.Context, cmd C) (R, error) {
			var zero R

			// Check Validatable seam first.
			if v, ok := any(cmd).(Validatable); ok {
				if err := v.Validate(); err != nil {
					return zero, fmt.Errorf("cqrs: validation: %w", err)
				}
				return next(ctx, cmd)
			}

			// Fall back to struct-tag validation only for struct/pointer-to-struct.
			t := reflect.TypeOf(cmd)
			if t == nil {
				return next(ctx, cmd)
			}
			kind := t.Kind()
			if kind == reflect.Pointer {
				kind = t.Elem().Kind()
			}
			if kind != reflect.Struct {
				return next(ctx, cmd)
			}

			if err := validate.Struct(cmd); err != nil {
				return zero, validationAppErr(err)
			}
			return next(ctx, cmd)
		}
	}
}

// validationAppErr converts a struct-tag validation failure into a permanent
// apperr VALIDATION_FAILED carrying structured per-field params, keeping the
// raw validator error in the chain for callers that inspect it.
func validationAppErr(err error) error {
	var verrs validator.ValidationErrors
	if !errors.As(err, &verrs) {
		// InvalidValidationError (nil pointer command etc.) — not a
		// field-level failure; keep the historical wrapping.
		return fmt.Errorf("cqrs: validation: %w", err)
	}
	fields := make([]map[string]any, len(verrs))
	for i, fe := range verrs {
		fields[i] = map[string]any{"field": fe.Field(), "rule": fe.Tag(), "param": fe.Param()}
	}
	return apperr.Wrap(err, apperr.CodeValidationFailed).WithParam("fields", fields)
}
