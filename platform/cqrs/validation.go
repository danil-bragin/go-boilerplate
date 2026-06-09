package cqrs

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-playground/validator/v10"
)

// Validatable is a seam for commands that implement their own validation logic.
// If a command type implements this interface, Validation calls Validate()
// instead of falling back to struct-tag validation.
type Validatable interface {
	Validate() error
}

// validate is the shared, package-level validator instance. It is safe for
// concurrent use after construction.
var validate = validator.New(validator.WithRequiredStructEnabled())

// Validation returns a Behavior that validates the command before passing it
// to the next handler. If C implements Validatable, Validate() is called;
// otherwise struct-tag validation is performed (only for struct/pointer-to-struct
// types — non-struct C values are passed through without error). On failure,
// the zero R and a wrapped error are returned and next is not called.
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
				return zero, fmt.Errorf("cqrs: validation: %w", err)
			}
			return next(ctx, cmd)
		}
	}
}
