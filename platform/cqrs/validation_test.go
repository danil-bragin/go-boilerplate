package cqrs_test

import (
	"context"
	"errors"
	"testing"

	"go-boilerplate/platform/apperr"
	"go-boilerplate/platform/cqrs"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type taggedCmd struct {
	Amount   int    `json:"amount_cents" validate:"min=1"`
	Customer string `json:"customer_id"  validate:"required"`
	NoTag    string `validate:"required"`
}

func TestValidation_TagFailure_ReturnsAppErr(t *testing.T) {
	handler := func(_ context.Context, _ taggedCmd) (string, error) { return "ok", nil }
	decorated := cqrs.Decorate(handler, cqrs.Validation[taggedCmd, string]())

	_, err := decorated(context.Background(), taggedCmd{})
	require.Error(t, err)

	// Typed: VALIDATION_FAILED, permanent (no retry can fix the payload).
	var ae *apperr.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, apperr.CodeValidationFailed, ae.Code)
	assert.Equal(t, 400, ae.Status)
	assert.True(t, apperr.IsPermanent(err))

	// Params carry structured per-field failures; field names use the json
	// tag when present, the Go field name otherwise.
	fields, ok := ae.Params["fields"].([]map[string]any)
	require.True(t, ok, "Params[fields] must be []map[string]any, got %T", ae.Params["fields"])
	byField := map[string]map[string]any{}
	for _, f := range fields {
		byField[f["field"].(string)] = f
	}
	require.Len(t, byField, 3)
	assert.Equal(t, map[string]any{"field": "amount_cents", "rule": "min", "param": "1"}, byField["amount_cents"])
	assert.Equal(t, map[string]any{"field": "customer_id", "rule": "required", "param": ""}, byField["customer_id"])
	assert.Equal(t, map[string]any{"field": "NoTag", "rule": "required", "param": ""}, byField["NoTag"])

	// The raw validator error stays reachable for callers that inspect it.
	var verrs validator.ValidationErrors
	assert.True(t, errors.As(err, &verrs), "validator.ValidationErrors must remain in the chain")
}

func TestValidation_TagFailure_JSONDashFallsBackToGoName(t *testing.T) {
	type dashCmd struct {
		Hidden string `json:"-" validate:"required"`
	}
	handler := func(_ context.Context, _ dashCmd) (string, error) { return "ok", nil }
	decorated := cqrs.Decorate(handler, cqrs.Validation[dashCmd, string]())

	_, err := decorated(context.Background(), dashCmd{})
	var ae *apperr.Error
	require.ErrorAs(t, err, &ae)
	fields := ae.Params["fields"].([]map[string]any)
	require.Len(t, fields, 1)
	assert.Equal(t, "Hidden", fields[0]["field"], `json:"-" must fall back to the Go field name`)
}

func TestValidation_ValidatableError_NotConverted(t *testing.T) {
	// The Validatable escape returns the command's own error (wrapped) —
	// domains may return their own apperr codes there; cqrs must not stamp
	// VALIDATION_FAILED over them.
	handler := func(_ context.Context, _ failingValidatable) (string, error) { return "ok", nil }
	decorated := cqrs.Decorate(handler, cqrs.Validation[failingValidatable, string]())

	_, err := decorated(context.Background(), failingValidatable{})
	require.Error(t, err)
	require.ErrorIs(t, err, errCustomValidate)
	assert.False(t, apperr.IsPermanent(err), "Validatable errors pass through untyped")
}

type failingValidatable struct{}

var errCustomValidate = errors.New("custom validate failed")

func (failingValidatable) Validate() error { return errCustomValidate }
