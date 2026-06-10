package apperr_test

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"go-boilerplate/platform/apperr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test-only codes. Registered once for the whole test binary; production
// code must register codes from init() of the package that owns them.
func init() {
	apperr.Register("TEST_NOT_FOUND", 404, true, "thing {id} not found", "id")
	apperr.Register("TEST_TRANSIENT", 503, false, "downstream unavailable")
	apperr.Register("TEST_TWO_PARAMS", 409, true, "conflict between {a} and {b}", "a", "b")
}

func TestNew_FillsFromRegistry(t *testing.T) {
	e := apperr.New("TEST_NOT_FOUND").WithParam("id", "42")

	assert.Equal(t, "TEST_NOT_FOUND", e.Code)
	assert.Equal(t, 404, e.Status)
	assert.True(t, e.Permanent)
	assert.Equal(t, map[string]any{"id": "42"}, e.Params)
	assert.Equal(t, "thing 42 not found", e.Message())
	assert.Equal(t, "TEST_NOT_FOUND: thing 42 not found", e.Error())
}

func TestNew_UnknownCode_Defaults(t *testing.T) {
	e := apperr.New("TEST_NEVER_REGISTERED")

	assert.Equal(t, "TEST_NEVER_REGISTERED", e.Code)
	assert.Equal(t, 500, e.Status)
	assert.False(t, e.Permanent)
	assert.Equal(t, "TEST_NEVER_REGISTERED", e.Message())
}

func TestNewf_RendersImmediately(t *testing.T) {
	e := apperr.Newf("TEST_TRANSIENT", "shard %d down", 7)

	assert.Equal(t, 503, e.Status)
	assert.Equal(t, "shard 7 down", e.Message())
}

func TestMessage_MissingParamKeepsPlaceholder(t *testing.T) {
	e := apperr.New("TEST_TWO_PARAMS").WithParam("a", "x")
	assert.Equal(t, "conflict between x and {b}", e.Message())
}

func TestWrap_UnwrapAndErrorsIs(t *testing.T) {
	cause := errors.New("row not found")
	e := apperr.Wrap(cause, "TEST_NOT_FOUND").WithParam("id", "9")

	require.ErrorIs(t, e, cause, "errors.Is must see the wrapped cause")
	assert.Equal(t, "TEST_NOT_FOUND: thing 9 not found: row not found", e.Error())

	// errors.As extracts the *apperr.Error through further wrapping.
	wrapped := fmt.Errorf("consume: process: %w", e)
	var ae *apperr.Error
	require.ErrorAs(t, wrapped, &ae)
	assert.Equal(t, "TEST_NOT_FOUND", ae.Code)
}

func TestIs_MatchesByCode(t *testing.T) {
	e := fmt.Errorf("outer: %w", apperr.New("TEST_NOT_FOUND").WithParam("id", "1"))
	assert.True(t, errors.Is(e, apperr.New("TEST_NOT_FOUND")))
	assert.False(t, errors.Is(e, apperr.New("TEST_TRANSIENT")))
}

func TestWithParam_DoesNotMutateOriginal(t *testing.T) {
	base := apperr.New("TEST_TWO_PARAMS").WithParam("a", "x")
	derived := base.WithParam("b", "y")

	assert.NotContains(t, base.Params, "b", "WithParam must copy-on-write")
	assert.Equal(t, map[string]any{"a": "x", "b": "y"}, derived.Params)
}

func TestWithParams_MergesCopy(t *testing.T) {
	e := apperr.New("TEST_TWO_PARAMS").
		WithParams(map[string]any{"a": 1, "b": 2})
	assert.Equal(t, map[string]any{"a": 1, "b": 2}, e.Params)
}

func TestCode_Helper(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil-safe apperr", apperr.New("TEST_TRANSIENT"), "TEST_TRANSIENT"},
		{"wrapped apperr", fmt.Errorf("x: %w", apperr.New("TEST_NOT_FOUND")), "TEST_NOT_FOUND"},
		{"plain error", errors.New("boom"), "INTERNAL"},
		{"nil", nil, "INTERNAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, apperr.Code(tt.err))
		})
	}
}

func TestIsPermanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"permanent apperr", apperr.New("TEST_NOT_FOUND"), true},
		{"transient apperr", apperr.New("TEST_TRANSIENT"), false},
		{"wrapped permanent", fmt.Errorf("x: %w", apperr.New("TEST_NOT_FOUND")), true},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, apperr.IsPermanent(tt.err))
		})
	}
}

func TestRegister_DuplicatePanics(t *testing.T) {
	assert.Panics(t, func() {
		apperr.Register("TEST_NOT_FOUND", 404, true, "dup")
	})
}

func TestPlatformCodes_Registered(t *testing.T) {
	tests := []struct {
		code      string
		status    int
		permanent bool
	}{
		{apperr.CodeInternal, 500, false},
		{apperr.CodeValidationFailed, 400, true},
		{apperr.CodeAuthUnauthenticated, 401, true},
		{apperr.CodeAuthForbidden, 403, true},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			reg, ok := apperr.Lookup(tt.code)
			require.True(t, ok, "platform code %s must be registered", tt.code)
			assert.Equal(t, tt.status, reg.Status)
			assert.Equal(t, tt.permanent, reg.Permanent)
		})
	}
}

// TestRegistered_SortedSnapshot pins the snapshot API used by cmd/errgen:
// one entry per registered code, sorted by code, each entry equal to what
// Lookup returns for that code.
func TestRegistered_SortedSnapshot(t *testing.T) {
	snap := apperr.Registered()
	require.Len(t, snap, len(apperr.Codes()))
	require.True(t, sort.SliceIsSorted(snap, func(i, j int) bool {
		return snap[i].Code < snap[j].Code
	}), "Registered() must be sorted by code")
	for _, e := range snap {
		reg, ok := apperr.Lookup(e.Code)
		require.True(t, ok, "snapshot code %s must be in the registry", e.Code)
		assert.Equal(t, reg, e.Registration, "snapshot entry for %s", e.Code)
	}
}

// TestRegistry_MessageTemplateInvariant enforces the AIP-193-style rule for
// EVERY registered code (platform and test alike): every {placeholder} in the
// default message template must be declared in the registration's Params.
// Service packages registering codes in init() get this check for free as
// long as their test binary (or this one, via import) registers them.
func TestRegistry_MessageTemplateInvariant(t *testing.T) {
	for _, code := range apperr.Codes() {
		reg, ok := apperr.Lookup(code)
		require.True(t, ok)
		declared := make(map[string]bool, len(reg.Params))
		for _, p := range reg.Params {
			declared[p] = true
		}
		for _, v := range apperr.TemplateVars(reg.Message) {
			assert.True(t, declared[v],
				"code %s: template variable {%s} not declared in Params %v",
				code, v, reg.Params)
		}
	}
}

func TestTemplateVars(t *testing.T) {
	tests := []struct {
		msg  string
		want []string
	}{
		{"no vars", nil},
		{"one {a} var", []string{"a"}},
		{"{a} and {b_2}", []string{"a", "b_2"}},
		{"unclosed {a", nil},
		{"empty {} braces", nil},
		{"{a}{a} dedup", []string{"a"}},
	}
	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			assert.Equal(t, tt.want, apperr.TemplateVars(tt.msg))
		})
	}
}
