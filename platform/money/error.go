package money

import (
	"errors"
	"fmt"
)

// ErrorCode classifies a money error for programmatic handling.
type ErrorCode int

const (
	// CodeCurrencyMismatch is a binary operation on two differing assets (or a
	// zero-value Money operand).
	CodeCurrencyMismatch ErrorCode = iota + 1
	// CodeUnknownAsset is an asset code that is not in the registry.
	CodeUnknownAsset
	// CodeDivByZero is a division by a zero divisor.
	CodeDivByZero
	// CodeInvalidAmount is a nil, empty, or otherwise invalid amount operand.
	CodeInvalidAmount
	// CodeParseFailed is a failure to parse a decimal/amount from its source.
	CodeParseFailed
	// CodeOutOfRange is a value outside an allowed Min/Max/Between bound.
	CodeOutOfRange
	// CodeScaleExceeded is a value whose scale exceeds an allowed maximum.
	CodeScaleExceeded
	// CodeAssetNotAllowed is an asset outside an allow-list.
	CodeAssetNotAllowed
	// CodeNotMultiple is a value that is not a multiple of a required step.
	CodeNotMultiple
	// CodeInvalidRatio is an invalid Split/Allocate ratio.
	CodeInvalidRatio
)

// String returns a short human-readable description of the code.
func (c ErrorCode) String() string {
	switch c {
	case CodeCurrencyMismatch:
		return "currency mismatch"
	case CodeUnknownAsset:
		return "unknown asset"
	case CodeDivByZero:
		return "division by zero"
	case CodeInvalidAmount:
		return "invalid amount"
	case CodeParseFailed:
		return "parse failed"
	case CodeOutOfRange:
		return "out of range"
	case CodeScaleExceeded:
		return "scale exceeded"
	case CodeAssetNotAllowed:
		return "asset not allowed"
	case CodeNotMultiple:
		return "not a multiple"
	case CodeInvalidRatio:
		return "invalid ratio"
	default:
		return "unknown error"
	}
}

// Error is the package's structured error. Code drives handling; the string
// fields carry context; wrapped holds an underlying cause.
type Error struct {
	Code    ErrorCode
	Op      string
	Asset   string
	Asset2  string
	Detail  string
	wrapped error
}

// Error renders the error as "money: <op>: <code> (<assets>): <detail>: <cause>",
// omitting any empty segment.
func (e *Error) Error() string {
	var b []byte
	b = append(b, "money: "...)
	if e.Op != "" {
		b = append(b, e.Op...)
		b = append(b, ": "...)
	}
	b = append(b, e.Code.String()...)
	switch {
	case e.Asset != "" && e.Asset2 != "":
		b = append(b, fmt.Sprintf(" (%s vs %s)", e.Asset, e.Asset2)...)
	case e.Asset != "":
		b = append(b, fmt.Sprintf(" (%s)", e.Asset)...)
	}
	if e.Detail != "" {
		b = append(b, ": "...)
		b = append(b, e.Detail...)
	}
	if e.wrapped != nil {
		b = append(b, ": "...)
		b = append(b, e.wrapped.Error()...)
	}
	return string(b)
}

// Unwrap returns the underlying cause, if any.
func (e *Error) Unwrap() error { return e.wrapped }

// Is matches another *Error by Code, so sentinels (an Error with only Code set)
// work with errors.Is against a fully-populated Error.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return t.Code == e.Code
}

// Sentinels (typed; match by Code via errors.Is).
var (
	// ErrCurrencyMismatch is returned by binary operations on two Money values
	// whose asset codes differ (or when an operand is a zero-value Money).
	ErrCurrencyMismatch = &Error{Code: CodeCurrencyMismatch}
	// ErrUnknownAsset is returned when an asset code is not in the registry.
	ErrUnknownAsset = &Error{Code: CodeUnknownAsset}
	// ErrDivByZero is returned by DivRound when the divisor is zero.
	ErrDivByZero = &Error{Code: CodeDivByZero}
)

// codeErr starts a new *Error with a code and operation name.
func codeErr(code ErrorCode, op string) *Error { return &Error{Code: code, Op: op} }

// withAssets sets the two asset fields and returns the receiver.
func (e *Error) withAssets(a, b string) *Error { e.Asset, e.Asset2 = a, b; return e }

// withAsset sets the single asset field and returns the receiver.
func (e *Error) withAsset(a string) *Error { e.Asset = a; return e }

// withDetail sets the detail field and returns the receiver.
func (e *Error) withDetail(d string) *Error { e.Detail = d; return e }

// wrap sets the underlying cause and returns the receiver.
func (e *Error) wrap(err error) *Error { e.wrapped = err; return e }
