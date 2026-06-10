// Package apperr defines the application error model shared by every layer
// of this repository: a flat UPPER_SNAKE error code, an HTTP status, a
// permanence flag (permanent errors skip retries and go straight to the DLT),
// and structured parameters.
//
// Messages are stored as TEMPLATES with {param} placeholders and rendered
// only at the edges (logs, problem+json detail). Following Google AIP-193,
// every variable referenced by a message template MUST be declared in the
// registration's Params — enforced by a vet-style test over the registry
// (see TestRegistry_MessageTemplateInvariant), not by runtime reflection.
//
// Codes are registered once, from init() of the package that OWNS them
// (services own their ORDERS_*/PAYMENTS_*/… blocks; this package owns only
// the cross-cutting platform codes — see codes.go). Duplicate registration
// panics so collisions surface at process start, not in production traffic.
//
// The package intentionally has ZERO dependencies beyond the standard
// library so that any layer — messaging, storage, domain — can return typed
// errors without import-cycle or weight concerns.
package apperr

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Error is a coded application error. Code/Status/Params/Permanent are
// exported for edge mapping (httpx.FromError, kafka DLT headers); the
// message template and the wrapped cause stay unexported and are rendered
// via Message/Error.
type Error struct {
	// Code is the flat UPPER_SNAKE error code (e.g. "VALIDATION_FAILED").
	Code string
	// Status is the HTTP status the code maps to at the edge.
	Status int
	// Params carries the structured values referenced by the message
	// template and exposed to API clients (problem+json "params" member).
	Params map[string]any
	// Permanent marks errors that no retry can fix: consumers short-circuit
	// straight to the DLT instead of burning retry attempts/tiers.
	Permanent bool

	msg string // message template with {param} placeholders
	err error  // wrapped cause (nil for root errors)
}

// New returns an *Error for a registered code, inheriting status, permanence
// and the default message template from the registry. Unknown codes fall
// back to status 500, transient, with the code itself as the message —
// prefer registering every code from init() of its owning package.
func New(code string) *Error {
	e := &Error{Code: code, Status: 500, msg: code}
	if reg, ok := Lookup(code); ok {
		e.Status = reg.Status
		e.Permanent = reg.Permanent
		e.msg = reg.Message
	}
	return e
}

// Newf is New with an ad-hoc, immediately rendered (fmt-style) message that
// replaces the registered default. Use it for developer detail that has no
// stable parameter schema; structured values belong in WithParam instead.
func Newf(code, format string, args ...any) *Error {
	e := New(code)
	e.msg = fmt.Sprintf(format, args...)
	return e
}

// Wrap returns a coded error wrapping cause (visible to errors.Is/As via
// Unwrap). The message template is the registered default for code.
func Wrap(cause error, code string) *Error {
	e := New(code)
	e.err = cause
	return e
}

// Wrapf is Wrap with an ad-hoc fmt-style message replacing the default.
func Wrapf(cause error, code, format string, args ...any) *Error {
	e := Wrap(cause, code)
	e.msg = fmt.Sprintf(format, args...)
	return e
}

// WithParam returns a copy of e with the parameter added. Copy-on-write so
// package-level sentinel errors can be safely parameterized per call.
func (e *Error) WithParam(key string, value any) *Error {
	return e.WithParams(map[string]any{key: value})
}

// WithParams returns a copy of e with all given parameters merged in.
func (e *Error) WithParams(params map[string]any) *Error {
	clone := *e
	merged := make(map[string]any, len(e.Params)+len(params))
	for k, v := range e.Params {
		merged[k] = v
	}
	for k, v := range params {
		merged[k] = v
	}
	clone.Params = merged
	return &clone
}

// Message renders the message template with the error's params substituted
// for {param} placeholders. Missing params leave the placeholder verbatim
// (so logs show what is absent instead of hiding it).
func (e *Error) Message() string {
	return renderTemplate(e.msg, e.Params)
}

// Error renders "CODE: message[: cause]" for logs.
func (e *Error) Error() string {
	s := e.Code + ": " + e.Message()
	if e.err != nil {
		s += ": " + e.err.Error()
	}
	return s
}

// Unwrap exposes the wrapped cause to errors.Is/As.
func (e *Error) Unwrap() error { return e.err }

// Is reports code equality against another *Error, so
// errors.Is(err, apperr.New(SOME_CODE)) matches any error of that code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// Code extracts the error code from any error: the *Error code when one is
// in the chain, otherwise "INTERNAL" (including for nil — callers should
// check err != nil first).
func Code(err error) string {
	var ae *Error
	if errors.As(err, &ae) {
		return ae.Code
	}
	return CodeInternal
}

// IsPermanent reports whether the chain contains a permanent *Error.
// Permanent errors must not be retried: messaging layers route them straight
// to the dead-letter topic after the first attempt.
func IsPermanent(err error) bool {
	var ae *Error
	return errors.As(err, &ae) && ae.Permanent
}

// renderTemplate substitutes {key} placeholders with fmt.Sprint(params[key]).
func renderTemplate(msg string, params map[string]any) string {
	if !strings.Contains(msg, "{") {
		return msg
	}
	var b strings.Builder
	b.Grow(len(msg))
	for {
		open := strings.IndexByte(msg, '{')
		if open < 0 {
			b.WriteString(msg)
			return b.String()
		}
		closing := strings.IndexByte(msg[open:], '}')
		if closing < 0 {
			b.WriteString(msg)
			return b.String()
		}
		name := msg[open+1 : open+closing]
		b.WriteString(msg[:open])
		if v, ok := params[name]; ok && name != "" {
			fmt.Fprint(&b, v)
		} else {
			b.WriteString(msg[open : open+closing+1])
		}
		msg = msg[open+closing+1:]
	}
}

// TemplateVars returns the sorted, deduplicated {placeholder} names found in
// a message template. Exported for the registry invariant test and for the
// docs generator (W3): template-vars must be a subset of declared Params.
func TemplateVars(msg string) []string {
	var vars []string
	seen := map[string]bool{}
	for {
		open := strings.IndexByte(msg, '{')
		if open < 0 {
			break
		}
		closing := strings.IndexByte(msg[open:], '}')
		if closing < 0 {
			break
		}
		name := msg[open+1 : open+closing]
		if name != "" && !seen[name] {
			seen[name] = true
			vars = append(vars, name)
		}
		msg = msg[open+closing+1:]
	}
	sort.Strings(vars)
	return vars
}

// Registration describes a registered error code: the edge mapping
// (status), retry semantics (permanent), the default developer message
// template, and the parameter names the template may reference.
type Registration struct {
	Status    int
	Permanent bool
	Message   string
	Params    []string
}

var (
	regMu    sync.RWMutex
	registry = map[string]Registration{}
)

// Register records a code with its status, permanence, default message
// template and declared parameter names. Call it from init() of the package
// that owns the code. Panics on duplicate registration so collisions
// surface at startup.
func Register(code string, status int, permanent bool, msg string, params ...string) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, dup := registry[code]; dup {
		panic(fmt.Sprintf("apperr: duplicate registration of code %q", code))
	}
	registry[code] = Registration{Status: status, Permanent: permanent, Message: msg, Params: params}
}

// Lookup returns the Registration for code.
func Lookup(code string) (Registration, bool) {
	regMu.RLock()
	defer regMu.RUnlock()
	reg, ok := registry[code]
	return reg, ok
}

// RegisteredCode is one row of the registry snapshot returned by Registered:
// a code paired with its Registration. Treat the embedded Params slice as
// read-only.
type RegisteredCode struct {
	Code string
	Registration
}

// Registered returns a snapshot of the full registry sorted by code — the
// deterministic input of the docs generator (cmd/errgen → docs/errors.md).
func Registered() []RegisteredCode {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]RegisteredCode, 0, len(registry))
	for c, r := range registry {
		out = append(out, RegisteredCode{Code: c, Registration: r})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}

// Codes returns all registered codes, sorted — the registry snapshot used by
// the template invariant test and the docs generator.
func Codes() []string {
	regMu.RLock()
	defer regMu.RUnlock()
	codes := make([]string, 0, len(registry))
	for c := range registry {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	return codes
}
