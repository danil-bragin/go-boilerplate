// Package featureflags wraps the OpenFeature Go SDK to provide a thin, type-safe
// feature-flag client that can be backed by any OpenFeature provider.
//
// # Quick start (in-memory, tests/examples)
//
//	flags, err := featureflags.NewInMemory("my-app", map[string]memprovider.InMemoryFlag{
//	    "new-checkout": {
//	        State:          memprovider.Enabled,
//	        DefaultVariant: "on",
//	        Variants:       map[string]any{"on": true, "off": false},
//	    },
//	})
//
// # Production — swap the provider
//
// Replace the in-memory provider with flagd, LaunchDarkly, or any compliant
// OpenFeature provider by calling openfeature.SetNamedProvider (or the global
// openfeature.SetProvider) before constructing the client:
//
//	_ = openfeature.SetNamedProviderAndWait("my-app", flagdProvider)
//	client := openfeature.NewClient("my-app")
//	flags  := featureflags.New(client)
//
// # Provider isolation
//
// This package uses the named-provider / domain API
// (openfeature.SetNamedProvider + openfeature.NewClient(domain)) so that
// different callers — and individual tests — can register independent
// providers without interfering with each other or with the global default.
package featureflags

import (
	"context"
	"fmt"

	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
)

// Flags is a thin, type-safe wrapper around an OpenFeature client.
type Flags struct {
	client *openfeature.Client
}

// New wraps an existing *openfeature.Client. The caller is responsible for
// registering (and waiting for) the backing provider before using the client.
func New(client *openfeature.Client) *Flags {
	return &Flags{client: client}
}

// NewInMemory creates a *Flags backed by an in-memory provider registered under
// the given domain. Each call registers an independent named provider so that
// concurrent tests do not share state.
//
// domain must be a non-empty string that is unique within the process (e.g. a
// test name). The memprovider flag map keys are flag names; see
// memprovider.InMemoryFlag for the full flag definition.
func NewInMemory(domain string, flags map[string]memprovider.InMemoryFlag) (*Flags, error) {
	if domain == "" {
		return nil, fmt.Errorf("featureflags: domain must not be empty")
	}

	p := memprovider.NewInMemoryProvider(flags)

	// SetNamedProviderAndWait blocks until the provider transitions out of
	// NOT_READY, which avoids a race between registration and the first evaluation.
	if err := openfeature.SetNamedProviderAndWait(domain, p); err != nil {
		return nil, fmt.Errorf("featureflags: register named provider %q: %w", domain, err)
	}

	return &Flags{client: openfeature.NewClient(domain)}, nil
}

// Bool evaluates a boolean feature flag. Returns def if the flag is not found
// or an error occurs during evaluation.
func (f *Flags) Bool(ctx context.Context, key string, def bool) bool {
	return f.client.Boolean(ctx, key, def, openfeature.EvaluationContext{})
}

// String evaluates a string feature flag. Returns def if the flag is not found
// or an error occurs during evaluation.
func (f *Flags) String(ctx context.Context, key string, def string) string {
	return f.client.String(ctx, key, def, openfeature.EvaluationContext{})
}

// Int evaluates an integer feature flag. Returns def if the flag is not found
// or an error occurs during evaluation.
func (f *Flags) Int(ctx context.Context, key string, def int64) int64 {
	return f.client.Int(ctx, key, def, openfeature.EvaluationContext{})
}
