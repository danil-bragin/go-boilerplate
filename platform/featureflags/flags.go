// Package featureflags wraps the OpenFeature Go SDK to provide a thin, type-safe
// feature-flag client that can be backed by any OpenFeature provider.
//
// # Quick start (in-memory, tests/examples)
//
//	flags, err := featureflags.NewInMemory("my-app", map[string]inmemory.InMemoryFlag{
//	    "new-checkout": {
//	        State:          inmemory.Enabled,
//	        DefaultVariant: "on",
//	        Variants:       map[string]any{"on": true, "off": false},
//	    },
//	})
//
// # Production — swap the provider
//
// Replace the in-memory provider with flagd, LaunchDarkly, or any compliant
// OpenFeature provider by calling openfeature.SetProviderAndWait (optionally
// scoped with openfeature.WithDomain) before constructing the client:
//
//	_ = openfeature.SetProviderAndWait(ctx, flagdProvider, openfeature.WithDomain("my-app"))
//	client := openfeature.NewClient(openfeature.WithDomain("my-app"))
//	flags  := featureflags.New(client)
//
// # Provider isolation
//
// This package uses the domain API (openfeature.WithDomain on both provider
// registration and client construction) so that different callers — and
// individual tests — can register independent providers without interfering
// with each other or with the global default.
package featureflags

import (
	"context"
	"errors"
	"fmt"

	"go.openfeature.dev/openfeature/v2"
	"go.openfeature.dev/openfeature/v2/providers/inmemory"
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
// the given domain. Each call registers an independent domain-scoped provider
// so that concurrent tests do not share state.
//
// domain must be a non-empty string that is unique within the process (e.g. a
// test name). The inmemory flag map keys are flag names; see
// inmemory.InMemoryFlag for the full flag definition.
func NewInMemory(domain string, flags map[string]inmemory.InMemoryFlag) (*Flags, error) {
	if domain == "" {
		return nil, errors.New("featureflags: domain must not be empty")
	}

	p := inmemory.NewProvider(flags)

	// SetProviderAndWait blocks until the provider transitions out of
	// NOT_READY, which avoids a race between registration and the first
	// evaluation. The in-memory provider initialises instantly, so a
	// background context is sufficient here.
	if err := openfeature.SetProviderAndWait(context.Background(), p, openfeature.WithDomain(domain)); err != nil {
		return nil, fmt.Errorf("featureflags: register provider for domain %q: %w", domain, err)
	}

	return &Flags{client: openfeature.NewClient(openfeature.WithDomain(domain))}, nil
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
