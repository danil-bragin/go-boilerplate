package fakes

import (
	"context"

	"go-boilerplate/platform/security/auth"
)

// Verifier is an in-memory fake implementing auth.Verifier.
//
// By default Verify returns the Principal stored in the Principal field.
// Set RejectToken to true to make Verify return auth.ErrInvalidToken.
// An empty rawToken always returns auth.ErrInvalidToken regardless of
// RejectToken.
type Verifier struct {
	Principal   auth.Principal
	RejectToken bool
}

var _ auth.Verifier = (*Verifier)(nil)

// NewVerifier returns a *Verifier pre-configured with a sensible default
// principal (Subject: "test-subject", Username: "test", Roles: ["user"]).
func NewVerifier() *Verifier {
	return &Verifier{
		Principal: auth.Principal{
			Subject:  "test-subject",
			Username: "test",
			Roles:    []string{"user"},
		},
	}
}

// Verify returns the configured Principal, or auth.ErrInvalidToken when
// rawToken is empty or RejectToken is true.
func (v *Verifier) Verify(_ context.Context, rawToken string) (auth.Principal, error) {
	if rawToken == "" || v.RejectToken {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return v.Principal, nil
}
