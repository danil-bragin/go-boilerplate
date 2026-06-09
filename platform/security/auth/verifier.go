package auth

import (
	"context"
	"errors"
)

// ErrInvalidToken is returned when a token cannot be verified (expired,
// tampered, wrong issuer/audience, etc.).
var ErrInvalidToken = errors.New("auth: invalid token")

// Verifier is the pluggable interface for JWT verification.
// Any IdP can be supported by providing a different implementation.
type Verifier interface {
	Verify(ctx context.Context, rawToken string) (Principal, error)
}
