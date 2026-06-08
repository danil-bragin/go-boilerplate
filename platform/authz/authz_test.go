package authz_test

import (
	"context"
	"errors"
	"testing"

	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/authz"
	"go-boilerplate/platform/cqrs"
)

// ---- RBAC tests ----

func TestRBAC_AllowsWithMatchingRole(t *testing.T) {
	rbac := authz.NewRBAC(map[string][]string{
		"order:create": {"admin", "ops"},
	})
	p := auth.Principal{Roles: []string{"ops"}}
	if err := rbac.Authorize(p, "order:create"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestRBAC_DeniesWithoutRole(t *testing.T) {
	rbac := authz.NewRBAC(map[string][]string{
		"order:create": {"admin", "ops"},
	})
	p := auth.Principal{Roles: []string{"viewer"}}
	err := rbac.Authorize(p, "order:create")
	if !errors.Is(err, authz.ErrForbidden) {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
}

func TestRBAC_DeniesUnknownAction(t *testing.T) {
	rbac := authz.NewRBAC(map[string][]string{
		"order:create": {"admin"},
	})
	p := auth.Principal{Roles: []string{"admin"}}
	err := rbac.Authorize(p, "order:delete")
	if !errors.Is(err, authz.ErrForbidden) {
		t.Fatalf("expected ErrForbidden for unknown action, got %v", err)
	}
}

// ---- Require behavior tests ----

type Cmd struct{ Value string }
type Res struct{ Value string }

func TestRequire_PassesWithPrincipalAndRole(t *testing.T) {
	rbac := authz.NewRBAC(map[string][]string{
		"order:create": {"admin", "ops"},
	})

	called := 0
	handler := cqrs.HandlerFunc[Cmd, Res](func(_ context.Context, cmd Cmd) (Res, error) {
		called++
		return Res(cmd), nil
	})

	decorated := cqrs.Decorate(handler, authz.Require[Cmd, Res](rbac, "order:create"))

	ctx := auth.Into(context.Background(), auth.Principal{Roles: []string{"admin"}})
	res, err := decorated(ctx, Cmd{Value: "hello"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if res.Value != "hello" {
		t.Fatalf("expected result Value=hello, got %q", res.Value)
	}
	if called != 1 {
		t.Fatalf("expected handler called once, got %d", called)
	}
}

func TestRequire_ForbiddenWithoutRole(t *testing.T) {
	rbac := authz.NewRBAC(map[string][]string{
		"order:create": {"admin", "ops"},
	})

	called := 0
	handler := cqrs.HandlerFunc[Cmd, Res](func(_ context.Context, _ Cmd) (Res, error) {
		called++
		return Res{}, nil
	})

	decorated := cqrs.Decorate(handler, authz.Require[Cmd, Res](rbac, "order:create"))

	ctx := auth.Into(context.Background(), auth.Principal{Roles: []string{"viewer"}})
	_, err := decorated(ctx, Cmd{})
	if !errors.Is(err, authz.ErrForbidden) {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
	if called != 0 {
		t.Fatalf("handler must not be called, but was called %d time(s)", called)
	}
}

func TestRequire_UnauthenticatedWithoutPrincipal(t *testing.T) {
	rbac := authz.NewRBAC(map[string][]string{
		"order:create": {"admin"},
	})

	called := 0
	handler := cqrs.HandlerFunc[Cmd, Res](func(_ context.Context, _ Cmd) (Res, error) {
		called++
		return Res{}, nil
	})

	decorated := cqrs.Decorate(handler, authz.Require[Cmd, Res](rbac, "order:create"))

	_, err := decorated(context.Background(), Cmd{})
	if !errors.Is(err, authz.ErrUnauthenticated) {
		t.Fatalf("expected ErrUnauthenticated, got %v", err)
	}
	if called != 0 {
		t.Fatalf("handler must not be called, but was called %d time(s)", called)
	}
}
