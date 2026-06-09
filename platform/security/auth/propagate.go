package auth

import (
	"context"
	"strings"
)

// Kafka record header keys used to propagate the authenticated principal
// from an edge service (which verified the JWT) to downstream consumers.
const (
	// HeaderPrincipalSub carries Principal.Subject.
	HeaderPrincipalSub = "principal-sub"
	// HeaderPrincipalRoles carries Principal.Roles as a comma-separated list.
	HeaderPrincipalRoles = "principal-roles"
)

// InjectHeaders copies the principal from ctx into the given header map
// (principal-sub + principal-roles). No-op when ctx carries no principal.
// Producers call this on command/event records so downstream audit trails
// can attribute the action to the real actor instead of "anonymous".
//
// SECURITY NOTE: these headers are transport metadata, NOT authentication.
// Any client that can produce to the topic can forge them. The trust
// boundary is the Kafka cluster itself: restrict produce rights with broker
// ACLs and authenticate inter-service connections (mTLS/SASL) so only
// trusted services can write to command/event topics. Never use these
// headers to make authorization decisions on data that arrived from an
// untrusted producer.
func InjectHeaders(ctx context.Context, headers map[string]string) {
	p, ok := From(ctx)
	if !ok || headers == nil {
		return
	}
	headers[HeaderPrincipalSub] = p.Subject
	if len(p.Roles) > 0 {
		headers[HeaderPrincipalRoles] = strings.Join(p.Roles, ",")
	}
}

// ExtractToContext installs a Principal built from the propagation headers
// (see InjectHeaders) into ctx. When no principal-sub header is present, ctx
// is returned unchanged. Consumer pipelines call this before dispatching to
// handlers so behaviors such as audit logging see the original actor.
//
// The same SECURITY NOTE as InjectHeaders applies: the extracted principal
// reflects what the producer CLAIMED. It is trustworthy only as far as the
// broker's ACL/mTLS perimeter guarantees that producers are trusted services.
func ExtractToContext(ctx context.Context, headers map[string]string) context.Context {
	sub := headers[HeaderPrincipalSub]
	if sub == "" {
		return ctx
	}
	var roles []string
	if raw := headers[HeaderPrincipalRoles]; raw != "" {
		for _, r := range strings.Split(raw, ",") {
			if r = strings.TrimSpace(r); r != "" {
				roles = append(roles, r)
			}
		}
	}
	return Into(ctx, Principal{Subject: sub, Roles: roles})
}
