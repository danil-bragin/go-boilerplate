package auth

import (
	"context"
	"encoding/json"
	"strings"
)

// Kafka record header keys used to propagate the authenticated principal
// from an edge service (which verified the JWT) to downstream consumers.
const (
	// HeaderPrincipalSub carries Principal.Subject.
	HeaderPrincipalSub = "principal-sub"
	// HeaderPrincipalRoles carries Principal.Roles as a JSON-encoded string
	// array (lossless: roles containing commas or whitespace round-trip
	// exactly). Older producers comma-joined the roles; ExtractToContext
	// still accepts that legacy form for mixed-version rollouts.
	HeaderPrincipalRoles = "principal-roles"
)

// InjectHeaders copies the principal from ctx into the given header map
// (principal-sub + principal-roles). No-op when ctx carries no principal.
// Producers call this on command/event records so downstream audit trails
// can attribute the action to the real actor instead of "anonymous".
//
// Roles are JSON-encoded so that a role containing a comma cannot be split
// into phantom roles at the consumer.
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
		// Marshalling a []string cannot fail; the error branch is untestable
		// and deliberately ignored.
		b, err := json.Marshal(p.Roles)
		if err == nil {
			headers[HeaderPrincipalRoles] = string(b)
		}
	}
}

// ExtractToContext installs a Principal built from the propagation headers
// (see InjectHeaders) into ctx. When no principal-sub header is present, ctx
// is returned unchanged. Consumer pipelines call this before dispatching to
// handlers so behaviors such as audit logging see the original actor.
//
// Roles are read as a JSON string array; a value that is not valid JSON is
// treated as the legacy comma-separated form (records produced by an older
// service version during a mixed-version rollout).
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
		if err := json.Unmarshal([]byte(raw), &roles); err != nil {
			// Legacy producer: comma-joined roles.
			roles = nil
			for _, r := range strings.Split(raw, ",") {
				if r = strings.TrimSpace(r); r != "" {
					roles = append(roles, r)
				}
			}
		}
	}
	return Into(ctx, Principal{Subject: sub, Roles: roles})
}
