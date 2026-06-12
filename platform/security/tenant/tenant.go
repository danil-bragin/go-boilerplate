// Package tenant carries a tenant identifier through context and propagates it
// across the Kafka transport, mirroring the principal-propagation model in
// platform/security/auth and the chain-lineage model in
// platform/messaging/msgctx.
//
// The flow is:
//
//   - An edge service resolves the tenant from the verified JWT principal
//     (Middleware reads a configurable claim, e.g. "tenant" / "org_id") and
//     installs it into the request context.
//   - Command/event producers call InjectHeaders so downstream consumers can
//     re-establish the tenant; the shared consumer pipeline
//     (platform/messaging/consume) calls ExtractToContext before dispatching a
//     typed handler, and outbox.Repository.Enqueue stamps the tenant onto
//     outgoing messages automatically.
//   - Tenant-scoped command handlers wrap themselves in the Require behavior so
//     a missing tenant fails closed with apperr.CodeTenantRequired.
//
// SECURITY NOTE: the tenant-id Kafka header is transport metadata, NOT
// authentication — any client that can produce to the topic can forge it. The
// trust boundary is the broker ACL / mTLS-SASL perimeter, exactly as for the
// principal headers (see auth.InjectHeaders). At the HTTP edge the tenant is
// sourced from the cryptographically verified JWT principal, not from a client
// header, so the edge value is trustworthy; never make an isolation decision
// from the propagated header for data that may originate outside the perimeter.
package tenant

import "context"

// HeaderTenantID is the Kafka/outbox record header carrying the tenant id.
const HeaderTenantID = "tenant-id"

// Context key uses the empty-struct style shared across the repo (auth, log,
// pg, msgctx): a distinct unexported type costs zero bytes and cannot collide.
type ctxKey struct{}

// WithContext returns a ctx carrying the tenant id. An empty id is treated as
// "no tenant": it is not stored, so FromContext still reports absence.
func WithContext(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext returns the tenant id and true when one is set, or ("", false)
// when the context carries no tenant.
func FromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(ctxKey{}).(string)
	return id, ok && id != ""
}

// InjectHeaders copies the tenant id from ctx into the given header map. No-op
// when ctx carries no tenant or headers is nil. Producers call this on
// command/event records so downstream consumers can re-scope to the tenant.
//
// See the package SECURITY NOTE: this header is transport metadata, not a
// trust assertion.
func InjectHeaders(ctx context.Context, headers map[string]string) {
	id, ok := FromContext(ctx)
	if !ok || headers == nil {
		return
	}
	headers[HeaderTenantID] = id
}

// ExtractToContext installs the tenant id from the propagation header (see
// InjectHeaders) into ctx. When the header is absent or empty, ctx is returned
// unchanged. Consumer pipelines call this before dispatching to handlers so
// tenant-scoped behaviors and repositories see the originating tenant.
func ExtractToContext(ctx context.Context, headers map[string]string) context.Context {
	return WithContext(ctx, headers[HeaderTenantID])
}
