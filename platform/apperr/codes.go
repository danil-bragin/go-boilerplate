package apperr

// Cross-cutting platform error codes. Domain codes (ORDERS_*, PAYMENTS_*,
// GATEWAY_*, …) live with — and are registered by — the service that owns
// them; only codes produced by platform packages themselves belong here.
const (
	// CodeInternal is the catch-all for unexpected failures. It is also the
	// fallback Code(err) returns for non-apperr errors. Transient: retries
	// may succeed.
	CodeInternal = "INTERNAL"

	// CodeValidationFailed marks input that failed structural validation
	// (struct tags, decode-level checks). Permanent: no retry can fix the
	// payload. Params carry "fields": [{field, rule, param}].
	CodeValidationFailed = "VALIDATION_FAILED"

	// CodeAuthUnauthenticated marks requests with no (valid) principal.
	CodeAuthUnauthenticated = "AUTH_UNAUTHENTICATED"

	// CodeAuthForbidden marks principals lacking permission for an action.
	CodeAuthForbidden = "AUTH_FORBIDDEN"

	// CodeMalformedPayload marks a message whose bytes cannot be decoded
	// (protobuf unmarshal failure, Schema-Registry framing mismatch).
	// Permanent: redelivery re-reads the same bytes — straight to the DLT.
	CodeMalformedPayload = "MESSAGING_MALFORMED_PAYLOAD"

	// CodeRateLimited marks requests rejected by the per-key rate limiter.
	CodeRateLimited = "RATE_LIMITED"

	// CodeRateLimitUnavailable marks fail-closed rejections when the
	// distributed limiter backend is unreachable (503, transient).
	CodeRateLimitUnavailable = "RATE_LIMIT_UNAVAILABLE"

	// CodeTenantRequired marks a request or command that reached a
	// tenant-scoped boundary without a resolvable tenant id (no tenant claim
	// in the principal, no tenant-id propagation header). Permanent: the same
	// input will never carry a tenant on retry.
	CodeTenantRequired = "TENANT_REQUIRED"
)

func init() {
	Register(CodeInternal, 500, false, "internal error")
	Register(CodeValidationFailed, 400, true, "validation failed")
	Register(CodeAuthUnauthenticated, 401, true, "authentication required")
	Register(CodeAuthForbidden, 403, true, "permission denied")
	Register(CodeMalformedPayload, 400, true, "malformed message payload")
	Register(CodeRateLimited, 429, true, "rate limit exceeded")
	Register(CodeRateLimitUnavailable, 503, false, "rate limiter unavailable")
	Register(CodeTenantRequired, 400, true, "tenant context required")
}
