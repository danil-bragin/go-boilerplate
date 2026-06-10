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
)

func init() {
	Register(CodeInternal, 500, false, "internal error")
	Register(CodeValidationFailed, 400, true, "validation failed")
	Register(CodeAuthUnauthenticated, 401, true, "authentication required")
	Register(CodeAuthForbidden, 403, true, "permission denied")
}
