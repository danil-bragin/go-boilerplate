# Error code registry

> **Generated file — do not edit.** Regenerate with `just errgen` (runs
> `go run ./cmd/errgen`). CI regenerates it and fails the unit job when this
> file is out of sync with the registry.

Source of truth: the **live `platform/apperr` registry**. Every code is
registered from `init()` of the package that owns it — `platform/apperr/codes.go`
for the cross-cutting platform codes, `examples/orders/internal/domain/order/codes.go`
for `ORDERS_*`, `examples/gateway/internal/apperrs` for `GATEWAY_*` (payments and
notifications currently register none, by design).

The registry is an **additive-only contract**:

- **Codes are never renamed or removed** once shipped — clients switch on them.
- **Params are stable API** (Google AIP-193: every `{placeholder}` in the
  message template is a declared param, surfaced verbatim in the problem+json
  `params` member). New params may be added; existing ones are never renamed,
  removed, or repurposed.
- A code's HTTP status and permanence are part of its meaning; changing
  either is a new code, not an edit.

Column semantics: **HTTP** is the status `httpx.FromError` maps the code to at
the edge. **Permanent** means no retry can succeed — messaging layers
short-circuit the record straight to the DLT after the first attempt, with the
code in the `x-error-code` header. **Message (en)** is the registered developer
message template (rendered into the problem `detail` field); client-facing
localized texts live in the owning service's i18n catalog, keyed by code.

| Code | HTTP | Permanent | Params | Message (en) |
|---|---|---|---|---|
| `AUTH_FORBIDDEN` | 403 | yes | — | permission denied |
| `AUTH_UNAUTHENTICATED` | 401 | yes | — | authentication required |
| `GATEWAY_ATTACHMENTS_DISABLED` | 404 | yes | — | attachments disabled |
| `GATEWAY_ATTACHMENT_INVALID_FILENAME` | 400 | yes | — | invalid filename |
| `GATEWAY_ATTACHMENT_INVALID_ORDER_ID` | 400 | yes | — | invalid order id |
| `GATEWAY_ATTACHMENT_NOT_FOUND` | 404 | yes | — | attachment not found |
| `GATEWAY_ATTACHMENT_TOO_LARGE` | 413 | yes | — | attachment exceeds the maximum allowed size |
| `GATEWAY_ATTACHMENT_TYPE_REJECTED` | 415 | yes | `content_type` | attachment content type {content_type} is not allowed |
| `GATEWAY_IDEMPOTENCY_BODY_MISMATCH` | 409 | yes | — | idempotency key reused with different request body; replay the original request unchanged or use a new key |
| `GATEWAY_INVALID_CURSOR` | 400 | yes | — | invalid cursor parameter |
| `GATEWAY_INVALID_TIMEZONE` | 400 | yes | `timezone` | invalid X-Timezone value {timezone}: must be an IANA tz database name such as Europe/Kyiv |
| `GATEWAY_MALFORMED_REQUEST` | 400 | yes | `reason` | malformed request: {reason} |
| `GATEWAY_ORDER_NOT_FOUND` | 404 | yes | `order_id` | order {order_id} not found |
| `GATEWAY_SSE_SATURATED` | 503 | no | — | order event stream capacity reached; retry shortly |
| `INTERNAL` | 500 | no | — | internal error |
| `MESSAGING_MALFORMED_PAYLOAD` | 400 | yes | — | malformed message payload |
| `ORDERS_INVALID_ORDER_ID` | 400 | yes | `order_id` | invalid order id {order_id} |
| `ORDERS_INVALID_STATUS_TRANSITION` | 409 | yes | `from`, `to` | order cannot transition from {from} to {to} |
| `RATE_LIMITED` | 429 | yes | — | rate limit exceeded |
| `RATE_LIMIT_UNAVAILABLE` | 503 | no | — | rate limiter unavailable |
| `VALIDATION_FAILED` | 400 | yes | — | validation failed |
