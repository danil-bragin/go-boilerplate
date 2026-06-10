// Package apperrs owns the gateway's GATEWAY_* error codes: the const block
// below is the single registry source for every code the gateway edge can
// emit, registered with platform/apperr at init. Handlers return apperr
// errors carrying these codes and let httpx.FromError map them to RFC 9457
// problem+json at the edge — no layer constructs problem bodies inline.
//
// Cross-cutting codes (INTERNAL, VALIDATION_FAILED, AUTH_*) are owned and
// registered by platform/apperr and flow through the same path.
package apperrs

import "go-boilerplate/platform/apperr"

// Gateway error codes. All are permanent: they describe the client's request
// (or a missing resource) — no retry with the same input can succeed.
const (
	// CodeOrderNotFound: the requested order does not exist in the read
	// model, or the caller is not allowed to see it (non-owners get the SAME
	// code/status as a nonexistent order — no existence oracle). 404.
	CodeOrderNotFound = "GATEWAY_ORDER_NOT_FOUND"

	// CodeInvalidCursor: an undecodable pagination cursor on GET /v1/orders.
	// Cursors are opaque; clients must echo next_cursor verbatim. 400.
	CodeInvalidCursor = "GATEWAY_INVALID_CURSOR"

	// CodeIdempotencyBodyMismatch: an Idempotency-Key was reused with a
	// DIFFERENT request body — a key identifies one logical request. 409.
	CodeIdempotencyBodyMismatch = "GATEWAY_IDEMPOTENCY_BODY_MISMATCH"

	// CodeMalformedRequest: the request could not be bound (malformed JSON,
	// bad parameter types). The binding error text is carried in
	// params.reason — it describes the client's own input, safe to echo. 400.
	CodeMalformedRequest = "GATEWAY_MALFORMED_REQUEST"

	// CodeAttachmentsDisabled: the order-attachments feature flag is off;
	// the endpoints behave as if they do not exist. 404.
	CodeAttachmentsDisabled = "GATEWAY_ATTACHMENTS_DISABLED"

	// CodeAttachmentInvalidOrderID: the attachment route {id} is not a UUID. 400.
	CodeAttachmentInvalidOrderID = "GATEWAY_ATTACHMENT_INVALID_ORDER_ID"

	// CodeAttachmentInvalidFilename: the attachment filename failed
	// sanitization (path separators, control bytes, too long). The rejected
	// value is deliberately NOT echoed back. 400.
	CodeAttachmentInvalidFilename = "GATEWAY_ATTACHMENT_INVALID_FILENAME"

	// CodeAttachmentNotFound: no such attachment object for the order. 404.
	CodeAttachmentNotFound = "GATEWAY_ATTACHMENT_NOT_FOUND"
)

func init() {
	apperr.Register(CodeOrderNotFound, 404, true, "order {order_id} not found", "order_id")
	apperr.Register(CodeInvalidCursor, 400, true, "invalid cursor parameter")
	apperr.Register(CodeIdempotencyBodyMismatch, 409, true,
		"idempotency key reused with different request body; replay the original request unchanged or use a new key")
	apperr.Register(CodeMalformedRequest, 400, true, "malformed request: {reason}", "reason")
	apperr.Register(CodeAttachmentsDisabled, 404, true, "attachments disabled")
	apperr.Register(CodeAttachmentInvalidOrderID, 400, true, "invalid order id")
	apperr.Register(CodeAttachmentInvalidFilename, 400, true, "invalid filename")
	apperr.Register(CodeAttachmentNotFound, 404, true, "attachment not found")
}
