package order

import "go-boilerplate/platform/apperr"

// ORDERS_* error codes owned by the orders service. Registered here — in the
// package that produces them — per the apperr convention; the registration is
// the single source of truth for status mapping, retry semantics (permanent →
// straight to the DLT) and the default message template. Message templates
// follow the AIP-193 rule: every {placeholder} is declared in Params (pinned
// by TestCodes_RegisteredWithTemplateInvariant).
const (
	// CodeInvalidStatusTransition marks an attempt to move an order to a
	// status the state machine forbids from its current status (see
	// statemachine.go). Permanent: replaying the same transition can never
	// succeed. 409: the order's current state conflicts with the request.
	CodeInvalidStatusTransition = "ORDERS_INVALID_STATUS_TRANSITION"

	// CodeInvalidOrderID marks a caller-supplied order id that is not a
	// valid UUID. Permanent: the payload is malformed and no retry fixes it
	// (consumers short-circuit it to the DLT after one attempt).
	CodeInvalidOrderID = "ORDERS_INVALID_ORDER_ID"
)

func init() {
	apperr.Register(CodeInvalidStatusTransition, 409, true,
		"order cannot transition from {from} to {to}", "from", "to")
	apperr.Register(CodeInvalidOrderID, 400, true,
		"invalid order id {order_id}", "order_id")
}
