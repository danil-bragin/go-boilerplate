package order

import "go-boilerplate/platform/apperr"

// Status is the lifecycle status of an order row. The string values are
// stored verbatim in orders.status (and mirrored by the gateway projection,
// which keeps its own — intentionally separate — state machine for
// out-of-order event arrival).
type Status string

// Order lifecycle statuses. An order is inserted as StatusCreated and moves
// to exactly one of the three terminal payment outcomes.
const (
	// StatusCreated is the initial status: the order row exists, payment
	// outcome unknown.
	StatusCreated Status = "created"
	// StatusPaid is terminal: the payments service processed the payment.
	StatusPaid Status = "paid"
	// StatusPaymentFailed is terminal: the payments service declined or
	// failed the payment.
	StatusPaymentFailed Status = "payment_failed"
	// StatusPaymentTimeout is terminal: no payment outcome arrived within
	// the payment deadline and the unpaid watcher emitted
	// OrderPaymentTimedOut.
	StatusPaymentTimeout Status = "payment_timeout"
)

// transitions is the table-driven state machine — the declarative source of
// truth for which status changes are legal. 'created' is the only state with
// outgoing edges; the three payment outcomes are terminal (first outcome
// wins; see Service.ApplyPaymentOutcome for how the concurrent-safe
// enforcement works at the SQL layer).
var transitions = map[Status]map[Status]bool{
	StatusCreated: {
		StatusPaid:           true,
		StatusPaymentFailed:  true,
		StatusPaymentTimeout: true,
	},
}

// CanTransition reports whether the state machine allows from → to.
func CanTransition(from, to Status) bool {
	return transitions[from][to]
}

// Transition validates from → to against the state machine and returns a
// permanent ORDERS_INVALID_STATUS_TRANSITION apperr (409) when the edge does
// not exist. Permanent: a forbidden transition stays forbidden on every
// retry, so messaging consumers route it straight to the DLT.
func Transition(from, to Status) error {
	if CanTransition(from, to) {
		return nil
	}
	return apperr.New(CodeInvalidStatusTransition).WithParams(map[string]any{
		"from": string(from),
		"to":   string(to),
	})
}
