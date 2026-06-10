// Package notification is the domain layer of the notifications service —
// deliberately minimal, demonstrating that the uniform layering (transport →
// domain service) applies even when the "business logic" is a few lines:
// entry points stay decode+dispatch adapters, rules live here, and the
// service grows in place when the rules do ("cmd never calls cmd").
package notification

// Notifier delivers one payment notification. The default production
// implementation logs a structured line (wired in the notifications app);
// tests inject a capturing implementation.
type Notifier func(orderID, paymentID, status string)

// Service owns the notification rules for payment outcomes.
type Service struct {
	notify Notifier
}

// NewService builds the notifications domain service over a delivery channel.
func NewService(notify Notifier) *Service {
	return &Service{notify: notify}
}

// PaymentProcessed notifies about a successful payment, forwarding the
// payment identity and status from the event.
func (s *Service) PaymentProcessed(orderID, paymentID, status string) {
	s.notify(orderID, paymentID, status)
}

// PaymentFailed notifies about a declined/failed payment. Rule: no payment
// id exists (no payment row was created) and the status is the terminal
// "failed" outcome — the machine-readable reason travels in the event and is
// logged by the default notifier wiring, not surfaced to the customer.
func (s *Service) PaymentFailed(orderID string) {
	s.notify(orderID, "", "failed")
}
