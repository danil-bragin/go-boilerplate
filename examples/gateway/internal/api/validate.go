package api

import (
	"reflect"
	"strings"

	"go-boilerplate/platform/apperr"

	"github.com/go-playground/validator/v10"

	ordersv1 "go-boilerplate/gen/proto/orders/v1"
)

// createOrderCommand mirrors the CreateOrderCommand wire shape for edge
// validation. The first four validate tags are DUPLICATED verbatim from the
// authoritative consumer-side command (orders/internal/app.CreateOrder:
// required / required / gt=0 / required) — duplication at the edge is
// deliberate: the gateway rejects bad input with a 400 BEFORE producing,
// while the orders consumer keeps the same checks as defense in depth (a
// validation failure there is permanent and goes straight to the DLT).
//
// Edge-only additions on top of the orders tags (mirroring openapi.yaml):
// customer_id max=128, currency len=3 + uppercase. The ISO-4217 allowlist
// check happens in code (see allowedCurrencies), not via tag.
//
// Field names in validation errors use the json tag (same RegisterTagNameFunc
// convention as platform/cqrs) so params.fields matches the wire contract.
type createOrderCommand struct {
	OrderID     string `json:"order_id"     validate:"required,uuid"`
	CustomerID  string `json:"customer_id"  validate:"required,max=128"`
	AmountCents int64  `json:"amount_cents" validate:"gt=0"`
	Currency    string `json:"currency"     validate:"required,len=3,uppercase"`
}

// allowedCurrencies is the gateway's ISO-4217 allowlist: the demo accepts a
// pragmatic subset of common codes rather than the full 180-entry table.
//
// Extension point: replace this set with a full ISO-4217 source of truth
// (e.g. golang.org/x/text/currency or a config-driven list) when the
// product needs more — the validation seam (validateCreateOrderCommand)
// stays the same, only this set changes.
var allowedCurrencies = map[string]struct{}{
	"USD": {}, "EUR": {}, "GBP": {}, "UAH": {}, "PLN": {},
	"JPY": {}, "CHF": {}, "CAD": {}, "AUD": {}, "NZD": {},
	"SEK": {}, "NOK": {}, "DKK": {}, "CZK": {}, "HUF": {},
	"RON": {}, "BGN": {}, "TRY": {}, "ILS": {}, "SGD": {},
}

// edgeValidate is the package-level validator for edge request validation.
// Configured exactly like platform/cqrs's validator: field names in errors
// come from the json tag so structured params match what clients sent.
var edgeValidate = newEdgeValidator()

func newEdgeValidator() *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled())
	v.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})
	return v
}

// validateCreateOrderCommand validates the outgoing command BEFORE any DB
// read or Kafka produce. Failures return a permanent apperr
// VALIDATION_FAILED (registered 400) with Params["fields"] =
// [{field, rule, param}] — the same structured shape platform/cqrs emits for
// consumer-side struct-tag failures, so clients see one validation contract
// end to end.
func validateCreateOrderCommand(cmd *ordersv1.CreateOrderCommand) error {
	c := createOrderCommand{
		OrderID:     cmd.GetOrderId(),
		CustomerID:  cmd.GetCustomerId(),
		AmountCents: cmd.GetAmountCents(),
		Currency:    cmd.GetCurrency(),
	}

	var fields []map[string]any
	if err := edgeValidate.Struct(c); err != nil {
		verrs, ok := err.(validator.ValidationErrors) //nolint:errorlint // Struct returns ValidationErrors directly
		if !ok {
			return apperr.Wrap(err, apperr.CodeValidationFailed)
		}
		for _, fe := range verrs {
			fields = append(fields, map[string]any{"field": fe.Field(), "rule": fe.Tag(), "param": fe.Param()})
		}
	}

	// ISO-4217 allowlist (code check, not a tag): only run when the shape
	// checks passed for currency — a len/case failure already reports it.
	if c.Currency != "" && len(c.Currency) == 3 && c.Currency == strings.ToUpper(c.Currency) {
		if _, ok := allowedCurrencies[c.Currency]; !ok {
			fields = append(fields, map[string]any{"field": "currency", "rule": "iso4217", "param": ""})
		}
	}

	if len(fields) > 0 {
		return apperr.New(apperr.CodeValidationFailed).WithParam("fields", fields)
	}
	return nil
}
