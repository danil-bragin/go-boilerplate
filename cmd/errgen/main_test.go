package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"go-boilerplate/platform/apperr"
)

// TestRender_Deterministic pins the generator contract CI relies on:
// rendering twice yields byte-identical output, so `errgen && git diff
// --exit-code docs/errors.md` never flakes on map iteration order.
func TestRender_Deterministic(t *testing.T) {
	a := render()
	b := render()
	if !bytes.Equal(a, b) {
		t.Fatal("render() is not deterministic: two runs produced different bytes")
	}
}

// TestRender_OneRowPerRegisteredCode asserts every registered code appears as
// exactly one table row, in sorted (registry-snapshot) order, carrying its
// registered status.
func TestRender_OneRowPerRegisteredCode(t *testing.T) {
	out := string(render())
	lastIdx := -1
	for _, e := range apperr.Registered() {
		row := fmt.Sprintf("| `%s` | %d |", e.Code, e.Status)
		idx := strings.Index(out, row)
		if idx < 0 {
			t.Errorf("missing or malformed row for %s (want prefix %q)", e.Code, row)
			continue
		}
		if strings.Contains(out[idx+len(row):], row) {
			t.Errorf("duplicate row for %s", e.Code)
		}
		if idx < lastIdx {
			t.Errorf("row for %s is out of sorted order", e.Code)
		}
		lastIdx = idx
	}
}

// TestRender_IncludesServiceCodes guards the blank imports in main.go: if a
// service's registration package stops being linked into the generator, its
// codes silently vanish from docs/errors.md — this test turns that into a
// hard failure.
func TestRender_IncludesServiceCodes(t *testing.T) {
	out := string(render())
	for _, code := range []string{
		"INTERNAL",                         // platform/apperr
		"ORDERS_INVALID_STATUS_TRANSITION", // examples/orders
		"GATEWAY_ORDER_NOT_FOUND",          // examples/gateway
	} {
		if !strings.Contains(out, "| `"+code+"` |") {
			t.Errorf("generated registry is missing code %s", code)
		}
	}
}
