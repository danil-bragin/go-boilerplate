// Command errgen renders docs/errors.md from the LIVE platform/apperr
// registry: it links every package that registers error codes (blank imports
// below), takes the sorted registry snapshot (apperr.Registered) and writes a
// deterministic markdown table — Code | HTTP | Permanent | Params | Message.
//
// The generated file is a CI-enforced contract: the unit job runs the
// generator and fails on `git diff docs/errors.md`, so adding a code without
// regenerating (just errgen) breaks the build instead of silently drifting.
//
// Adding a service that registers codes: blank-import its ROOT package here
// (the root package transitively pulls the internal codes package, whose
// init() registers them) and run `just errgen`.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go-boilerplate/platform/apperr"

	// Registration side effects only: each example service's root package
	// transitively imports the internal package that owns its codes
	// (orders → internal/domain/order, gateway → internal/apperrs).
	// payments and notifications register no codes today — they are imported
	// anyway so any code they grow appears here without touching errgen.
	_ "go-boilerplate/examples/gateway"
	_ "go-boilerplate/examples/notifications"
	_ "go-boilerplate/examples/orders"
	_ "go-boilerplate/examples/payments"
)

//go:generate go run . -out ../../docs/errors.md

const header = `# Error code registry

> **Generated file — do not edit.** Regenerate with ` + "`just errgen`" + ` (runs
> ` + "`go run ./cmd/errgen`" + `). CI regenerates it and fails the unit job when this
> file is out of sync with the registry.

Source of truth: the **live ` + "`platform/apperr`" + ` registry**. Every code is
registered from ` + "`init()`" + ` of the package that owns it — ` + "`platform/apperr/codes.go`" + `
for the cross-cutting platform codes, ` + "`examples/orders/internal/domain/order/codes.go`" + `
for ` + "`ORDERS_*`" + `, ` + "`examples/gateway/internal/apperrs`" + ` for ` + "`GATEWAY_*`" + ` (payments and
notifications currently register none, by design).

The registry is an **additive-only contract**:

- **Codes are never renamed or removed** once shipped — clients switch on them.
- **Params are stable API** (Google AIP-193: every ` + "`{placeholder}`" + ` in the
  message template is a declared param, surfaced verbatim in the problem+json
  ` + "`params`" + ` member). New params may be added; existing ones are never renamed,
  removed, or repurposed.
- A code's HTTP status and permanence are part of its meaning; changing
  either is a new code, not an edit.

Column semantics: **HTTP** is the status ` + "`httpx.FromError`" + ` maps the code to at
the edge. **Permanent** means no retry can succeed — messaging layers
short-circuit the record straight to the DLT after the first attempt, with the
code in the ` + "`x-error-code`" + ` header. **Message (en)** is the registered developer
message template (rendered into the problem ` + "`detail`" + ` field); client-facing
localized texts live in the owning service's i18n catalog, keyed by code.

| Code | HTTP | Permanent | Params | Message (en) |
|---|---|---|---|---|
`

// render produces the full docs/errors.md content from the registry
// snapshot. Deterministic by construction: apperr.Registered is sorted by
// code and no map is iterated (pinned by TestRender_Deterministic).
func render() []byte {
	var b strings.Builder
	b.WriteString(header)
	for _, e := range apperr.Registered() {
		params := "—"
		if len(e.Params) > 0 {
			quoted := make([]string, len(e.Params))
			for i, p := range e.Params {
				quoted[i] = "`" + p + "`"
			}
			params = strings.Join(quoted, ", ")
		}
		permanent := "no"
		if e.Permanent {
			permanent = "yes"
		}
		b.WriteString("| `" + e.Code + "` | " + strconv.Itoa(e.Status) + " | " +
			permanent + " | " + params + " | " + cell(e.Message) + " |\n")
	}
	return []byte(b.String())
}

// cell escapes a message template for use inside a markdown table cell.
func cell(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	return strings.ReplaceAll(s, "\n", " ")
}

func main() {
	out := flag.String("out", "docs/errors.md", "output file (relative to the working directory)")
	flag.Parse()
	// 0o600 keeps gosec happy; the perms only apply when the file is first
	// created — in a checkout it already exists with the repo's umask.
	if err := os.WriteFile(*out, render(), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "errgen:", err)
		os.Exit(1)
	}
	fmt.Printf("errgen: wrote %s (%d codes)\n", *out, len(apperr.Registered()))
}
