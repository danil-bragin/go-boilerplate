package main

import (
	"bytes"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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

// TestMain_ImportsEveryExampleServiceRoot is the linkage guard for the blank
// imports in main.go: every example SERVICE (a directory under examples/ with
// a cmd/ subdirectory, i.e. it ships a binary) must have its root package
// blank-imported here, so a new service that registers apperr codes cannot
// silently skip the generated registry. Non-service example dirs (e2e,
// testing) are exempt — they own no codes and ship no binary.
func TestMain_ImportsEveryExampleServiceRoot(t *testing.T) {
	// 1. Discover services: examples/<name>/cmd exists.
	entries, err := os.ReadDir(filepath.Join("..", "..", "examples"))
	if err != nil {
		t.Fatalf("reading examples/: %v", err)
	}
	want := map[string]bool{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join("..", "..", "examples", e.Name(), "cmd")); err == nil {
			want["go-boilerplate/examples/"+e.Name()] = true
		}
	}
	if len(want) == 0 {
		t.Fatal("no example services discovered — layout changed? update this test")
	}

	// 2. Collect the example imports actually present in main.go.
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parsing main.go: %v", err)
	}
	got := map[string]bool{}
	for _, imp := range f.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if strings.HasPrefix(path, "go-boilerplate/examples/") {
			got[path] = true
		}
	}

	for path := range want {
		if !got[path] {
			t.Errorf("main.go is missing the blank import %q — its apperr codes would silently skip docs/errors.md", path)
		}
	}
	for path := range got {
		if !want[path] {
			t.Errorf("main.go imports %q which is not an example service (no cmd/ dir) — stale import?", path)
		}
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
