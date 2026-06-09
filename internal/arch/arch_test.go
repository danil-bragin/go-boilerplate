// Package arch contains architecture-level invariant tests.
// These tests are fast (no Docker) and always run, even under -short.
package arch_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestPlatformDoesNotImportExamples enforces the core boundary invariant:
// platform/ is the reusable half; examples/ are deletable demos. Deleting
// examples/ must leave platform/ fully functional.
func TestPlatformDoesNotImportExamples(t *testing.T) {
	// go list resolves ./platform/... relative to the module root, not the
	// test binary's working directory. Use go env GOMODCACHE-independent
	// approach: pass the absolute module root via go list -m -json first,
	// then run with the correct -C flag (go 1.21+) or let the go tool find it.
	// Simplest: use "go list -m -f {{.Dir}}" to get the module root, then run
	// go list -deps from there.
	rootOut, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		t.Fatalf("go list -m: %v", err)
	}
	root := strings.TrimSpace(string(rootOut))

	cmd := exec.Command("go", "list", "-deps", "./platform/...")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps ./platform/...: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.Contains(line, "go-boilerplate/examples/") {
			t.Errorf("platform package imports examples: %s", line)
		}
	}
}
