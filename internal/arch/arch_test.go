// Package arch contains architecture-level invariant tests.
// These tests are fast (no Docker) and always run, even under -short.
package arch_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// module is the Go module path, derived from `go list -m` so the guards keep
// guarding after scripts/rename-module.sh — a hardcoded constant would make
// every Contains/HasPrefix check below pass vacuously post-rename.
var module = sync.OnceValue(func() string {
	out, err := exec.Command("go", "list", "-m").Output()
	if err != nil {
		panic("arch: go list -m: " + err.Error())
	}
	return strings.TrimSpace(string(out))
})

// moduleRoot returns the module root directory. go list resolves ./... and
// ./platform/... relative to the module root, not the test binary's working
// directory, so every guard runs go commands from there.
func moduleRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		t.Fatalf("go list -m: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// goList runs `go list args...` at the module root and returns output lines.
func goList(t *testing.T, args ...string) []string {
	t.Helper()
	cmd := exec.Command("go", append([]string{"list"}, args...)...)
	cmd.Dir = moduleRoot(t)
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("go list %s: %v\n%s", strings.Join(args, " "), err, stderr)
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

// exampleServices discovers the service directories under examples/ (those
// with a cmd/ subdirectory), so scaffolded services are guarded automatically.
func exampleServices(t *testing.T) []string {
	t.Helper()
	root := moduleRoot(t)
	entries, err := os.ReadDir(filepath.Join(root, "examples"))
	if err != nil {
		t.Fatalf("read examples/: %v", err)
	}
	var svcs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, "examples", e.Name(), "cmd")); err == nil {
			svcs = append(svcs, e.Name())
		}
	}
	if len(svcs) < 2 {
		t.Fatalf("expected >=2 example services, found %v — guard misconfigured?", svcs)
	}
	return svcs
}

// TestPlatformDoesNotImportExamples enforces the core boundary invariant:
// platform/ is the reusable half; examples/ are deletable demos. Deleting
// examples/ must leave platform/ fully functional.
func TestPlatformDoesNotImportExamples(t *testing.T) {
	for _, line := range goList(t, "-deps", "./platform/...") {
		if strings.Contains(line, module()+"/examples/") {
			t.Errorf("platform package imports examples: %s", line)
		}
	}
}

// TestExampleServicesDoNotImportEachOther enforces service autonomy: each
// example service is an independently deployable unit that communicates only
// via Kafka events — never via Go imports of another service (root package
// AND internals). Shared contracts live in gen/proto (allowed — not under
// examples/).
func TestExampleServicesDoNotImportEachOther(t *testing.T) {
	for _, svc := range exampleServices(t) {
		ownPrefix := module() + "/examples/" + svc
		for _, dep := range goList(t, "-deps", "./examples/"+svc+"/...") {
			if !strings.HasPrefix(dep, module()+"/examples/") {
				continue
			}
			if dep == ownPrefix || strings.HasPrefix(dep, ownPrefix+"/") {
				continue
			}
			t.Errorf("service %q imports another service's package: %s (services share contracts via gen/proto + Kafka, never Go imports)", svc, dep)
		}
	}
}

// TestNoCrossServiceInternalImports is the explicit belt-and-braces form of
// the previous guard for internal/ trees: no package anywhere in the module
// may DIRECTLY import examples/<svc>/internal/... unless it lives inside
// examples/<svc>. Direct imports only — transitive deps would falsely flag
// legitimate root-package consumers like cmd/migrate (a service root pulls
// its own internals; that is the point of internal/). The Go compiler
// already enforces internal/ visibility — this guard keeps the invariant if
// a refactor ever moves code out of internal/ (the compiler stops caring;
// this test does not).
func TestNoCrossServiceInternalImports(t *testing.T) {
	internalRe := regexp.MustCompile(`^` + regexp.QuoteMeta(module()) + `/examples/([^/]+)/internal(/|$)`)
	for _, line := range goList(t, "-f", `{{.ImportPath}}|{{join .Imports " "}}`, "./...") {
		importer, deps, ok := strings.Cut(line, "|")
		if !ok {
			continue
		}
		for _, dep := range strings.Fields(deps) {
			m := internalRe.FindStringSubmatch(dep)
			if m == nil {
				continue
			}
			owner := module() + "/examples/" + m[1]
			if importer == owner || strings.HasPrefix(importer, owner+"/") {
				continue
			}
			t.Errorf("%s depends on %s — another service's internal/ tree", importer, dep)
		}
	}
}

// TestRunBatchOnlyCalledFromServicekit keeps the batch consumption path behind
// servicekit.AddBatchConsumer, which wraps the per-record fallback in
// kafka.WithRetry (poison → <topic>.DLT). A service that called
// (*kafka.Consumer).RunBatch directly would build a batch loop with NO DLT
// escalation — a deterministic poison record would hot-loop the partition
// forever (seek-back + backoff), breaking failure-contract parity with
// AddConsumer. This guard asserts `.RunBatch(` appears only inside
// platform/servicekit (the sanctioned wiring) and platform/messaging/kafka
// (the implementation + its own tests). A pure go-list/AST guard is impractical
// here (RunBatch is a method call, not an import), so this is a source scan in
// the same spirit as the import guards above.
func TestRunBatchOnlyCalledFromServicekit(t *testing.T) {
	root := moduleRoot(t)
	allowed := []string{
		filepath.Join("platform", "servicekit"),
		filepath.Join("platform", "messaging", "kafka"),
	}
	// Exclude this guard's own source: it must name the method in its docstring
	// and error message, which would otherwise match the scan and flag itself.
	_, selfPath, _, _ := runtime.Caller(0)
	selfRel, _ := filepath.Rel(root, selfPath)

	// Build the search pattern at runtime so the literal call text does not
	// appear in this file's source (it would self-match).
	runBatchCall := regexp.MustCompile(regexp.QuoteMeta(".RunBatch" + "("))

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Skip non-source trees that can legitimately mention the method in
			// prose/fixtures and would only produce noise.
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		if rel == selfRel {
			return nil
		}
		for _, a := range allowed {
			if rel == a || strings.HasPrefix(rel, a+string(filepath.Separator)) {
				return nil
			}
		}
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if runBatchCall.Match(src) {
			t.Errorf("%s calls RunBatch directly — batch consumption must go through "+
				"servicekit.AddBatchConsumer so the per-record fallback is wrapped in "+
				"kafka.WithRetry (poison escalates to the DLT); a raw batch loop skips DLT escalation", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk module root: %v", err)
	}
}

// TestTestkitOnlyImportedFromTests keeps test doubles out of production
// builds: platform/testkit (fakes, fixtures, mocks, mockhttp) may be imported
// only from _test.go files. go list without -test excludes test-file imports,
// so any testkit dependency surfacing here would ship test code in a real
// binary.
func TestTestkitOnlyImportedFromTests(t *testing.T) {
	testkit := module() + "/platform/testkit"

	// Exception: platform/testkit/traffic is a load-generation library, not
	// a test double — by design it is consumed OUTSIDE _test.go files by
	// exactly two packages: the gateway scenario pack (which lives with the
	// service so the e2e traffic test AND the CLI can share it) and the
	// trafficgen dev tool. Neither is linked into a service binary; every
	// other testkit package stays test-only everywhere.
	trafficDep := testkit + "/traffic"
	trafficImporters := map[string]bool{
		module() + "/examples/gateway/traffic": true,
		module() + "/cmd/trafficgen":           true,
	}

	for _, line := range goList(t, "-f", `{{.ImportPath}}|{{join .Deps " "}}`, "./...") {
		importer, deps, ok := strings.Cut(line, "|")
		if !ok {
			continue
		}
		// testkit packages may depend on each other.
		if importer == testkit || strings.HasPrefix(importer, testkit+"/") {
			continue
		}
		for _, dep := range strings.Fields(deps) {
			if dep == testkit || strings.HasPrefix(dep, testkit+"/") {
				if dep == trafficDep && trafficImporters[importer] {
					continue
				}
				t.Errorf("non-test package %s depends on %s — testkit must only be imported from _test.go files", importer, dep)
			}
		}
	}
}
