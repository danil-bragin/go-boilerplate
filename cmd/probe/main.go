// Command probe is a minimal static healthcheck client for distroless
// container images (no shell, no curl/wget). The Dockerfile builds it
// alongside the service binary and wires it as the image HEALTHCHECK.
//
// URL resolution, in order:
//  1. first CLI argument, used verbatim (e.g. /probe http://127.0.0.1:8080/healthz)
//  2. ADMIN_HTTP_ADDR env (the servicekit admin listener, default :9090) —
//     its port is probed as http://127.0.0.1:<port>/livez
//  3. fallback http://127.0.0.1:9090/livez
//
// Exit code 0 on any 2xx response, 1 otherwise. Hard 2s timeout.
package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

func main() {
	os.Exit(run(os.Args[1:], os.Getenv("ADMIN_HTTP_ADDR"), 2*time.Second))
}

// run performs a single GET against the resolved URL and maps the outcome to
// a process exit code: 0 for 2xx, 1 for anything else (non-2xx, timeout,
// connection or URL errors).
func run(args []string, adminAddr string, timeout time.Duration) int {
	url := probeURL(args, adminAddr)
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url) //nolint:gosec // G704: target comes from the container's own argv/env, not request input
	if err != nil {
		fmt.Fprintf(os.Stderr, "probe: %v\n", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "probe: %s -> %s\n", url, resp.Status)
		return 1
	}
	return 0
}

// probeURL resolves the target URL from the CLI args, the ADMIN_HTTP_ADDR
// env value, or the default admin /livez endpoint, in that order.
func probeURL(args []string, adminAddr string) string {
	if len(args) > 0 && args[0] != "" {
		return args[0]
	}
	port := "9090"
	if adminAddr != "" {
		if _, p, err := net.SplitHostPort(adminAddr); err == nil && p != "" {
			port = p
		}
	}
	return "http://127.0.0.1:" + port + "/livez"
}
