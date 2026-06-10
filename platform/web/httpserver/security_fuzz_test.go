package httpserver_test

import (
	"net/http"
	"net/netip"
	"testing"

	"go-boilerplate/platform/web/httpserver"
)

// FuzzClientIPKey hammers the X-Forwarded-For walk with arbitrary RemoteAddr
// and XFF values. Invariants:
//
//  1. Never panics, whatever garbage arrives in either input.
//  2. Always returns a non-empty key — rate-limit buckets need a stable key
//     even for unparseable peers (documented contract of ClientIPKey).
//  3. With no trusted proxies the key NEVER depends on XFF: a spoofed header
//     from an untrusted peer must not move the caller into another bucket.
func FuzzClientIPKey(f *testing.F) {
	// Seeds from the unit tests (security_test.go).
	f.Add("1.2.3.4:50001", "")
	f.Add("1.2.3.4:50001", "9.9.9.9")
	f.Add("10.0.0.1:9000", "192.168.1.1")
	f.Add("10.0.0.1:9000", "2.3.4.5, 10.0.0.2")
	f.Add("10.0.0.1:9000", "10.0.0.3, 10.0.0.2") // all-trusted → leftmost fallback
	f.Add("not-an-ip", "also,not,ips")
	f.Add("", "")
	f.Add("[::1]:8080", "2001:db8::1, 10.0.0.2")
	f.Add("10.0.0.1:9000", ",,,")

	trusted := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("fc00::/7"),
	}
	keyNone := httpserver.ClientIPKey(nil)
	keyTrusted := httpserver.ClientIPKey(trusted)

	f.Fuzz(func(t *testing.T, remoteAddr, xff string) {
		req := &http.Request{RemoteAddr: remoteAddr, Header: http.Header{}}
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
		}

		gotNone := keyNone(req)
		gotTrusted := keyTrusted(req)

		if remoteAddr != "" {
			if gotNone == "" {
				t.Fatalf("ClientIPKey(nil) returned empty key for RemoteAddr=%q XFF=%q", remoteAddr, xff)
			}
			if gotTrusted == "" {
				t.Fatalf("ClientIPKey(trusted) returned empty key for RemoteAddr=%q XFF=%q", remoteAddr, xff)
			}
		}

		// No trusted proxies → XFF must be a no-op.
		bare := &http.Request{RemoteAddr: remoteAddr, Header: http.Header{}}
		if gotBare := keyNone(bare); gotBare != gotNone {
			t.Fatalf("ClientIPKey(nil) honoured XFF: with=%q without=%q (RemoteAddr=%q XFF=%q)",
				gotNone, gotBare, remoteAddr, xff)
		}
	})
}
