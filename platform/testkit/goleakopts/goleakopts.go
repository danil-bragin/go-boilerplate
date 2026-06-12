// Package goleakopts centralizes the goleak ignore set shared by this repo's
// integration tests, so individual tests stop copy-pasting the same
// IgnoreTopFunction lines.
package goleakopts

import "go.uber.org/goleak"

// Default returns the standard goleak options for tests that use
// testcontainers, franz-go and/or the openfeature global SDK:
//
//   - the testcontainers reaper (ryuk) connection goroutine is a
//     process-wide singleton that lives until the test binary exits;
//   - franz-go's metadata and connection-reaper loops belong to the client
//     lifetime and wind down asynchronously after Close;
//   - openfeature's global event-executor listener is started once by the
//     SDK's package-level API and lives until process exit.
//
// None of these indicate a leak in code under test. Extra options (e.g.
// goleak.IgnoreCurrent()) are appended after the defaults.
func Default(extra ...goleak.Option) []goleak.Option {
	opts := make([]goleak.Option, 0, 4+len(extra))
	opts = append(
		opts,
		goleak.IgnoreTopFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).updateMetadataLoop"),
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).reapConnectionsLoop"),
		// OpenFeature v2's global event executor starts a listener goroutine that
		// has no public Shutdown hook in this pseudo-version. The closure's
		// reported name differs by build (with/without the newEventExecutor.
		// prefix), so both forms are ignored. Not our leak — a third-party
		// process-global singleton.
		goleak.IgnoreTopFunction("go.openfeature.dev/openfeature/v2.(*eventExecutor).startEventListener.func1.1"),
		goleak.IgnoreTopFunction("go.openfeature.dev/openfeature/v2.newEventExecutor.(*eventExecutor).startEventListener.func1.1"),
	)
	return append(opts, extra...)
}
