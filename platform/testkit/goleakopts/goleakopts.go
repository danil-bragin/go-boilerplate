// Package goleakopts centralizes the goleak ignore set shared by this repo's
// integration tests, so individual tests stop copy-pasting the same three
// IgnoreTopFunction lines.
package goleakopts

import "go.uber.org/goleak"

// Default returns the standard goleak options for tests that use
// testcontainers and/or franz-go:
//
//   - the testcontainers reaper (ryuk) connection goroutine is a
//     process-wide singleton that lives until the test binary exits;
//   - franz-go's metadata and connection-reaper loops belong to the client
//     lifetime and wind down asynchronously after Close.
//
// None of these indicate a leak in code under test. Extra options (e.g.
// goleak.IgnoreCurrent()) are appended after the defaults.
func Default(extra ...goleak.Option) []goleak.Option {
	opts := make([]goleak.Option, 0, 3+len(extra))
	opts = append(opts,
		goleak.IgnoreTopFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).updateMetadataLoop"),
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).reapConnectionsLoop"),
	)
	return append(opts, extra...)
}
