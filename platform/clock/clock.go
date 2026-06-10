// Package clock defines the minimal time source injected into business code.
//
// Inject a Clock ONLY where business logic reads "now" to make decisions
// (deadlines, state-machine timestamps, occurred_at fields) — that is what
// makes the logic testable. Database-generated time (DEFAULT now(),
// inserted_at set by SQL) stays in the database: it is transactional and
// consistent across replicas; do not replace it with application time.
//
// Testing: prefer testing/synctest bubbles over fake-clock implementations —
// inside synctest.Test, time.Now (and therefore System.Now) is fake and
// advances deterministically with time.Sleep. See the package tests for the
// pattern.
package clock

import "time"

// Clock is a source of the current time.
//
// Implementations MUST return UTC. Everything in this repository stores and
// compares instants in UTC (see platform/storage/pg's timestamptz
// ScanLocation); a Clock returning local time would silently produce
// mixed-zone timestamps in logs and payloads.
type Clock interface {
	Now() time.Time
}

// System is the production Clock: time.Now() in UTC.
type System struct{}

// Now returns the current time in UTC.
func (System) Now() time.Time { return time.Now().UTC() }
