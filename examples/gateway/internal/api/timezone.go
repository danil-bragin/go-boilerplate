package api

import (
	"time"

	"go-boilerplate/examples/gateway/internal/apperrs"
	"go-boilerplate/platform/apperr"
)

// Time contract (see openapi.yaml and docs/conventions.md):
//
//   - created_at is ALWAYS UTC, RFC 3339 with the "Z" suffix. The read model
//     stores timestamptz and the pg pool scans with ScanLocation=UTC; the
//     edge serializes via .UTC().Format(time.RFC3339) so the contract holds
//     even if a future code path hands over a non-UTC time.Time.
//   - created_at_local is a DISPLAY-ONLY convenience derived from the
//     X-Timezone request header: the same instant rendered in the named IANA
//     zone, RFC 3339 with that zone's UTC offset. It never replaces
//     created_at and clients must not compute with it (DST makes local
//     arithmetic wrong twice a year).

// formatCreatedAt renders t for the created_at contract field: RFC 3339,
// always UTC ("Z" suffix), second precision.
func formatCreatedAt(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// formatCreatedAtLocal renders t in loc for the display-only
// created_at_local field: RFC 3339 with the zone's UTC offset at that
// instant (DST-correct — e.g. America/New_York flips -05:00 → -04:00 across
// the 2026-03-08 spring-forward).
func formatCreatedAtLocal(t time.Time, loc *time.Location) string {
	return t.In(loc).Format(time.RFC3339)
}

// parseTimezone validates an X-Timezone header value and resolves it to a
// *time.Location. Absent or empty header → (nil, nil): no localization.
//
// Only IANA tz database names are accepted (time.LoadLocation): offsets
// ("UTC+3", "+02:00", "GMT-5") and garbage fail LoadLocation, and the
// special name "Local" is rejected explicitly — it resolves to the SERVER's
// zone, which is meaningless to the client and non-deterministic across
// deployments. Failures yield 400 GATEWAY_INVALID_TIMEZONE with the
// offending value in params.timezone.
func parseTimezone(header *string) (*time.Location, error) {
	if header == nil || *header == "" {
		return nil, nil //nolint:nilnil // no header = no localization, not an error
	}
	name := *header
	if name == "Local" {
		return nil, apperr.New(apperrs.CodeInvalidTimezone).WithParam("timezone", name)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, apperr.Wrap(err, apperrs.CodeInvalidTimezone).WithParam("timezone", name)
	}
	return loc, nil
}
