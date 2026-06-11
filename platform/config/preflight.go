package config

import (
	"errors"
	"os"
	"strings"
)

// AppEnvVar is the environment variable that selects the deployment profile.
// "production" (case-insensitive) activates the production safety preflight;
// anything else (unset, "development", "staging", "test", …) keeps the
// convenient-but-insecure local defaults available.
const AppEnvVar = "APP_ENV"

// IsProduction reports whether APP_ENV selects the production profile
// (case-insensitive match on "production"). Use it to gate
// production-only behaviour.
func IsProduction() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv(AppEnvVar)), "production")
}

// RequireProductionSafety runs the given preflight checks ONLY when
// APP_ENV=production, aggregating every failure into one error. Outside
// production it is a no-op, so development and test keep their insecure-but-
// convenient defaults (sslmode=disable, CORS="*", the shipped dev DSN, …).
//
// It is the reusable seam for a service Config.Validate() implementation: the
// service supplies closures that inspect ITS fields and return a clear error
// per insecure value, e.g.
//
//	func (c Config) Validate() error {
//	    return config.RequireProductionSafety(
//	        func() error { if c.AuthDisabled { return errors.New("...") }; return nil },
//	        ...
//	    )
//	}
//
// Aggregating (rather than failing on the first) means an operator sees ALL
// misconfigurations in a single failed boot instead of fixing them one
// redeploy at a time.
func RequireProductionSafety(checks ...func() error) error {
	if !IsProduction() {
		return nil
	}
	var errs []error
	for _, check := range checks {
		if err := check(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
