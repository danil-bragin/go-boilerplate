// Package config loads strongly-typed configuration from environment
// variables (12-factor) using struct tags, with optional .env file loading.
package config

import (
	"fmt"
	"reflect"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Validator is the optional self-validation hook: when a config type (or its
// pointer) implements it, Load/LoadFromFile call Validate() after env parsing
// and fail loading on error. Use it for cross-field invariants the env tags
// cannot express (e.g. "TLS cert and key must be set together").
type Validator interface {
	Validate() error
}

// Load reads configuration of type T from environment variables.
// Struct fields use caarlos0/env tags: `env:"NAME"`, `envDefault:"x"`,
// `env:"NAME,required"`, `envSeparator:","`.
//
// T must be a struct type. If T (or *T) implements Validator, the hook runs
// after parsing.
func Load[T any]() (T, error) {
	var cfg T
	if err := requireStruct[T](); err != nil {
		return cfg, err
	}
	if err := env.Parse(&cfg); err != nil {
		return cfg, fmt.Errorf("config: parse env: %w", err)
	}
	if err := validateHook(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// LoadFromFile loads key=value pairs from a .env file at path (without
// overriding variables already set in the environment), then parses T from
// the environment. Existing environment variables take precedence over the
// file (env overlays file).
func LoadFromFile[T any](path string) (T, error) {
	var cfg T
	if err := requireStruct[T](); err != nil {
		return cfg, err
	}
	if err := godotenv.Load(path); err != nil {
		return cfg, fmt.Errorf("config: load %s: %w", path, err)
	}
	if err := env.Parse(&cfg); err != nil {
		return cfg, fmt.Errorf("config: parse env: %w", err)
	}
	if err := validateHook(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// validateHook calls cfg.Validate() when the config type implements Validator
// on either receiver (the *T check covers pointer-receiver implementations).
func validateHook[T any](cfg *T) error {
	if v, ok := any(cfg).(Validator); ok {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("config: validate: %w", err)
		}
	}
	return nil
}

// requireStruct returns a clear error when T is not a struct type.
func requireStruct[T any]() error {
	if k := reflect.TypeFor[T]().Kind(); k != reflect.Struct {
		return fmt.Errorf("config: type parameter must be a struct, got %s", k)
	}
	return nil
}
