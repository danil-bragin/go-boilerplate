// Package config loads strongly-typed configuration from environment
// variables (12-factor) using struct tags, with optional .env file loading.
package config

import (
	"fmt"
	"reflect"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Load reads configuration of type T from environment variables.
// Struct fields use caarlos0/env tags: `env:"NAME"`, `envDefault:"x"`,
// `env:"NAME,required"`, `envSeparator:","`.
//
// T must be a struct type.
func Load[T any]() (T, error) {
	var cfg T
	if err := requireStruct[T](); err != nil {
		return cfg, err
	}
	if err := env.Parse(&cfg); err != nil {
		return cfg, fmt.Errorf("config: parse env: %w", err)
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
	return cfg, nil
}

// requireStruct returns a clear error when T is not a struct type.
func requireStruct[T any]() error {
	if k := reflect.TypeFor[T]().Kind(); k != reflect.Struct {
		return fmt.Errorf("config: type parameter must be a struct, got %s", k)
	}
	return nil
}
