// Package config loads strongly-typed configuration from environment
// variables (and optional config files) using struct tags.
package config

import (
	"fmt"
	"reflect"

	"github.com/ilyakaznacheev/cleanenv"
)

// Load reads configuration of type T from environment variables.
// Struct fields use cleanenv tags: `env:"NAME"`, `env-default:"x"`,
// `env-required:"true"`, `env-separator:","`.
//
// T must be a struct type; a non-struct T returns a clear error rather than
// an opaque cleanenv internal message.
func Load[T any]() (T, error) {
	var cfg T
	if err := requireStruct[T](); err != nil {
		return cfg, err
	}
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return cfg, fmt.Errorf("config: read env: %w", err)
	}
	return cfg, nil
}

// LoadFromFile reads configuration of type T from a config file
// (YAML/JSON/TOML/ENV by extension) and then overlays environment variables.
// T must be a struct type.
func LoadFromFile[T any](path string) (T, error) {
	var cfg T
	if err := requireStruct[T](); err != nil {
		return cfg, err
	}
	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return cfg, fmt.Errorf("config: read %s: %w", path, err)
	}
	return cfg, nil
}

// requireStruct returns a clear error when T is not a struct type. Go generics
// cannot constrain T to structs, so this is enforced at runtime.
func requireStruct[T any]() error {
	if k := reflect.TypeFor[T]().Kind(); k != reflect.Struct {
		return fmt.Errorf("config: type parameter must be a struct, got %s", k)
	}
	return nil
}
