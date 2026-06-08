// Package config loads strongly-typed configuration from environment
// variables (and optional config files) using struct tags.
package config

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
)

// Load reads configuration of type T from environment variables.
// Struct fields use cleanenv tags: `env:"NAME"`, `env-default:"x"`,
// `env-required:"true"`, `env-separator:","`.
func Load[T any]() (T, error) {
	var cfg T
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return cfg, fmt.Errorf("config: read env: %w", err)
	}
	return cfg, nil
}

// LoadFromFile reads configuration of type T from a config file
// (YAML/JSON/TOML/ENV by extension) and then overlays environment variables.
func LoadFromFile[T any](path string) (T, error) {
	var cfg T
	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return cfg, fmt.Errorf("config: read %s: %w", path, err)
	}
	return cfg, nil
}
