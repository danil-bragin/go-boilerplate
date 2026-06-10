package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
)

// applyFileSecrets implements the <NAME>_FILE convention for Secret fields:
// after env parsing, every config.Secret field with an `env:"NAME"` tag whose
// variable is NOT set in the environment is populated from the file named by
// NAME_FILE (when that variable is set). The file's trailing newline is
// trimmed — Docker secrets and Kubernetes secret mounts conventionally end
// in \n.
//
// Precedence (highest first):
//  1. NAME set in the environment (even to "") — explicit env always wins.
//  2. NAME_FILE set → file contents (overrides envDefault: the default is a
//     dev convenience, a mounted file is operator intent).
//  3. envDefault from the struct tag (already applied by env.Parse).
//
// A set-but-unreadable NAME_FILE is a hard error: silently starting with an
// empty credential would surface as confusing auth failures much later.
//
// CAVEAT: do not combine `env:"NAME,required"` with _FILE sourcing —
// env.Parse enforces `required` BEFORE this pass runs, so a secret supplied
// only via NAME_FILE would fail the required check. Use a Validate() hook
// for "must be non-empty after loading" semantics instead.
//
// The walk recurses into nested and embedded structs (and non-nil struct
// pointers), mirroring caarlos0/env's traversal. Only fields of type Secret
// participate — plain strings keep their env-only behaviour.
func applyFileSecrets(v reflect.Value) error {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}

	secretType := reflect.TypeFor[Secret]()
	for i := range v.NumField() {
		field := v.Field(i)
		sf := v.Type().Field(i)
		if !sf.IsExported() {
			continue
		}

		if sf.Type == secretType {
			if err := loadSecretFromFile(field, sf); err != nil {
				return err
			}
			continue
		}

		switch field.Kind() {
		case reflect.Struct, reflect.Pointer:
			if err := applyFileSecrets(field); err != nil {
				return err
			}
		default:
			// Scalars and collections cannot contain Secret struct fields.
		}
	}
	return nil
}

// loadSecretFromFile applies the _FILE fallback to a single Secret field.
func loadSecretFromFile(field reflect.Value, sf reflect.StructField) error {
	envName, _, _ := strings.Cut(sf.Tag.Get("env"), ",")
	if envName == "" {
		return nil
	}
	if _, set := os.LookupEnv(envName); set {
		return nil // explicit env var wins
	}
	fileVar := envName + "_FILE"
	path, set := os.LookupEnv(fileVar)
	if !set {
		return nil
	}

	raw, err := os.ReadFile(path) //nolint:gosec // G304: reading an operator-supplied secret-mount path is the feature
	if err != nil {
		return fmt.Errorf("config: read secret file %s=%q: %w", fileVar, path, err)
	}
	field.Set(reflect.ValueOf(Secret(strings.TrimRight(string(raw), "\r\n"))))
	return nil
}
