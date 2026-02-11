package cmd

import (
	"reflect"
	"strings"

	"github.com/agnivade/levenshtein"
)

// ConfigField represents a single leaf field in the Config struct,
// mapped to its dotted TOML key path.
type ConfigField struct {
	Key       string        // dotted key, e.g. "git.auto_commit"
	Value     any           // current value
	Default   any           // value from defaultConfig()
	IsDefault bool          // Value == Default
	Field     reflect.Value // settable reflect.Value (from cfg pointer)
	Kind      reflect.Kind  // field type kind
}

// configFields walks cfg and defaults in parallel, returning a ConfigField
// for every leaf field. Keys are built from TOML struct tags.
func configFields(cfg, defaults *Config) []ConfigField {
	var fields []ConfigField
	walk(reflect.ValueOf(cfg).Elem(), reflect.ValueOf(defaults).Elem(), "", &fields)
	return fields
}

func walk(cv, dv reflect.Value, prefix string, out *[]ConfigField) {
	ct := cv.Type()
	for i := 0; i < ct.NumField(); i++ {
		sf := ct.Field(i)
		tag := sf.Tag.Get("toml")
		if tag == "" || tag == "-" {
			continue
		}

		key := tag
		if prefix != "" {
			key = prefix + "." + tag
		}

		cfv := cv.Field(i)
		dfv := dv.Field(i)

		if sf.Type.Kind() == reflect.Struct {
			walk(cfv, dfv, key, out)
			continue
		}

		*out = append(*out, ConfigField{
			Key:       key,
			Value:     cfv.Interface(),
			Default:   dfv.Interface(),
			IsDefault: reflect.DeepEqual(cfv.Interface(), dfv.Interface()),
			Field:     cfv,
			Kind:      sf.Type.Kind(),
		})
	}
}

// findConfigField returns the ConfigField matching the given dotted key,
// or nil if not found.
func findConfigField(fields []ConfigField, key string) *ConfigField {
	for i := range fields {
		if fields[i].Key == key {
			return &fields[i]
		}
	}
	return nil
}

// suggestConfigKey returns suggestions for a mistyped config key. More generous
// than key/store suggestions since the config key space is small (~11 keys).
// Normalises spaces to underscores and matches against both the full dotted key
// and the leaf segment (part after the last dot).
func suggestConfigKey(fields []ConfigField, target string) []string {
	normalized := strings.ReplaceAll(target, " ", "_")
	var suggestions []string
	for _, f := range fields {
		if matchesConfigKey(normalized, f.Key) {
			suggestions = append(suggestions, f.Key)
		}
	}
	return suggestions
}

func matchesConfigKey(input, key string) bool {
	// Substring match (either direction)
	if strings.Contains(key, input) || strings.Contains(input, key) {
		return true
	}
	// Levenshtein against full dotted key
	if levenshtein.ComputeDistance(input, key) <= max(len(key)/3, 4) {
		return true
	}
	// Levenshtein against leaf segment
	if i := strings.LastIndex(key, "."); i >= 0 {
		leaf := key[i+1:]
		if levenshtein.ComputeDistance(input, leaf) <= max(len(leaf)/3, 1) {
			return true
		}
	}
	return false
}
