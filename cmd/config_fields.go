package cmd

import "reflect"

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
