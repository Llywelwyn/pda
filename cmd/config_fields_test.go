package cmd

import (
	"reflect"
	"testing"
)

func TestConfigFieldsReturnsAllFields(t *testing.T) {
	cfg := defaultConfig()
	defaults := defaultConfig()
	fields := configFields(&cfg, &defaults)

	// Count expected leaf fields by walking the struct
	expected := countLeafFields(reflect.TypeOf(Config{}))
	if len(fields) != expected {
		t.Errorf("configFields returned %d fields, want %d", len(fields), expected)
	}
}

func countLeafFields(t reflect.Type) int {
	n := 0
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Type.Kind() == reflect.Struct {
			n += countLeafFields(f.Type)
		} else {
			n++
		}
	}
	return n
}

func TestConfigFieldsDottedKeys(t *testing.T) {
	cfg := defaultConfig()
	defaults := defaultConfig()
	fields := configFields(&cfg, &defaults)

	want := map[string]bool{
		"display_ascii_art":             true,
		"key.always_prompt_delete":      true,
		"key.always_prompt_glob_delete": true,
		"key.always_prompt_overwrite":   true,
		"key.always_encrypt":            true,
		"store.default_store_name":      true,
		"store.always_prompt_delete":    true,
		"store.always_prompt_overwrite": true,
		"list.always_show_all_stores":   true,
		"list.default_list_format":      true,
		"list.always_show_full_values":  true,
		"list.always_hide_header":       true,
		"list.default_columns":          true,
		"git.auto_fetch":               true,
		"git.auto_commit":              true,
		"git.auto_push":                true,
		"git.default_commit_message":    true,
	}

	got := make(map[string]bool)
	for _, f := range fields {
		got[f.Key] = true
	}

	for k := range want {
		if !got[k] {
			t.Errorf("missing key %q", k)
		}
	}
	for k := range got {
		if !want[k] {
			t.Errorf("unexpected key %q", k)
		}
	}
}

func TestConfigFieldsAllDefaults(t *testing.T) {
	cfg := defaultConfig()
	defaults := defaultConfig()
	fields := configFields(&cfg, &defaults)

	for _, f := range fields {
		if !f.IsDefault {
			t.Errorf("field %q should be default, got Value=%v Default=%v", f.Key, f.Value, f.Default)
		}
	}
}

func TestConfigFieldsDetectsNonDefault(t *testing.T) {
	cfg := defaultConfig()
	cfg.Git.AutoCommit = true
	defaults := defaultConfig()
	fields := configFields(&cfg, &defaults)

	for _, f := range fields {
		if f.Key == "git.auto_commit" {
			if f.IsDefault {
				t.Errorf("git.auto_commit should not be default after change")
			}
			if f.Value != true {
				t.Errorf("git.auto_commit Value = %v, want true", f.Value)
			}
			return
		}
	}
	t.Error("git.auto_commit not found in fields")
}

func TestConfigFieldsSettable(t *testing.T) {
	cfg := defaultConfig()
	defaults := defaultConfig()
	fields := configFields(&cfg, &defaults)

	for _, f := range fields {
		if f.Key == "git.auto_push" {
			if f.Kind != reflect.Bool {
				t.Errorf("git.auto_push Kind = %v, want Bool", f.Kind)
			}
			f.Field.SetBool(true)
			if !cfg.Git.AutoPush {
				t.Error("setting field via reflect did not update cfg")
			}
			return
		}
	}
	t.Error("git.auto_push not found in fields")
}

func TestConfigFieldsStringField(t *testing.T) {
	cfg := defaultConfig()
	defaults := defaultConfig()
	fields := configFields(&cfg, &defaults)

	for _, f := range fields {
		if f.Key == "store.default_store_name" {
			if f.Kind != reflect.String {
				t.Errorf("store.default_store_name Kind = %v, want String", f.Kind)
			}
			if f.Value != "default" {
				t.Errorf("store.default_store_name Value = %v, want 'default'", f.Value)
			}
			return
		}
	}
	t.Error("store.default_store_name not found in fields")
}
