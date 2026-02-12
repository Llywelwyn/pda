/*
Copyright © 2025 Lewis Wynne <lew@ily.rs>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package cmd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	gap "github.com/muesli/go-app-paths"
)

type Config struct {
	DisplayAsciiArt bool        `toml:"display_ascii_art"`
	Key             KeyConfig   `toml:"key"`
	Store           StoreConfig `toml:"store"`
	List            ListConfig  `toml:"list"`
	Git             GitConfig   `toml:"git"`
}

type KeyConfig struct {
	AlwaysPromptDelete     bool `toml:"always_prompt_delete"`
	AlwaysPromptGlobDelete bool `toml:"always_prompt_glob_delete"`
	AlwaysPromptOverwrite  bool `toml:"always_prompt_overwrite"`
	AlwaysEncrypt          bool `toml:"always_encrypt"`
}

type StoreConfig struct {
	DefaultStoreName      string `toml:"default_store_name"`
	AlwaysPromptDelete    bool   `toml:"always_prompt_delete"`
	AlwaysPromptOverwrite bool   `toml:"always_prompt_overwrite"`
}

type ListConfig struct {
	AlwaysShowAllStores  bool   `toml:"always_show_all_stores"`
	DefaultListFormat    string `toml:"default_list_format"`
	AlwaysShowFullValues bool   `toml:"always_show_full_values"`
	AlwaysHideHeader     bool   `toml:"always_hide_header"`
	DefaultColumns       string `toml:"default_columns"`
}

type GitConfig struct {
	AutoFetch            bool   `toml:"auto_fetch"`
	AutoCommit           bool   `toml:"auto_commit"`
	AutoPush             bool   `toml:"auto_push"`
	DefaultCommitMessage string `toml:"default_commit_message"`
}

var (
	config              Config
	configUndecodedKeys []string
	asciiArt string = `                 ▄▄           
                 ██           
 ██▄███▄    ▄███▄██   ▄█████▄
 ██▀  ▀██  ██▀  ▀██   ▀ ▄▄▄██
 ██    ██  ██    ██  ▄██▀▀▀██
 ███▄▄██▀  ▀██▄▄███  ██▄▄▄███
 ██ ▀▀▀      ▀▀▀ ▀▀   ▀▀▀▀ ▀▀
 ██      (c) 2025 Lewis Wynne
`
	configErr error
)

func init() {
	var migrations []migration
	config, configUndecodedKeys, migrations, configErr = loadConfig()
	for _, m := range migrations {
		if m.Conflict {
			warnf("both '%s' and '%s' present; using '%s'", m.Old, m.New, m.New)
		} else {
			warnf("config key '%s' is deprecated, use '%s'", m.Old, m.New)
		}
	}
}

func defaultConfig() Config {
	return Config{
		DisplayAsciiArt: true,
		Key: KeyConfig{
			AlwaysPromptDelete:     false,
			AlwaysPromptGlobDelete: true,
			AlwaysPromptOverwrite:  false,
		},
		Store: StoreConfig{
			DefaultStoreName:      "default",
			AlwaysPromptDelete:    true,
			AlwaysPromptOverwrite: true,
		},
		List: ListConfig{
			AlwaysShowAllStores: true,
			DefaultListFormat:   "table",
			DefaultColumns:      "key,store,value,ttl",
		},
		Git: GitConfig{
			AutoFetch:            false,
			AutoCommit:           false,
			AutoPush:             false,
			DefaultCommitMessage: "{{ summary }} {{ time }}",
		},
	}
}

// loadConfig returns (config, undecodedKeys, migrations, error).
// Migrations are returned but NOT printed — callers decide.
func loadConfig() (Config, []string, []migration, error) {
	cfg := defaultConfig()

	path, err := configPath()
	if err != nil {
		return cfg, nil, nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil, nil, nil
		}
		return cfg, nil, nil, err
	}

	// Decode into a raw map so we can run deprecation migrations before
	// the struct decode sees the keys.
	var raw map[string]any
	if _, err := toml.Decode(string(data), &raw); err != nil {
		return cfg, nil, nil, fmt.Errorf("parse %s: %w", path, err)
	}

	warnings := migrateRawConfig(raw)

	// Re-encode the migrated map and decode into the typed struct so
	// defaults fill any missing fields.
	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(raw); err != nil {
		return cfg, nil, nil, fmt.Errorf("parse %s: %w", path, err)
	}

	meta, err := toml.Decode(buf.String(), &cfg)
	if err != nil {
		return cfg, nil, nil, fmt.Errorf("parse %s: %w", path, err)
	}

	var undecoded []string
	for _, key := range meta.Undecoded() {
		undecoded = append(undecoded, key.String())
	}

	if cfg.Store.DefaultStoreName == "" {
		cfg.Store.DefaultStoreName = defaultConfig().Store.DefaultStoreName
	}

	if cfg.List.DefaultListFormat == "" {
		cfg.List.DefaultListFormat = defaultConfig().List.DefaultListFormat
	}
	if err := validListFormat(cfg.List.DefaultListFormat); err != nil {
		return cfg, undecoded, warnings, fmt.Errorf("parse %s: list.default_list_format: %w", path, err)
	}

	if cfg.List.DefaultColumns == "" {
		cfg.List.DefaultColumns = defaultConfig().List.DefaultColumns
	}
	if err := validListColumns(cfg.List.DefaultColumns); err != nil {
		return cfg, undecoded, warnings, fmt.Errorf("parse %s: list.default_columns: %w", path, err)
	}

	if cfg.Git.DefaultCommitMessage == "" {
		cfg.Git.DefaultCommitMessage = defaultConfig().Git.DefaultCommitMessage
	}

	return cfg, undecoded, warnings, nil
}

// validateConfig checks invariants on a Config value before it is persisted.
func validateConfig(cfg Config) error {
	if err := validListFormat(cfg.List.DefaultListFormat); err != nil {
		return err
	}
	return validListColumns(cfg.List.DefaultColumns)
}

func configPath() (string, error) {
	if override := os.Getenv("PDA_CONFIG"); override != "" {
		return filepath.Join(override, "config.toml"), nil
	}
	scope := gap.NewScope(gap.User, "pda")
	dir, err := scope.ConfigPath("")
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.toml"), nil
}
