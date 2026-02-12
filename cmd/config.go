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
}

type StoreConfig struct {
	DefaultStoreName      string `toml:"default_store_name"`
	AlwaysPromptDelete    bool   `toml:"always_prompt_delete"`
	AlwaysPromptOverwrite bool   `toml:"always_prompt_overwrite"`
}

type ListConfig struct {
	ListAllStores     bool   `toml:"list_all_stores"`
	DefaultListFormat string `toml:"default_list_format"`
}

type GitConfig struct {
	AutoFetch  bool `toml:"auto_fetch"`
	AutoCommit bool `toml:"auto_commit"`
	AutoPush   bool `toml:"auto_push"`
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
	config, configUndecodedKeys, configErr = loadConfig()
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
			ListAllStores:     true,
			DefaultListFormat: "table",
		},
		Git: GitConfig{
			AutoFetch:  false,
			AutoCommit: false,
			AutoPush:   false,
		},
	}
}

func loadConfig() (Config, []string, error) {
	cfg := defaultConfig()

	path, err := configPath()
	if err != nil {
		return cfg, nil, err
	}

	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return cfg, nil, nil
		}
		return cfg, nil, err
	}

	meta, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return cfg, nil, fmt.Errorf("parse %s: %w", path, err)
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
		return cfg, undecoded, fmt.Errorf("parse %s: list.default_list_format: %w", path, err)
	}

	return cfg, undecoded, nil
}

// validateConfig checks invariants on a Config value before it is persisted.
func validateConfig(cfg Config) error {
	return validListFormat(cfg.List.DefaultListFormat)
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
