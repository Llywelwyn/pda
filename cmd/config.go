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
	Git             GitConfig   `toml:"git"`
}

type KeyConfig struct {
	AlwaysPromptDelete    bool `toml:"always_prompt_delete"`
	AlwaysPromptOverwrite bool `toml:"always_prompt_overwrite"`
}

type StoreConfig struct {
	DefaultStoreName   string `toml:"default_store_name"`
	AlwaysPromptDelete bool   `toml:"always_prompt_delete"`
}

type GitConfig struct {
	AutoCommit bool `toml:"auto_commit"`
}

var (
	config   Config
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
	config, configErr = loadConfig()
}

func defaultConfig() Config {
	return Config{
		DisplayAsciiArt: true,
		Key: KeyConfig{
			AlwaysPromptDelete:    false,
			AlwaysPromptOverwrite: false,
		},
		Store: StoreConfig{
			DefaultStoreName:   "default",
			AlwaysPromptDelete: true,
		},
		Git: GitConfig{
			AutoCommit: false,
		},
	}
}

func loadConfig() (Config, error) {
	cfg := defaultConfig()

	path, err := configPath()
	if err != nil {
		return cfg, err
	}

	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}

	md, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return cfg, fmt.Errorf("parse %s: %w", path, err)
	}

	if !md.IsDefined("display_ascii_art") {
		cfg.DisplayAsciiArt = defaultConfig().DisplayAsciiArt

	}

	if !md.IsDefined("key", "always_prompt_delete") {
		cfg.Key.AlwaysPromptDelete = defaultConfig().Key.AlwaysPromptDelete
	}

	if !md.IsDefined("store", "default_store_name") || cfg.Store.DefaultStoreName == "" {
		cfg.Store.DefaultStoreName = defaultConfig().Store.DefaultStoreName

	}
	if !md.IsDefined("store", "always_prompt_delete") {
		cfg.Store.AlwaysPromptDelete = defaultConfig().Store.AlwaysPromptDelete
	}

	if !md.IsDefined("key", "always_prompt_overwrite") {
		cfg.Key.AlwaysPromptOverwrite = defaultConfig().Key.AlwaysPromptOverwrite
	}

	if !md.IsDefined("git", "auto_commit") {
		cfg.Git.AutoCommit = defaultConfig().Git.AutoCommit
	}

	return cfg, nil
}

func configPath() (string, error) {
	if override := os.Getenv("PDA_CONFIG"); override != "" {
		return override, nil
	}
	scope := gap.NewScope(gap.User, "pda")
	dir, err := scope.ConfigPath("")
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.toml"), nil
}
