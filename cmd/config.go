package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	gap "github.com/muesli/go-app-paths"
)

type Config struct {
	DefaultDB       string `toml:"default_db"`
	DisplayArt      bool   `toml:"display_art"`
	WarnOnDelete    bool   `toml:"warn_on_delete"`
	WarnOnOverwrite bool   `toml:"warn_on_overwrite"`
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
		DefaultDB:       "default",
		DisplayArt:      false,
		WarnOnOverwrite: true,
		WarnOnDelete:    true,
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

	if !md.IsDefined("default_db") || cfg.DefaultDB == "" {
		cfg.DefaultDB = defaultConfig().DefaultDB
	}

	if !md.IsDefined("display_art") {
		cfg.DisplayArt = defaultConfig().DisplayArt
	}

	if !md.IsDefined("warn_on_delete") {
		cfg.WarnOnDelete = defaultConfig().WarnOnDelete
	}

	if !md.IsDefined("warn_on_overwrite") {
		cfg.WarnOnOverwrite = defaultConfig().WarnOnOverwrite
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
