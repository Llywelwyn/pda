package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	gap "github.com/muesli/go-app-paths"
)

type Config struct {
	DefaultDB string `toml:"default_db"`
}

var (
	config    Config
	configErr error
)

func init() {
	config, configErr = loadConfig()
}

func defaultConfig() Config {
	return Config{
		DefaultDB: "default",
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

	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return cfg, fmt.Errorf("parse %s: %w", path, err)
	}

	if cfg.DefaultDB == "" {
		cfg.DefaultDB = defaultConfig().DefaultDB
	}

	return cfg, nil
}

func configPath() (string, error) {
	if override := os.Getenv("PDA_CONFIG"); override != "" {
		return override, nil
	}
	scope := gap.NewVendorScope(gap.User, "pda", "config")
	dir, err := scope.ConfigPath("")
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.toml"), nil
}
