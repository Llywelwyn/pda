package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "View and modify configuration",
}

var configListCmd = &cobra.Command{
	Use:          "list",
	Aliases:      []string{"ls"},
	Short:        "List all configuration values",
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		defaults := defaultConfig()
		fields := configFields(&config, &defaults)
		for _, f := range fields {
			fmt.Printf("%s = %v\n", f.Key, f.Value)
		}
		return nil
	},
}

var configGetCmd = &cobra.Command{
	Use:          "get <key>",
	Short:        "Print a configuration value",
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		defaults := defaultConfig()
		fields := configFields(&config, &defaults)
		f := findConfigField(fields, args[0])
		if f == nil {
			err := fmt.Errorf("unknown config key '%s'", args[0])
			if suggestions := suggestConfigKey(fields, args[0]); len(suggestions) > 0 {
				return withHint(err, fmt.Sprintf("did you mean '%s'?", strings.Join(suggestions, "', '")))
			}
			return err
		}
		fmt.Printf("%v\n", f.Value)
		return nil
	},
}

var configPathCmd = &cobra.Command{
	Use:          "path",
	Short:        "Print config file path",
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		p, err := configPath()
		if err != nil {
			return fmt.Errorf("cannot determine config path: %w", err)
		}
		fmt.Println(p)
		return nil
	},
}

func writeConfigFile(cfg Config) error {
	p, err := configPath()
	if err != nil {
		return fmt.Errorf("cannot determine config path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return fmt.Errorf("cannot create config directory: %w", err)
	}
	f, err := os.Create(p)
	if err != nil {
		return fmt.Errorf("cannot write config: %w", err)
	}
	defer f.Close()
	enc := toml.NewEncoder(f)
	return enc.Encode(cfg)
}

var configInitCmd = &cobra.Command{
	Use:          "init",
	Short:        "Generate default config file",
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		p, err := configPath()
		if err != nil {
			return fmt.Errorf("cannot determine config path: %w", err)
		}
		newFlag, _ := cmd.Flags().GetBool("new")
		if !newFlag {
			if _, err := os.Stat(p); err == nil {
				return withHint(
					fmt.Errorf("config file already exists"),
					"use 'pda config edit' or 'pda config init --new'",
				)
			}
		}
		return writeConfigFile(defaultConfig())
	},
}

var configSetCmd = &cobra.Command{
	Use:          "set <key> <value>",
	Short:        "Set a configuration value",
	Args:         cobra.ExactArgs(2),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		key, raw := args[0], args[1]

		// Work on a copy of the current config so we can write it back.
		cfg := config
		defaults := defaultConfig()
		fields := configFields(&cfg, &defaults)
		f := findConfigField(fields, key)
		if f == nil {
			err := fmt.Errorf("unknown config key '%s'", key)
			if suggestions := suggestConfigKey(fields, key); len(suggestions) > 0 {
				return withHint(err, fmt.Sprintf("did you mean '%s'?", strings.Join(suggestions, "', '")))
			}
			return err
		}

		switch f.Kind {
		case reflect.Bool:
			switch raw {
			case "true":
				f.Field.SetBool(true)
			case "false":
				f.Field.SetBool(false)
			default:
				return fmt.Errorf("cannot set '%s': expected bool (true/false), got '%s'", key, raw)
			}
		case reflect.String:
			f.Field.SetString(raw)
		default:
			return fmt.Errorf("cannot set '%s': unsupported type %s", key, f.Kind)
		}

		if err := writeConfigFile(cfg); err != nil {
			return err
		}

		// Reload so subsequent commands in the same process see the change.
		config = cfg
		return nil
	},
}

func init() {
	configInitCmd.Flags().Bool("new", false, "overwrite existing config file")
	configCmd.AddCommand(configGetCmd)
	configCmd.AddCommand(configInitCmd)
	configCmd.AddCommand(configListCmd)
	configCmd.AddCommand(configPathCmd)
	configCmd.AddCommand(configSetCmd)
	rootCmd.AddCommand(configCmd)
}
