package cmd

import (
	"fmt"
	"os"
	"path/filepath"
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

func init() {
	configInitCmd.Flags().Bool("new", false, "overwrite existing config file")
	configCmd.AddCommand(configGetCmd)
	configCmd.AddCommand(configInitCmd)
	configCmd.AddCommand(configListCmd)
	configCmd.AddCommand(configPathCmd)
	rootCmd.AddCommand(configCmd)
}
