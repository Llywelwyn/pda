package cmd

import (
	"fmt"
	"strings"

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

func init() {
	configCmd.AddCommand(configGetCmd)
	configCmd.AddCommand(configListCmd)
	configCmd.AddCommand(configPathCmd)
	rootCmd.AddCommand(configCmd)
}
