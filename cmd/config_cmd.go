package cmd

import (
	"fmt"

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
	configCmd.AddCommand(configListCmd)
	configCmd.AddCommand(configPathCmd)
	rootCmd.AddCommand(configCmd)
}
