package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "View and modify configuration",
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
	configCmd.AddCommand(configPathCmd)
	rootCmd.AddCommand(configCmd)
}
