package cmd

import (
	"strings"

	"github.com/spf13/cobra"
)

var defaultGlobSeparators = []rune{'/', '-', '_', '.', '@', ':', ' '}

func defaultGlobSeparatorsDisplay() string {
	var b strings.Builder
	for _, r := range defaultGlobSeparators {
		b.WriteRune(r)
	}
	return b.String()
}

func parseGlobSeparators(cmd *cobra.Command) ([]rune, error) {
	sepStr, err := cmd.Flags().GetString("glob-sep")
	if err != nil {
		return nil, err
	}
	if sepStr == "" {
		return defaultGlobSeparators, nil
	}
	return []rune(sepStr), nil
}
