package cmd

import (
	"fmt"
	"strings"

	"github.com/gobwas/glob"
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

func compileGlobMatchers(patterns []string, separators []rune) ([]glob.Glob, error) {
	var matchers []glob.Glob
	for _, pattern := range patterns {
		m, err := glob.Compile(strings.ToLower(pattern), separators...)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)
	}
	return matchers, nil
}

func globMatch(matchers []glob.Glob, key string) bool {
	if len(matchers) == 0 {
		return true
	}
	lowered := strings.ToLower(key)
	for _, m := range matchers {
		if m.Match(lowered) {
			return true
		}
	}
	return false
}

func formatGlobPatterns(patterns []string) string {
	quoted := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		quoted = append(quoted, fmt.Sprintf("'%s'", pattern))
	}
	return strings.Join(quoted, ", ")
}
