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
