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
	"os"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// templateFuncMap returns the shared FuncMap used by both value templates
// (pda get) and commit message templates.
func templateFuncMap() template.FuncMap {
	return template.FuncMap{
		"require": func(v any) (string, error) {
			s := fmt.Sprint(v)
			if s == "" {
				return "", fmt.Errorf("required value is missing or empty")
			}
			return s, nil
		},
		"default": func(def string, v any) string {
			s := fmt.Sprint(v)
			if s == "" {
				return def
			}
			return s
		},
		"env": os.Getenv,
		"enum": func(v any, allowed ...string) (string, error) {
			s := fmt.Sprint(v)
			if s == "" {
				return "", fmt.Errorf("enum value is missing or empty")
			}
			if slices.Contains(allowed, s) {
				return s, nil
			}
			return "", fmt.Errorf("invalid value '%s', allowed: %v", s, allowed)
		},
		"int": func(v any) (int, error) {
			s := fmt.Sprint(v)
			i, err := strconv.Atoi(s)
			if err != nil {
				return 0, fmt.Errorf("cannot convert to int: %w", err)
			}
			return i, nil
		},
		"list": func(v any) []string {
			s := fmt.Sprint(v)
			if s == "" {
				return nil
			}
			parts := strings.Split(s, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			return parts
		},
		"time": func() string { return time.Now().UTC().Format(time.RFC3339) },
	}
}
